defmodule Squidie.ReleasePrepScriptTest do
  use ExUnit.Case, async: false

  @script Path.expand("../../scripts/squidie_release_prep.exs", __DIR__)
  @command_env [{"GIT_TERMINAL_PROMPT", "0"}, {"GIT_ASKPASS", "true"}]

  setup do
    tmp_dir =
      Path.join(System.tmp_dir!(), "squidie-release-prep-#{System.unique_integer([:positive])}")

    File.mkdir_p!(Path.join(tmp_dir, "docs"))

    File.write!(Path.join(tmp_dir, "mix.exs"), """
    defmodule Example.MixProject do
      use Mix.Project

      def project do
        [
          app: :squidie,
          version: "0.3.0"
        ]
      end
    end
    """)

    File.write!(Path.join(tmp_dir, "README.md"), """
    defp deps do
      [
        {:squidie, "~> 0.3.0"}
      ]
    end
    """)

    File.write!(Path.join(tmp_dir, "docs/workflow_authoring.livemd"), """
    Mix.install([
      {:squidie, "~> 0.3.0"}
    ])
    """)

    File.write!(Path.join(tmp_dir, "docs/host_app_integration.md"), """
    defp deps do
      [
        {:squidie, "~> 0.3.0"}
      ]
    end

    defp deps do
      [
        {:jido, "~> 2.0"},
        {:squidie, "~> 0.3.0"}
      ]
    end
    """)

    File.write!(Path.join(tmp_dir, "CHANGELOG.md"), """
    # Changelog

    All notable changes to Squidie will be documented in this file.

    ## [0.3.0] - 2026-06-12

    ### Changed
    - Previous release.
    """)

    File.cd!(tmp_dir, fn ->
      git!(["init", "--initial-branch", "main"])
      git!(["config", "user.email", "test@example.com"])
      git!(["config", "user.name", "Test User"])
      git!(["add", "."])
      git!(["commit", "-m", "chore: release 0.3.0"])
      git!(["tag", "-a", "v0.3.0", "-m", "Release 0.3.0"])

      File.write!(
        Path.join(tmp_dir, "README.md"),
        File.read!(Path.join(tmp_dir, "README.md")) <> "\nupdate\n"
      )

      git!(["add", "README.md"])
      git!(["commit", "-m", "docs: update readme"])
    end)

    on_exit(fn -> File.rm_rf!(tmp_dir) end)

    {:ok, tmp_dir: tmp_dir}
  end

  test "updates version metadata and changelog from commits since the previous tag", %{
    tmp_dir: tmp_dir
  } do
    assert {output, 0} = release_prep(tmp_dir, ["0.3.1", "--date", "2026-06-17"])

    assert File.read!(Path.join(tmp_dir, "mix.exs")) =~ ~s(version: "0.3.1")
    assert File.read!(Path.join(tmp_dir, "README.md")) =~ ~s({:squidie, "~> 0.3.1"})

    assert File.read!(Path.join(tmp_dir, "docs/workflow_authoring.livemd")) =~
             ~s({:squidie, "~> 0.3.1"})

    host_app_integration = File.read!(Path.join(tmp_dir, "docs/host_app_integration.md"))
    assert host_app_integration =~ ~s({:squidie, "~> 0.3.1"})
    refute host_app_integration =~ ~s({:squidie, "~> 0.3.0"})

    changelog = File.read!(Path.join(tmp_dir, "CHANGELOG.md"))
    assert changelog =~ "## [0.3.1] - 2026-06-17"
    assert changelog =~ "docs: update readme"
    assert changelog =~ "## [0.3.0] - 2026-06-12"
    assert output =~ "Prepared Squidie 0.3.1 release metadata."
    assert output =~ "Changelog source: v0.3.0..HEAD"
  end

  test "writes release notes for GitHub releases", %{tmp_dir: tmp_dir} do
    notes_path = Path.join(tmp_dir, "tmp/release-notes.md")

    assert {_output, 0} =
             release_prep(tmp_dir, [
               "0.3.1",
               "--date",
               "2026-06-17",
               "--notes-file",
               notes_path
             ])

    assert File.read!(notes_path) =~ "docs: update readme"
  end

  test "rejects an existing changelog section", %{tmp_dir: tmp_dir} do
    File.write!(Path.join(tmp_dir, "CHANGELOG.md"), """
    # Changelog

    ## [0.3.1] - 2026-06-17

    ### Changed
    - Already released.

    ## [0.3.0] - 2026-06-12
    """)

    assert {output, 1} = release_prep(tmp_dir, ["0.3.1", "--date", "2026-06-17"])
    assert output =~ "CHANGELOG.md already has a 0.3.1 section"
  end

  defp release_prep(tmp_dir, args) do
    System.cmd("elixir", [@script | args],
      cd: tmp_dir,
      env: @command_env,
      stderr_to_stdout: true
    )
  end

  defp git!(args) do
    assert {_output, 0} = System.cmd("git", args, env: @command_env, stderr_to_stdout: true)
  end
end
