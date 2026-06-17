defmodule Squidie.ReleasePrepScript do
  @version_files [
    "README.md",
    "docs/workflow_authoring.livemd",
    "docs/host_app_integration.md"
  ]
  @git_env [{"GIT_TERMINAL_PROMPT", "0"}, {"GIT_ASKPASS", "true"}]

  def main(args) do
    {opts, args, invalid} =
      OptionParser.parse(args,
        strict: [date: :string, from_tag: :string, notes_file: :string],
        aliases: [d: :date]
      )

    if invalid != [] do
      fail!("Invalid options: #{inspect(invalid)}")
    end

    version = single_version!(args)
    validate_version!(version)

    date = Keyword.get(opts, :date, Date.to_iso8601(Date.utc_today()))
    from_tag = Keyword.get(opts, :from_tag) || previous_version_tag!(version)
    commits = commits_since!(from_tag)

    update_mix_version!(version)
    update_install_snippets!(version)

    notes = changelog_notes(commits)
    update_changelog!(version, date, notes)
    write_notes_file(opts[:notes_file], notes)

    IO.puts("""
    Prepared Squidie #{version} release metadata.
    Changelog source: #{from_tag}..HEAD
    """)
  end

  defp single_version!([version]), do: version

  defp single_version!(_args) do
    fail!("Expected exactly one version argument, for example: elixir scripts/squidie_release_prep.exs 0.3.1")
  end

  defp validate_version!(version) do
    unless Regex.match?(~r/^\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.-]+)?$/, version) do
      fail!("Expected a SemVer version without the leading v, got: #{inspect(version)}")
    end
  end

  defp previous_version_tag!(version) do
    target_tag = "v#{version}"

    "git"
    |> run_git!(["tag", "--sort=-v:refname", "--merged", "HEAD"])
    |> String.split("\n", trim: true)
    |> Enum.find(fn tag ->
      tag != target_tag and Regex.match?(~r/^v?\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.-]+)?$/, tag)
    end)
    |> case do
      nil -> fail!("Could not find a previous version tag merged into HEAD")
      tag -> tag
    end
  end

  defp commits_since!(from_tag) do
    "git"
    |> run_git!(["log", "--reverse", "--format=%h%x09%s", "#{from_tag}..HEAD"])
    |> String.split("\n", trim: true)
    |> Enum.map(fn line ->
      case String.split(line, "\t", parts: 2) do
        [sha, subject] -> {sha, subject}
        [subject] -> {"", subject}
      end
    end)
    |> case do
      [] -> fail!("No commits found since #{from_tag}")
      commits -> commits
    end
  end

  defp run_git!(command, args) do
    case System.cmd(command, args, env: @git_env, stderr_to_stdout: true) do
      {output, 0} ->
        output

      {output, status} ->
        fail!("git #{Enum.join(args, " ")} failed with #{status}:\n#{output}")
    end
  end

  defp update_mix_version!(version) do
    update_file!("mix.exs", fn body ->
      replace_once!(
        body,
        ~r/version: "\d+\.\d+\.\d+(?:[-+][^"]+)?"/,
        ~s(version: "#{version}"),
        "mix.exs version"
      )
    end)
  end

  defp update_install_snippets!(version) do
    Enum.each(@version_files, fn path ->
      update_file!(path, fn body ->
        replace_all!(
          body,
          ~r/{:squidie, "~> \d+\.\d+\.\d+(?:[-+][^"]+)?"}/,
          ~s({:squidie, "~> #{version}"}),
          "#{path} install snippet"
        )
      end)
    end)
  end

  defp changelog_notes(commits) do
    commit_lines =
      Enum.map_join(commits, "\n", fn
        {"", subject} -> "- #{subject}"
        {sha, subject} -> "- `#{sha}` #{subject}"
      end)

    """
    ### Changed
    #{commit_lines}
    """
  end

  defp update_changelog!(version, date, notes) do
    update_file!("CHANGELOG.md", fn body ->
      if String.contains?(body, "## [#{version}]") do
        fail!("CHANGELOG.md already has a #{version} section")
      end

      section = """
      ## [#{version}] - #{date}

      #{String.trim_trailing(notes)}

      """

      replace_once!(body, ~r/\n## \[/, "\n#{section}## [", "CHANGELOG.md release insertion point")
    end)
  end

  defp write_notes_file(nil, _notes), do: :ok

  defp write_notes_file(path, notes) do
    path
    |> Path.dirname()
    |> File.mkdir_p!()

    File.write!(path, notes)
  end

  defp update_file!(path, fun) do
    body = File.read!(path)
    File.write!(path, fun.(body))
  end

  defp replace_once!(body, pattern, replacement, label) do
    if Regex.match?(pattern, body) do
      Regex.replace(pattern, body, replacement, global: false)
    else
      fail!("Could not find #{label}")
    end
  end

  defp replace_all!(body, pattern, replacement, label) do
    if Regex.match?(pattern, body) do
      Regex.replace(pattern, body, replacement)
    else
      fail!("Could not find #{label}")
    end
  end

  defp fail!(message) do
    IO.puts(:stderr, message)
    System.halt(1)
  end
end

Squidie.ReleasePrepScript.main(System.argv())
