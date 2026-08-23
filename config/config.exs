import Config

if config_env() in [:dev, :test] do
  config :git_hooks,
    auto_install: false,
    hooks: [
      commit_msg: [
        tasks: [
          {:cmd, "mix git_ops.check_message", include_hook_args: true}
        ]
      ]
    ]
end

if config_env() == :dev do
  config :git_ops,
    mix_project: Jizoku.MixProject,
    changelog_file: "CHANGELOG.md",
    repository_url: "https://github.com/agentjido/jizoku",
    manage_mix_version?: true,
    version_tag_prefix: "v",
    types: [
      feat: [header: "Features"],
      fix: [header: "Bug Fixes"],
      perf: [header: "Performance"],
      refactor: [header: "Refactoring"],
      build: [hidden?: true],
      chore: [hidden?: true],
      ci: [hidden?: true],
      docs: [hidden?: true],
      revert: [hidden?: true],
      style: [hidden?: true],
      test: [hidden?: true]
    ]
end

import_config "#{config_env()}.exs"
