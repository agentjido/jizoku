[
  %{
    workflow: MinimalHostApp.Workflows.VersionedRouting,
    definition_version: "v1",
    definition_fingerprint: "c614f980554f330a7175faacaa1d592ede8bde136ff00accf0193fea4c281637",
    golden_history: %{
      schema_version: 1,
      workflow: "Elixir.MinimalHostApp.Workflows.VersionedRouting",
      queue: "minimal-host-workflow-evolution",
      partition: nil,
      status: :completed,
      terminal_status: :completed,
      events: [
        %{type: :run_started, offset_us: 0, run: "run-1", status: :running},
        %{
          type: :attempt_claimed,
          offset_us: 0,
          run: "run-1",
          step: "record_version",
          runnable: "runnable-1",
          status: :claimed,
          details: %{attempt_number: 1}
        },
        %{
          type: :attempt_completed,
          offset_us: 0,
          run: "run-1",
          step: "record_version",
          runnable: "runnable-1",
          status: :completed,
          details: %{attempt_number: 1}
        },
        %{
          type: :runnable_applied,
          offset_us: 0,
          run: "run-1",
          step: "record_version",
          runnable: "runnable-1",
          status: :applied
        },
        %{
          type: :attempt_scheduled,
          offset_us: 0,
          run: "run-1",
          step: "record_version",
          runnable: "runnable-1",
          status: :available,
          details: %{attempt_number: 1, visible_offset_us: 0}
        },
        %{type: :run_terminal, offset_us: 0, run: "run-1", status: :completed}
      ]
    }
  }
]
