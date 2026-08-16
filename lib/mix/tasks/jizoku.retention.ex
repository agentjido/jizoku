defmodule Mix.Tasks.Jizoku.Retention do
  @moduledoc """
  Previews retention deletion and applies only an exact confirmed plan.

      mix jizoku.retention --terminal-before 2025-01-01T00:00:00Z
      mix jizoku.retention --terminal-before 2025-01-01T00:00:00Z \
        --created-at 2026-08-16T23:00:00Z --apply \
        --confirmation PLAN_TOKEN

  Preview is the default and never deletes state. Applying requires the
  original preview timestamp and confirmation token. The runtime rechecks token
  expiry and every source revision against the current database state.
  """

  @shortdoc "Previews or explicitly applies partition-scoped retention"

  use Mix.Task

  alias Jizoku.Operations.CLI
  alias Jizoku.Retention.Plan
  alias Jizoku.Retention.Receipt

  @switches [
    terminal_before: :string,
    statuses: :string,
    limit: :integer,
    partition: :string,
    created_at: :string,
    apply: :boolean,
    confirmation: :string,
    json: :boolean
  ]
  @statuses %{
    "cancelled" => :cancelled,
    "completed" => :completed,
    "continued" => :continued,
    "failed" => :failed
  }

  @impl Mix.Task
  def run(args) do
    opts = CLI.parse_options!("jizoku.retention", args, @switches)
    validate_mode!(opts)
    filters = retention_filters!(opts)
    preview_opts = preview_options!(opts)

    case CLI.run(fn -> retention_operation(filters, preview_opts, opts) end) do
      {:ok, report} -> render(report, opts)
      {:error, reason} -> Mix.raise("Retention operation failed: #{CLI.format_error(reason)}")
    end
  end

  defp retention_operation(filters, preview_opts, opts) do
    with {:ok, %Plan{} = plan} <- Jizoku.preview_retention(filters, preview_opts) do
      finish_operation(plan, preview_opts, opts)
    end
  end

  defp finish_operation(plan, preview_opts, opts) do
    if Keyword.get(opts, :apply, false) do
      apply_plan(plan, preview_opts, Keyword.fetch!(opts, :confirmation))
    else
      {:ok, %{mode: :preview, plan: plan_report(plan)}}
    end
  end

  defp apply_plan(plan, preview_opts, confirmation) do
    apply_opts = Keyword.delete(preview_opts, :now)

    with {:ok, %Receipt{} = receipt} <-
           Jizoku.apply_retention(plan, confirmation, apply_opts) do
      {:ok, %{mode: :apply, plan: plan_report(plan), receipt: receipt_report(receipt)}}
    end
  end

  defp retention_filters!(opts) do
    terminal_before = required_datetime!(opts, :terminal_before)

    [terminal_before: terminal_before]
    |> put_filter(:statuses, parse_statuses!(Keyword.get(opts, :statuses)))
    |> put_filter(:limit, Keyword.get(opts, :limit))
  end

  defp preview_options!(opts) do
    created_at =
      case Keyword.get(opts, :created_at) do
        nil -> DateTime.utc_now()
        value -> parse_datetime!(value, :created_at)
      end

    put_filter([now: created_at], :partition, Keyword.get(opts, :partition))
  end

  defp validate_mode!(opts) do
    apply? = Keyword.get(opts, :apply, false)
    confirmation = Keyword.get(opts, :confirmation)
    created_at = Keyword.get(opts, :created_at)

    validate_confirmation_mode!(apply?, confirmation)
    validate_created_at_mode!(apply?, created_at)
  end

  defp validate_confirmation_mode!(true, confirmation) when not is_binary(confirmation) do
    Mix.raise("jizoku.retention --apply requires --confirmation")
  end

  defp validate_confirmation_mode!(false, confirmation) when is_binary(confirmation) do
    Mix.raise("jizoku.retention --confirmation requires --apply")
  end

  defp validate_confirmation_mode!(_apply?, _confirmation) do
    :ok
  end

  defp validate_created_at_mode!(true, created_at) when not is_binary(created_at) do
    Mix.raise("jizoku.retention --apply requires the preview --created-at timestamp")
  end

  defp validate_created_at_mode!(false, created_at) when is_binary(created_at) do
    Mix.raise("jizoku.retention --created-at requires --apply")
  end

  defp validate_created_at_mode!(_apply?, created_at) do
    validate_created_at!(created_at)
  end

  defp validate_created_at!(nil) do
    :ok
  end

  defp validate_created_at!(value) do
    created_at = parse_datetime!(value, :created_at)

    if DateTime.compare(created_at, DateTime.utc_now()) == :gt do
      Mix.raise("Invalid jizoku.retention --created-at timestamp")
    end
  end

  defp required_datetime!(opts, key) do
    case Keyword.get(opts, key) do
      value when is_binary(value) -> parse_datetime!(value, key)
      _missing -> Mix.raise("jizoku.retention requires --#{option_name(key)}")
    end
  end

  defp parse_datetime!(value, key) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} -> datetime
      {:error, _reason} -> Mix.raise("Invalid jizoku.retention --#{option_name(key)} timestamp")
    end
  end

  defp parse_statuses!(nil) do
    nil
  end

  defp parse_statuses!(value) do
    statuses =
      value
      |> String.split(",", trim: true)
      |> Enum.map(&Map.get(@statuses, &1))

    if statuses != [] and Enum.all?(statuses, &is_atom/1) and
         length(statuses) == length(Enum.uniq(statuses)) do
      statuses
    else
      Mix.raise("Invalid jizoku.retention --statuses value")
    end
  end

  defp plan_report(%Plan{} = plan) do
    %{
      version: plan.version,
      partition: plan.partition,
      created_at: DateTime.to_iso8601(plan.created_at),
      expires_at: DateTime.to_iso8601(plan.expires_at),
      terminal_before: DateTime.to_iso8601(plan.terminal_before),
      statuses: plan.statuses,
      limit: plan.limit,
      catalog_revision: plan.catalog_revision,
      eligible: Enum.map(plan.eligible, &candidate_report/1),
      blocked: Enum.map(plan.blocked, &blocked_report/1),
      confirmation_token: plan.confirmation_token
    }
  end

  defp candidate_report(candidate) do
    candidate
    |> Map.from_struct()
    |> Map.update!(:terminal_at, &DateTime.to_iso8601/1)
    |> Map.update!(:archived_at, &DateTime.to_iso8601/1)
  end

  defp blocked_report(blocked) do
    blocked
    |> Map.from_struct()
    |> Map.update!(:terminal_at, &DateTime.to_iso8601/1)
  end

  defp receipt_report(%Receipt{} = receipt) do
    receipt
    |> Map.from_struct()
    |> Map.update!(:applied_at, &DateTime.to_iso8601/1)
  end

  defp render(report, opts) do
    if Keyword.get(opts, :json, false) do
      Mix.shell().info(Jason.encode!(report))
    else
      render_text(report)
    end
  end

  defp render_text(%{mode: :preview, plan: plan}) do
    Mix.shell().info(
      "Retention preview created_at=#{plan.created_at} expires_at=#{plan.expires_at}"
    )

    render_partition(plan.partition)
    Mix.shell().info("Eligible: #{length(plan.eligible)}  Blocked: #{length(plan.blocked)}")

    Enum.each(plan.eligible, fn candidate ->
      Mix.shell().info(
        "eligible run=#{candidate.run_id} status=#{candidate.terminal_status} " <>
          "run_entries=#{candidate.run_revision} dispatch_entries=#{candidate.dispatch_entry_count}"
      )

      Mix.shell().info(
        "affected run_thread=#{candidate.affected.run_thread} " <>
          "run_checkpoint=#{candidate.affected.run_checkpoint} " <>
          "dispatch_thread=#{candidate.affected.dispatch_thread} " <>
          "catalog_thread=#{candidate.affected.catalog_thread} " <>
          "workflow_index_thread=#{candidate.affected.workflow_index_thread}"
      )
    end)

    Enum.each(plan.blocked, fn blocked ->
      Mix.shell().info(
        "blocked run=#{blocked.run_id} reasons=#{Enum.map_join(blocked.reasons, ",", &to_string/1)}"
      )
    end)

    Mix.shell().info("Confirmation: #{plan.confirmation_token}")

    Mix.shell().info(
      "Apply requires --created-at #{plan.created_at} --apply --confirmation TOKEN"
    )
  end

  defp render_text(%{mode: :apply, receipt: receipt}) do
    Mix.shell().info(
      "Retention applied runs=#{receipt.run_count} run_entries=#{receipt.run_entries_deleted} " <>
        "dispatch_entries=#{receipt.dispatch_entries_deleted} idempotent=#{receipt.idempotent?}"
    )
  end

  defp render_partition(nil) do
    :ok
  end

  defp render_partition(partition) do
    Mix.shell().info("Partition: #{partition}")
  end

  defp put_filter(filters, _key, nil) do
    filters
  end

  defp put_filter(filters, key, value) do
    Keyword.put(filters, key, value)
  end

  defp option_name(key) do
    key
    |> Atom.to_string()
    |> String.replace("_", "-")
  end
end
