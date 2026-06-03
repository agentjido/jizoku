defmodule Squidie.Workflow.Dsl do
  @moduledoc """
  Spark DSL wrapper for Squidie workflow declarations.

  This module installs the Squidie Spark extension used by `use
  Squidie.Workflow`. Keeping the wrapper small lets the public workflow module
  focus on compiling validated definitions while Spark owns the declaration
  metadata.
  """

  use Spark.Dsl,
    default_extensions: [
      extensions: [Squidie.Workflow.SparkExtension]
    ]
end
