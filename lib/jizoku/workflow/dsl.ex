defmodule Jizoku.Workflow.Dsl do
  @moduledoc """
  Spark DSL wrapper for Jizoku workflow declarations.

  This module installs the Jizoku Spark extension used by `use
  Jizoku.Workflow`. Keeping the wrapper small lets the public workflow module
  focus on compiling validated definitions while Spark owns the declaration
  metadata.
  """

  use Spark.Dsl,
    default_extensions: [
      extensions: [Jizoku.Workflow.SparkExtension]
    ]
end
