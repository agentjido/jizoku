# Used by "mix format"
locals_without_parens = [
  approval_step: 1,
  approval_step: 2,
  cron: 1,
  cron: 2,
  field: 2,
  field: 3,
  manual_review_step: 1,
  manual_review_step: 2,
  payload: 1,
  step: 2,
  step: 3,
  transition: 2,
  trigger: 2,
  version: 1,
  workflow: 1
]

[
  inputs: ["{mix,.formatter,.credo}.exs", "{config,lib,test}/**/*.{ex,exs}"],
  subdirectories: ["priv/*/migrations"],
  locals_without_parens: locals_without_parens,
  export: [
    locals_without_parens: locals_without_parens
  ]
]
