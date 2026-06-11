[
  source: [
    forbidden_files: [
      "_build/**",
      "cover/**",
      "deps/**",
      "doc/**",
      "tmp/**"
    ]
  ],
  tests: [
    hints: [
      {"lib/squidie/runtime/**", ["test/squidie/runtime/**"]},
      {"lib/squidie/workflow/**", ["test/squidie/workflow/**"]},
      {"lib/squidie/read_model/**", ["test/squidie/read_model/**"]},
      {"lib/squidie/tools/**", ["test/squidie/tools/**"]}
    ]
  ]
]
