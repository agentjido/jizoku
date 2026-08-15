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
      {"lib/jizoku/runtime/**", ["test/jizoku/runtime/**"]},
      {"lib/jizoku/workflow/**", ["test/jizoku/workflow/**"]},
      {"lib/jizoku/read_model/**", ["test/jizoku/read_model/**"]},
      {"lib/jizoku/tools/**", ["test/jizoku/tools/**"]}
    ]
  ]
]
