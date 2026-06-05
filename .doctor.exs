%Doctor.Config{
  ignore_modules: [],
  ignore_paths: [~r/^test\/support\//],
  min_module_doc_coverage: 60,
  min_module_spec_coverage: 40,
  min_overall_doc_coverage: 95,
  min_overall_moduledoc_coverage: 100,
  min_overall_spec_coverage: 95,
  exception_moduledoc_required: true,
  raise: true,
  reporter: Doctor.Reporters.Summary,
  struct_type_spec_required: true,
  umbrella: false
}
