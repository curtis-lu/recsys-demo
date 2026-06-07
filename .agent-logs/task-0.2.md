# Task 0.2 Progress Log

## 2026-06-07 Start

### Initial state exploration
- Worktree: /Users/curtislu/projects/recsys_tfb/.worktrees/two-stage-stacking
- Task: Point evaluation + config at the shared resolver (core.categories)

### Step 1: Added delegation regression test to tests/test_core/test_categories.py
- Appended test_evaluation_build_mapping_delegates

### Step 2: Confirmed test FAILS before implementation
- Command: pytest tests/test_core/test_categories.py::test_evaluation_build_mapping_delegates -q
- Result: FAIL (fund_a mapping not resolved from top-level product_categories)

### Step 3: Rewrote _build_category_mapping in metrics_spark.py
- Replaced full body with delegation to resolve_category_mapping from core.categories
- Enabled-toggle check stays in evaluation.product_categories.enabled
- Confirmed delegation test now PASSES

### Step 4: Added top-level product_categories block to conf/base/parameters.yaml
- Product names match parameters_evaluation.yaml: fund_stock/fund_bond/fund_mix, exchange_fx/exchange_usd, ccard_bill/ccard_cash/ccard_ins

### Step 5: Trimmed conf/base/parameters_evaluation.yaml
- Removed mapping: and unmapped: from evaluation.product_categories
- Kept enabled: true with comment noting mapping is from top-level

### Step 6: Reconciled all affected tests
- tests/test_evaluation/test_metrics_spark_category.py: moved mapping/unmapped to top-level product_categories in _params()
- tests/test_evaluation/test_metrics_spark_category.py: fixed test_unknown_product_in_mapping_fails_loud to mutate p["product_categories"]["mapping"] not p["evaluation"]...
- tests/test_evaluation/test_metrics_spark_orchestrator.py: moved mapping/unmapped to top-level product_categories in _params()
- tests/test_evaluation/test_parameters_evaluation_yaml.py: updated test_product_categories_block to assert mapping absent from eval yaml and present in top-level parameters.yaml
- tests/test_evaluation/test_report_builder.py: moved mapping to p["product_categories"] in test_category_section_has_composition_table
- src/recsys_tfb/evaluation/report_builder.py: updated build_category_section to read mapping from parameters["product_categories"] (top-level) instead of evaluation.product_categories

### Test runs
- Non-Spark tests (test_categories, test_parameters_evaluation_yaml, test_report_builder, test_comparison_report): 52 passed
- Spark tests (test_metrics_spark_category, test_metrics_spark_orchestrator): 7 passed
- All: PASS
