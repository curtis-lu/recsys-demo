# Cache Source Tables Auto-Inject from Catalog Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate manual sync between `catalog.yaml` (HiveTableDataset.table) and `parameters_training.yaml` (`cache.source_tables`) by auto-deriving cache source tables from raw catalog config in `__main__.py:_run_pipeline`. Drops path A (yaml override) shipped at `054e3fc` — breaking change.

**Architecture:** `__main__.py` reads the YAML-loaded `catalog_config` dict (already in scope at line 108), extracts each `HiveTableDataset` cache entry's `table` field, mutates `substitution_params` with the auto-derived mapping under a private key `_cache_source_tables` (underscore signals "internal, not yaml-facing"). `_populate_cache_from_hive` reads from this key. The dataset abstraction is preserved — no public `.table` property on `HiveTableDataset`, no `isinstance` check on dataset instances; we operate on the raw config dict whose schema is already public.

**Tech Stack:** PySpark 3.3.2, pytest with unittest.mock.

**Spec:** Inline in conversation history 2026-05-08 (path B refactor of `054e3fc`). The original cache work spec is at `docs/superpowers/specs/2026-05-07-training-cache-hdfs-copy-design.md`.

**Breaking change**: yaml `cache.source_tables` (shipped at `054e3fc`) no longer has any effect. User has confirmed no production conf has this key set.

---

## Self-Review (done before TDD)

1. **Why `_cache_source_tables` (private prefix), not `cache.source_tables`?**
   The latter was path A's user-facing yaml knob (`054e3fc`). Reusing the same name would muddle "is this a deprecated yaml entry that still works, or auto-injected?". Underscore prefix makes the role clear: framework-internal, not yaml-facing.

2. **Why iterate `_CACHE_SOURCE_TABLE` keys (not iterate catalog config)?**
   `_CACHE_SOURCE_TABLE` is the cache layer's own declaration of "I know how to handle these logical names." Iterating catalog could pick up unrelated HiveTableDataset entries. Iterating `_CACHE_SOURCE_TABLE` keeps the cache layer in charge.

3. **Why `entry.get("type") == "HiveTableDataset"` string compare (not dataset class import)?**
   Importing `HiveTableDataset` to do `_DATASET_REGISTRY[entry["type"]] is HiveTableDataset` adds a runtime dep just for type check. The catalog yaml schema already locks this string ("HiveTableDataset" is what users type in yaml). String compare is appropriate, fragility is shared with the broader catalog config schema.

4. **Why mutate `parameters` instead of returning a new dict?**
   Matches `__main__.py`'s existing style (e.g. `runtime_params.pop(...)`, `substitution_params = {**params, **runtime_params}`). Tests assert mutation diff explicitly.

5. **Why unconditional call across all pipelines (not only training)?**
   The helper is no-op when the catalog has no cache entries (inference, evaluation, etc.). Conditional on `pipeline_name == "training"` would couple `__main__.py` to pipeline-specific knowledge it doesn't need.

6. **Keep `_CACHE_SOURCE_TABLE` fallback in `_populate_cache_from_hive`?**
   Yes — unit tests that exercise `_populate_cache_from_hive` directly don't go through `__main__.py`, so they don't get auto-injection. Fallback is the test-default. Production paths always have auto-injected values; fallback only kicks in for unit tests.

---

## File Structure

**Modified files:**
- `src/recsys_tfb/pipelines/training/nodes.py` — change read key in `_populate_cache_from_hive`; add `inject_cache_source_tables` helper
- `src/recsys_tfb/__main__.py` — import + call helper in `_run_pipeline` between line 108 and line 119
- `tests/test_pipelines/test_training/test_cache_nodes.py` — update `TestPopulateCacheFromHive::test_source_table_override_from_parameters` (rename key, reframe narrative); add new `TestInjectCacheSourceTables` class with 4 tests
- `tests/test_cli.py` — add 1 unit test asserting `_run_pipeline` calls `inject_cache_source_tables`

No new files.

---

## Tasks

### Task 1: Rename `parameters['cache']['source_tables']` → `parameters['_cache_source_tables']` in `_populate_cache_from_hive`

**Files:**
- Modify: `src/recsys_tfb/pipelines/training/nodes.py:71-95` (the `_populate_cache_from_hive` function)
- Modify: `tests/test_pipelines/test_training/test_cache_nodes.py` — `TestPopulateCacheFromHive::test_source_table_override_from_parameters` (line ~509-540)

- [ ] **Step 1: Update existing override test to use new key, reframe narrative**

In `tests/test_pipelines/test_training/test_cache_nodes.py`, replace the existing `test_source_table_override_from_parameters` method (the one added at `054e3fc`) with:

```python
    def test_auto_injected_source_tables_flow_to_get_hive_table_location(self, tmp_path):
        """parameters['_cache_source_tables'] (auto-injected by __main__.py from
        catalog config) is what _populate_cache_from_hive reads to resolve
        the actual Hive table name. This test pins the read path; the
        injection path is covered separately in TestInjectCacheSourceTables.
        """
        from recsys_tfb.pipelines.training.nodes import _populate_cache_from_hive

        params = self._params(tmp_path)
        params["_cache_source_tables"] = {
            "train_model_input": "recsys_prod_train_model_input"
        }

        with patch(
            "recsys_tfb.pipelines.training.nodes.get_hive_table_location",
            return_value="hdfs://nn/warehouse/ml_recsys.db/recsys_prod_train_model_input",
        ) as mock_loc, patch(
            "recsys_tfb.pipelines.training.nodes.copy_hdfs_to_local"
        ) as mock_copy:
            _populate_cache_from_hive(
                MagicMock(), "train_model_input", params, "/tmp/dst"
            )

        mock_loc.assert_called_once_with(ANY, "ml_recsys", "recsys_prod_train_model_input")
        mock_copy.assert_called_once_with(
            ANY,
            "hdfs://nn/warehouse/ml_recsys.db/recsys_prod_train_model_input"
            "/base_dataset_version=base_v1/train_variant_id=train_v1/snap_date=*",
            "/tmp/dst",
            glob=True,
        )
```

- [ ] **Step 2: Run test — verify it fails**

Run: `.venv/bin/pytest tests/test_pipelines/test_training/test_cache_nodes.py::TestPopulateCacheFromHive::test_auto_injected_source_tables_flow_to_get_hive_table_location -v`

Expected: FAIL — old code reads `parameters['cache']['source_tables']` (which is empty after rename), falls back to `_CACHE_SOURCE_TABLE['train_model_input']` (= `"train_model_input"`), so `get_hive_table_location` is called with `"train_model_input"` not `"recsys_prod_train_model_input"`.

- [ ] **Step 3: Update `_populate_cache_from_hive` to read new key**

In `src/recsys_tfb/pipelines/training/nodes.py:71-95`, replace the function body's docstring + override-read line. The full new function:

```python
def _populate_cache_from_hive(
    spark, dataset_name: str, parameters: dict, local_dst: str
) -> None:
    """Copy the relevant Hive partition subtree to driver-local fs.

    Local layout after copy:
        <local_dst>/snap_date=.../prod_name=.../*.parquet

    Source-table resolution:
      1. parameters['_cache_source_tables'][dataset_name] — auto-injected by
         __main__.py:_run_pipeline from catalog_config (HiveTableDataset.table).
         This is the production path and works across envs that prefix table
         names (e.g. company prod 'recsys_prod_train_model_input').
      2. _CACHE_SOURCE_TABLE[dataset_name] — fallback used by unit tests that
         don't go through __main__.py and therefore have no auto-injection.
    """
    db = parameters["hive"]["db"]
    source_tables = parameters.get("_cache_source_tables", {})
    table = source_tables.get(dataset_name, _CACHE_SOURCE_TABLE[dataset_name])
    location = get_hive_table_location(spark, db, table)
    outer = "/".join(
        f"{tok}={parameters[tok]}"
        for tok in _CACHE_OUTER_PARTITIONS[dataset_name]
    )
    src_glob = f"{location.rstrip('/')}/{outer}/snap_date=*"
    copy_hdfs_to_local(spark, src_glob, local_dst, glob=True)
```

- [ ] **Step 4: Run all cache-node tests — verify pass**

Run: `.venv/bin/pytest tests/test_pipelines/test_training/test_cache_nodes.py -v`

Expected: ALL PASSED — including the renamed test, plus the existing `TestPopulateCacheFromHive::test_train_model_input_constructs_correct_src_glob` etc. (those don't set source_tables in params, so they exercise the `_CACHE_SOURCE_TABLE` fallback — still works).

- [ ] **Step 5: Run full test suite for safety**

Run: `.venv/bin/pytest tests/ -q --ignore=tests/scenarios`

Expected: ALL PASSED (727).

- [ ] **Step 6: Commit**

```bash
git add src/recsys_tfb/pipelines/training/nodes.py tests/test_pipelines/test_training/test_cache_nodes.py
git commit -m "refactor(training): rename source_tables key to _cache_source_tables

Drops path A (yaml override at parameters.cache.source_tables shipped in
054e3fc). Read path renamed to private parameters._cache_source_tables.
__main__.py wiring (Task 2-3) auto-injects this key from catalog config,
removing the manual catalog/yaml sync requirement."
```

---

### Task 2: Add `inject_cache_source_tables` helper in nodes.py

**Files:**
- Modify: `src/recsys_tfb/pipelines/training/nodes.py` — add new function after `_populate_cache_from_hive`
- Modify: `tests/test_pipelines/test_training/test_cache_nodes.py` — add new `TestInjectCacheSourceTables` class

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_pipelines/test_training/test_cache_nodes.py`, after `TestPopulateCacheFromHive`:

```python
class TestInjectCacheSourceTables:
    """Auto-derivation of parameters['_cache_source_tables'] from raw catalog config.

    Operates on YAML-loaded catalog config dict (not DataCatalog instances),
    so we don't reach into HiveTableDataset's private attrs or do isinstance
    checks. The catalog yaml schema is already a public contract.
    """

    def _catalog_config_with_train_only(self):
        return {
            "train_model_input": {
                "type": "HiveTableDataset",
                "database": "${hive.db}",
                "table": "recsys_prod_train_model_input",
                "partition_filter": {"base_dataset_version": "abc"},
            },
            "feature_table": {
                "type": "HiveTableDataset",
                "database": "${hive.db}",
                "table": "recsys_prod_feature_table",
            },
        }

    def test_extracts_table_for_known_cache_names(self):
        from recsys_tfb.pipelines.training.nodes import inject_cache_source_tables

        params = {"hive": {"db": "ml_recsys"}}
        catalog_config = {
            "train_model_input": {
                "type": "HiveTableDataset",
                "table": "recsys_prod_train_model_input",
            },
            "val_model_input": {
                "type": "HiveTableDataset",
                "table": "recsys_prod_val_model_input",
            },
        }

        inject_cache_source_tables(params, catalog_config)

        assert params["_cache_source_tables"] == {
            "train_model_input": "recsys_prod_train_model_input",
            "val_model_input": "recsys_prod_val_model_input",
        }

    def test_skips_non_hive_table_datasets(self):
        from recsys_tfb.pipelines.training.nodes import inject_cache_source_tables

        params = {}
        catalog_config = {
            "train_model_input": {
                "type": "HiveTableDataset",
                "table": "recsys_prod_train_model_input",
            },
            "preprocessor": {
                "type": "JSONDataset",
                "filepath": "data/preprocessor.json",
            },
        }

        inject_cache_source_tables(params, catalog_config)

        assert params["_cache_source_tables"] == {
            "train_model_input": "recsys_prod_train_model_input",
        }

    def test_skips_cache_names_missing_from_catalog(self):
        from recsys_tfb.pipelines.training.nodes import inject_cache_source_tables

        params = {}
        # catalog_config only has train_model_input — val/cal/test missing
        catalog_config = {
            "train_model_input": {
                "type": "HiveTableDataset",
                "table": "recsys_prod_train_model_input",
            },
        }

        inject_cache_source_tables(params, catalog_config)

        # Only train_model_input is included; absent entries are silently skipped
        assert params["_cache_source_tables"] == {
            "train_model_input": "recsys_prod_train_model_input",
        }

    def test_no_op_when_no_cache_entries_in_catalog(self):
        from recsys_tfb.pipelines.training.nodes import inject_cache_source_tables

        params = {"hive": {"db": "ml_recsys"}}
        catalog_config = {
            "feature_table": {
                "type": "HiveTableDataset",
                "table": "recsys_prod_feature_table",
            },
        }

        inject_cache_source_tables(params, catalog_config)

        # No cache-relevant entries → don't write the key at all
        assert "_cache_source_tables" not in params
        # params dict otherwise untouched
        assert params == {"hive": {"db": "ml_recsys"}}
```

- [ ] **Step 2: Run tests — verify they fail**

Run: `.venv/bin/pytest tests/test_pipelines/test_training/test_cache_nodes.py::TestInjectCacheSourceTables -v`

Expected: FAIL — `ImportError: cannot import name 'inject_cache_source_tables'`.

- [ ] **Step 3: Implement `inject_cache_source_tables`**

In `src/recsys_tfb/pipelines/training/nodes.py`, after the `_populate_cache_from_hive` function (around line 96), add:

```python
def inject_cache_source_tables(parameters: dict, catalog_config: dict) -> None:
    """Auto-derive cache source_tables from catalog_config and write into parameters.

    Mutates `parameters` to add `_cache_source_tables` mapping (cache logical name
    → actual Hive table name). Cache nodes read this in _populate_cache_from_hive.

    For each known cache name in _CACHE_SOURCE_TABLE, look up the catalog entry.
    If present and `type: HiveTableDataset`, take its `table` field. Skips entries
    that aren't HiveTableDataset and missing entries.

    Operates on raw catalog_config dict (not DataCatalog instance) — the yaml
    schema is the public contract; we don't access dataset instance internals.

    No-op (does not write the key) when no cache entries match.

    Called by __main__.py:_run_pipeline before DataCatalog construction so the
    cache nodes see the auto-derived mapping at runtime.
    """
    auto: dict[str, str] = {}
    for cache_name in _CACHE_SOURCE_TABLE:
        entry = catalog_config.get(cache_name)
        if entry and entry.get("type") == "HiveTableDataset":
            table = entry.get("table")
            if table:
                auto[cache_name] = table
    if auto:
        parameters["_cache_source_tables"] = auto
```

- [ ] **Step 4: Run tests — verify pass**

Run: `.venv/bin/pytest tests/test_pipelines/test_training/test_cache_nodes.py::TestInjectCacheSourceTables -v`

Expected: 4 PASSED.

- [ ] **Step 5: Run full cache-node tests for safety**

Run: `.venv/bin/pytest tests/test_pipelines/test_training/test_cache_nodes.py -v`

Expected: ALL PASSED.

- [ ] **Step 6: Commit**

```bash
git add src/recsys_tfb/pipelines/training/nodes.py tests/test_pipelines/test_training/test_cache_nodes.py
git commit -m "feat(training): add inject_cache_source_tables helper

Auto-derives cache source_tables from raw catalog_config dict (yaml-loaded,
public schema). Avoids reaching into HiveTableDataset's private attrs or
doing isinstance checks on dataset instances. Wired into __main__.py
in next task."
```

---

### Task 3: Wire `inject_cache_source_tables` into `__main__.py:_run_pipeline`

**Files:**
- Modify: `src/recsys_tfb/__main__.py:108-119` area
- Modify: `tests/test_cli.py` — add 1 unit test

- [ ] **Step 1: Write the failing test**

In `tests/test_cli.py`, add a new test method (place it within an existing test class that already mocks `DataCatalog` and `Runner`, or add a new class — see the file's structure). The test:

```python
def test_run_pipeline_auto_injects_cache_source_tables(self):
    """_run_pipeline calls inject_cache_source_tables with substitution_params
    and catalog_config before constructing DataCatalog."""
    import recsys_tfb.__main__ as main_module

    # Minimal config object with the methods _run_pipeline calls
    fake_config = MagicMock()
    fake_config.get_catalog_config.return_value = {
        "train_model_input": {
            "type": "HiveTableDataset",
            "table": "recsys_prod_train_model_input",
        }
    }

    with patch("recsys_tfb.__main__.DataCatalog") as mock_catalog_cls, \
         patch("recsys_tfb.__main__.Runner"), \
         patch("recsys_tfb.__main__.get_pipeline"), \
         patch(
             "recsys_tfb.__main__.inject_cache_source_tables"
         ) as mock_inject:
        main_module._run_pipeline(
            pipeline_name="training",
            pipeline_kwargs={},
            runtime_params={},
            config=fake_config,
            params={},
            env="production",
        )

    # Helper called once with (substitution_params, catalog_config)
    assert mock_inject.call_count == 1
    args, kwargs = mock_inject.call_args
    injected_params, injected_catalog = args
    # catalog_config is the dict the config returned
    assert injected_catalog == {
        "train_model_input": {
            "type": "HiveTableDataset",
            "table": "recsys_prod_train_model_input",
        }
    }
    # substitution_params is a dict (we don't pin its content here — that's
    # tested via the helper's own tests)
    assert isinstance(injected_params, dict)
```

If the existing `tests/test_cli.py` doesn't import `MagicMock` / `patch`, add at the top:
```python
from unittest.mock import MagicMock, patch
```

- [ ] **Step 2: Run test — verify it fails**

Run: `.venv/bin/pytest tests/test_cli.py::test_run_pipeline_auto_injects_cache_source_tables -v` (or whatever class path you placed it under)

Expected: FAIL — `inject_cache_source_tables` not yet imported in `__main__.py`, so `patch("recsys_tfb.__main__.inject_cache_source_tables")` raises `AttributeError`.

- [ ] **Step 3: Wire into `__main__.py`**

In `src/recsys_tfb/__main__.py`, find the existing import block at the top (around line 8). Add:

```python
from recsys_tfb.pipelines.training.nodes import inject_cache_source_tables
```

Then in `_run_pipeline` between line 108 (where `catalog_config` is built) and line 119 (where `DataCatalog(catalog_config)` is called), add:

```python
catalog_config = config.get_catalog_config(runtime_params=substitution_params)

# Auto-inject cache source_tables from catalog config so cache nodes don't
# need a parallel parameters yaml mapping. Catalog.yaml's HiveTableDataset
# `table` field is the single source of truth.
inject_cache_source_tables(substitution_params, catalog_config)

# (existing inference symlink hack stays as-is)
if pipeline_name == "inference" and source_model_version is None:
    ...

catalog = DataCatalog(catalog_config)
```

- [ ] **Step 4: Run test — verify pass**

Run: `.venv/bin/pytest tests/test_cli.py::test_run_pipeline_auto_injects_cache_source_tables -v`

Expected: PASS.

- [ ] **Step 5: Run full test suite**

Run: `.venv/bin/pytest tests/ -q --ignore=tests/scenarios`

Expected: 728 PASSED (was 727 + 4 new in TestInjectCacheSourceTables - 0 deletions actually +4 from Task 2 + 1 from Task 3 = 732... let me recompute. Pre-Task 1: 727. After Task 1: 727 (rename test, count unchanged). After Task 2: 727 + 4 = 731. After Task 3: 731 + 1 = 732.)

- [ ] **Step 6: Commit**

```bash
git add src/recsys_tfb/__main__.py tests/test_cli.py
git commit -m "feat(cli): auto-inject cache source_tables from catalog config

_run_pipeline now mutates substitution_params with auto-derived
_cache_source_tables before DataCatalog instantiation. Catalog.yaml's
HiveTableDataset.table is the single source of truth for cache table
resolution; manual sync to parameters_training.yaml is no longer needed."
```

---

### Task 4: Manual smoke test on dev cluster

**Pre-requisites:**
- catalog.yaml on dev cluster has the prefixed names (e.g. `recsys_prod_train_model_input`) — user has already set this for prior testing
- `parameters_training.yaml` should NOT have any `cache.source_tables` or `cache._cache_source_tables` key (auto-inject is the only mechanism)

- [ ] **Step 1: Verify no stale yaml override lingers**

```bash
grep -n "source_tables" conf/base/parameters_training.yaml conf/base/catalog.yaml
```

Expected: no matches in `parameters_training.yaml`. (If user added one earlier, remove it.)

- [ ] **Step 2: Clean cache to force miss**

```bash
rm -rf data/recsys_cache/
```

- [ ] **Step 3: Run training (cache miss → should auto-resolve table names)**

```bash
source ~/dev-cluster/scripts/client-env.sh
export SPARK_CONF_DIR=~/dev-cluster/client-template-local/spark
PYTHONPATH=$(pwd)/src .venv/bin/python -m recsys_tfb training --env production
```

Expected:
- Logs show `cache_miss name=train_model_input ...`
- HDFS copy log shows the prefixed table location: `hdfs://.../recsys_prod_train_model_input/...`
- No errors about `usr_curtis_lu.train_model_input` not found
- Pipeline completes; `data/recsys_cache/<bdv>/.../train_model_input.parquet/_SUCCESS` exists

- [ ] **Step 4: Re-run training (cache hit)**

```bash
PYTHONPATH=$(pwd)/src .venv/bin/python -m recsys_tfb training --env production
```

Expected: `cache_hit` log lines, no HDFS copy logs, pipeline completes faster.

- [ ] **Step 5: Push and report**

If steps 3-4 pass, push the branch:

```bash
git push origin main
```

Otherwise, capture the error message and report back for debugging.

---

## Self-Review (post-plan)

**Spec coverage:**
- ✅ Path A砍: Task 1 (rename key)
- ✅ Path B implementation: Task 2 (helper) + Task 3 (wiring)
- ✅ Smoke test: Task 4

**Placeholder scan:** No TBDs, all code blocks complete with concrete content.

**Type consistency:** `inject_cache_source_tables(parameters: dict, catalog_config: dict) -> None` consistent across Task 2 and Task 3 callsites and tests.

**Scope check:** Single focused refactor; no scope creep into adjacent concerns (e.g. removing `_CACHE_SOURCE_TABLE` fallback, refactoring `__main__.py`'s pipeline dispatch, etc.).

**Breaking change documentation:** Plan header explicitly notes path A removal; commit messages note the breaking change.
