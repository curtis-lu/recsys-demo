# Encode-aware sample_weight lookup — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a categorical *feature* column (e.g. `cust_segment_typ_2a`) be used directly as a `sample_weight_keys` component with human-readable string values in config, by translating the weight table into the parquet's encoded space at weight-resolution time.

**Architecture:** The model_input parquet stores feature categoricals as int codes (`category_mappings[col]` index) but identity categoricals raw. Instead of decoding every data row, translate the *small* `sample_weights` table's keys into parquet space per component (feature→code, identity/label/raw→unchanged), then reuse the existing `_compute_row_weights`. Widen consistency gate A9a to accept declared categorical columns. Add a non-cache-gated training node that writes a data-driven `unmatched_keys` diagnostic into the model dir and surfaces it in `manifest.json`.

**Tech Stack:** Python 3.10, pandas, numpy, pyarrow, pytest. No Spark in the new code paths (reads the already-materialized local train parquet via pyarrow).

**Spec:** `docs/superpowers/specs/2026-06-01-encode-aware-sample-weight-design.md`

**Branch:** `feat/encode-aware-sample-weight` (spec already committed there).

---

## File Structure

- `src/recsys_tfb/io/extract.py` — add pure `_translate_weight_table`; make `_row_weights_from_pdf` encode-aware (gains `preprocessor_metadata`); thread it through `extract_Xy` / `extract_Xy_with_groups` call sites.
- `src/recsys_tfb/core/consistency.py` — widen A9a `weight_key_columns_unavailable` to allow declared categorical columns.
- `src/recsys_tfb/pipelines/training/nodes.py` — add pure `resolve_weight_diagnostics` + node `persist_sample_weight_report`.
- `src/recsys_tfb/pipelines/training/pipeline.py` — wire the new node.
- `src/recsys_tfb/__main__.py` — factor `_sample_weight_extra` + merge into the training manifest.
- `conf/base/parameters_training.yaml` — update the `sample_weight_keys` comment (feature categoricals now allowed).
- Tests: `tests/test_io/test_extract.py`, `tests/test_core/test_consistency.py`, `tests/test_pipelines/test_training/test_nodes.py`.

Reference facts (verified):
- feature encoding: `_encode_categoricals` (`preprocessing/_spark.py:103`) → `code = enumerate(category_mappings[col])`, unknown → -1. Identity cats: `extract.py` deferred `pd.Categorical(..., categories=known).codes` (same scheme).
- categorical columns config path: `parameters["dataset"]["prepare_model_input"]["categorical_columns"]`, default `[schema["item"]]` (`preprocessing/_common.py::_get_preprocessing_config`).
- manifest: `__main__.training()` post-run builds `version_dir = data_dir/"models"/mv`; `_write_pipeline_manifest(..., extra_metadata=...)` merges extra into `manifest.json`; `_dir_artifacts(version_dir)` lists files in the version dir root.
- `diagnostics_dir(parameters)` (`pipelines/training/diagnostics.py`) → `data/models/<model_version>/diagnostics/`; its `.parent` is the version dir.

---

## Task 1: Pure `_translate_weight_table`

**Files:**
- Modify: `src/recsys_tfb/io/extract.py` (add function near `_compute_row_weights`)
- Test: `tests/test_io/test_extract.py`

- [ ] **Step 1: Write the failing test**

Add after `class TestComputeRowWeights` in `tests/test_io/test_extract.py`:

```python
from recsys_tfb.io.extract import _translate_weight_table


class TestTranslateWeightTable:
    # category_mappings: code = list index. seg "mass"->0, "hnw"->1, "aff"->2.
    CM = {"cust_segment_typ_2a": ["mass", "hnw", "aff"]}
    ID = ["snap_date", "cust_id", "prod_name"]

    def test_feature_component_translated_to_code(self):
        t, unk = _translate_weight_table(
            {"hnw": 2.0}, ["cust_segment_typ_2a"], self.CM, self.ID)
        assert t == {"1": 2.0} and unk == {}

    def test_identity_component_passthrough(self):
        t, unk = _translate_weight_table(
            {"ccard_ins": 3.0}, ["prod_name"], self.CM, self.ID)
        assert t == {"ccard_ins": 3.0} and unk == {}

    def test_mixed_composite_feature_plus_identity(self):
        t, unk = _translate_weight_table(
            {"mass|ccard_ins": 2.0}, ["cust_segment_typ_2a", "prod_name"],
            self.CM, self.ID)
        assert t == {"0|ccard_ins": 2.0} and unk == {}

    def test_unknown_feature_value_dropped_and_recorded(self):
        t, unk = _translate_weight_table(
            {"afflunet": 2.0}, ["cust_segment_typ_2a"], self.CM, self.ID)
        assert t == {} and unk == {"cust_segment_typ_2a": ["afflunet"]}

    def test_arity_mismatch_passthrough(self):
        t, unk = _translate_weight_table(
            {"mass|x|y": 2.0}, ["cust_segment_typ_2a"], self.CM, self.ID)
        assert t == {"mass|x|y": 2.0} and unk == {}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_io/test_extract.py::TestTranslateWeightTable -q`
Expected: FAIL — `ImportError: cannot import name '_translate_weight_table'`.

- [ ] **Step 3: Write minimal implementation**

In `src/recsys_tfb/io/extract.py`, add immediately above `_compute_row_weights`:

```python
def _translate_weight_table(
    sample_weights: dict,
    weight_keys: list,
    category_mappings: dict,
    identity_columns: list,
) -> tuple[dict, dict]:
    """Translate config sample_weights keys into the parquet's encoded space.

    A component whose column is an *encoded feature* (in ``category_mappings``
    and NOT an identity column — identity cats are stored raw in model_input) is
    mapped from its human-readable value to ``str(index)`` in
    ``category_mappings[col]`` (matching ``_encode_categoricals``). Identity /
    label / carry / numeric components pass through unchanged. A key with any
    unknown feature value is dropped (cannot match) and recorded.

    Returns ``(translated, unknown_values)``; ``unknown_values`` maps a weight-key
    column to the sorted config values absent from its mapping.
    """
    identity = set(identity_columns)
    code_of: dict[str, dict[str, str]] = {}
    for col in weight_keys:
        if col in category_mappings and col not in identity:
            code_of[col] = {
                str(cat): str(i) for i, cat in enumerate(category_mappings[col])
            }

    translated: dict = {}
    unknown: dict[str, list] = {}
    for key, weight in sample_weights.items():
        parts = str(key).split("|")
        if len(parts) != len(weight_keys):
            # arity is enforced by A9b at the config gate; keep as-is defensively.
            translated[str(key)] = weight
            continue
        out_parts: list[str] = []
        bad = False
        for part, col in zip(parts, weight_keys):
            if col in code_of:
                code = code_of[col].get(part)
                if code is None:
                    unknown.setdefault(col, []).append(part)
                    bad = True
                else:
                    out_parts.append(code)
            else:
                out_parts.append(part)
        if not bad:
            translated["|".join(out_parts)] = weight
    return translated, {c: sorted(set(v)) for c, v in unknown.items()}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m pytest tests/test_io/test_extract.py::TestTranslateWeightTable -q`
Expected: PASS (5 passed).

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/io/extract.py tests/test_io/test_extract.py
git commit -m "feat(training): add _translate_weight_table for encode-aware weights"
```

---

## Task 2: Encode-aware `_row_weights_from_pdf` + thread preprocessor_metadata

**Files:**
- Modify: `src/recsys_tfb/io/extract.py` (`_row_weights_from_pdf`, plus call sites in `extract_Xy` ~line 197 and `extract_Xy_with_groups` ~line 273)
- Test: `tests/test_io/test_extract.py`

- [ ] **Step 1: Write the failing test**

In `tests/test_io/test_extract.py`, update `class TestRowWeightsObservability` (added in PR #53) so every `_row_weights_from_pdf(...)` call passes a third arg, and add encode-aware cases. Replace the `_params`/calls helper usage by adding a `_prep` helper and new tests:

```python
class TestRowWeightsEncodeAware:
    # cust_segment_typ_2a is an encoded feature: pdf stores int codes.
    def _pdf(self):
        return pd.DataFrame({
            "cust_segment_typ_2a": [0, 1, 0, 2],  # codes for mass/hnw/mass/aff
            "prod_name": ["a", "a", "b", "a"],
            "label": [1, 0, 1, 0],
        })

    def _params(self, weights, keys):
        return {
            "schema": {"columns": {"time": "snap_date", "entity": ["cust_id"],
                                   "item": "prod_name", "label": "label"}},
            "training": {"sample_weights": weights, "sample_weight_keys": keys},
        }

    def _prep(self):
        # identity cats stay raw; feature cat carries a code mapping.
        return {"category_mappings": {"cust_segment_typ_2a": ["mass", "hnw", "aff"]}}

    def test_feature_key_translated_and_applied(self):
        from recsys_tfb.io.extract import _row_weights_from_pdf
        w = _row_weights_from_pdf(
            self._pdf(), self._params({"hnw": 5.0}, ["cust_segment_typ_2a"]),
            self._prep())
        # only the single hnw row (code 1) is boosted
        np.testing.assert_array_equal(w, np.array([1.0, 5.0, 1.0, 1.0]))

    def test_composite_feature_plus_identity(self):
        from recsys_tfb.io.extract import _row_weights_from_pdf
        w = _row_weights_from_pdf(
            self._pdf(),
            self._params({"mass|a": 2.0}, ["cust_segment_typ_2a", "prod_name"]),
            self._prep())
        # mass(code 0) & prod a -> rows 0 only (row2 is mass|b, row3 is aff|a)
        np.testing.assert_array_equal(w, np.array([2.0, 1.0, 1.0, 1.0]))

    def test_unknown_feature_value_warns_and_all_ones(self, caplog):
        from recsys_tfb.io.extract import _row_weights_from_pdf
        with caplog.at_level(logging.WARNING, logger="recsys_tfb.io.extract"):
            w = _row_weights_from_pdf(
                self._pdf(),
                self._params({"afflunet": 2.0}, ["cust_segment_typ_2a"]),
                self._prep())
        np.testing.assert_array_equal(w, np.ones(4))
        assert any("unknown category value" in r.getMessage()
                   for r in caplog.records if r.levelname == "WARNING")
```

Also update the existing `TestRowWeightsObservability` methods: every `_row_weights_from_pdf(self._pdf(), self._params(...))` call becomes `_row_weights_from_pdf(self._pdf(), self._params(...), {})` (empty preprocessor_metadata → no feature translation, identical behavior).

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_io/test_extract.py::TestRowWeightsEncodeAware -q`
Expected: FAIL — `_row_weights_from_pdf() takes 2 positional arguments but 3 were given`.

- [ ] **Step 3: Write minimal implementation**

In `src/recsys_tfb/io/extract.py`, replace the body of `_row_weights_from_pdf` (keep the docstring intent) with the encode-aware version:

```python
def _row_weights_from_pdf(
    pdf: pd.DataFrame, parameters: dict, preprocessor_metadata: dict,
) -> np.ndarray:
    training = parameters.get("training", {}) or {}
    sw = training.get("sample_weights") or {}
    weight_keys = training.get("sample_weight_keys") or [get_schema(parameters)["item"]]
    n_rows = len(pdf)

    missing = [k for k in weight_keys if k not in pdf.columns]
    if not sw or missing:
        reason = (
            "sample_weights table is empty" if not sw
            else f"weight-key column(s) {missing} absent from parquet"
        )
        logger.info(
            "sample_weight INACTIVE — all %d rows weight=1.0 (%s); "
            "weight_keys=%s n_weight_entries=%d",
            n_rows, reason, weight_keys, len(sw),
        )
        return np.ones(n_rows, dtype=np.float64)

    category_mappings = (preprocessor_metadata or {}).get("category_mappings", {}) or {}
    identity_cols = get_schema(parameters)["identity_columns"]
    translated, unknown = _translate_weight_table(
        sw, weight_keys, category_mappings, identity_cols)
    if unknown:
        logger.warning(
            "sample_weight: unknown category value(s) %s — those entries cannot "
            "match any row (left at weight 1.0).", unknown,
        )

    w = _compute_row_weights(pdf, weight_keys, translated)
    n_adjusted = int((w != 1.0).sum())
    if n_adjusted == 0:
        sample_data_keys = (
            _composite_key_series(pdf, weight_keys).drop_duplicates().head(5).tolist()
        )
        logger.warning(
            "sample_weight matched 0 of %d rows — weight_keys=%s; sample "
            "configured keys=%s; sample data keys=%s",
            n_rows, weight_keys, sorted(map(str, sw))[:5], sample_data_keys,
        )
    else:
        logger.info(
            "sample_weight ACTIVE — weight_keys=%s n_weight_entries=%d; "
            "rows_total=%d rows_adjusted=%d (%.2f%%); weight min/mean/max=%.3f/%.4f/%.3f",
            weight_keys, len(sw), n_rows, n_adjusted,
            100.0 * n_adjusted / n_rows if n_rows else 0.0,
            float(w.min()), float(w.mean()), float(w.max()),
        )
    return w
```

Then update the two call sites:
- In `extract_Xy` (the `if with_weights:` block, ~line 197): change `w = _row_weights_from_pdf(pdf, parameters)` → `w = _row_weights_from_pdf(pdf, parameters, preprocessor_metadata)`.
- In `extract_Xy_with_groups` (the `if with_weights:` block, ~line 273): same change.

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_io/test_extract.py -q`
Expected: PASS (all extract tests, incl. updated `TestRowWeightsObservability` and new `TestRowWeightsEncodeAware`).

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/io/extract.py tests/test_io/test_extract.py
git commit -m "feat(training): encode-aware _row_weights_from_pdf (feature categorical keys)"
```

---

## Task 3: Widen A9a to accept declared categorical columns

**Files:**
- Modify: `src/recsys_tfb/core/consistency.py` (`weight_key_columns_unavailable`)
- Test: `tests/test_core/test_consistency.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_core/test_consistency.py` (near the existing weight predicate tests; reuse the module's `_base()` / helper style):

```python
def test_a9a_feature_categorical_is_available():
    from recsys_tfb.core.consistency import weight_key_columns_unavailable
    p = _base()
    p["dataset"] = {"prepare_model_input": {"categorical_columns":
                    ["prod_name", "cust_segment_typ_2a"]}}
    p["training"] = {"sample_weight_keys": ["cust_segment_typ_2a", "prod_name"]}
    assert weight_key_columns_unavailable(p) == []


def test_a9a_non_categorical_feature_still_blocked():
    from recsys_tfb.core.consistency import weight_key_columns_unavailable
    p = _base()
    p["dataset"] = {"prepare_model_input": {"categorical_columns": ["prod_name"]}}
    p["training"] = {"sample_weight_keys": ["some_numeric_feature"]}
    assert weight_key_columns_unavailable(p) == ["some_numeric_feature"]
```

(If `_base()` does not already provide a `schema` with identity/label, set `p["schema"]` to match the file's existing weight tests — mirror `test_a9_*` setup in the same file.)

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_core/test_consistency.py -k a9a_feature_categorical -q`
Expected: FAIL — `cust_segment_typ_2a` reported as unavailable (not yet in `available`).

- [ ] **Step 3: Write minimal implementation**

In `src/recsys_tfb/core/consistency.py`, in `weight_key_columns_unavailable`, extend `available`:

```python
    dataset_cfg = parameters.get("dataset", {}) or {}
    categorical_cols = (
        (dataset_cfg.get("prepare_model_input", {}) or {}).get("categorical_columns")
        or [schema["item"]]
    )
    available = (
        set(schema["identity_columns"])
        | {schema["label"]}
        | set(dataset_cfg.get("carry_columns") or [])
        | set(categorical_cols)
    )
```

Update the function docstring's "must therefore be one of identity ∪ {label} ∪ carry_columns" line to add "∪ declared categorical columns (encode-aware lookup translates these at runtime)".

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_core/test_consistency.py -q`
Expected: PASS (new A9a tests + existing consistency tests, incl. the `test_a9_*` weight tests).

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/core/consistency.py tests/test_core/test_consistency.py
git commit -m "feat(training): A9a accepts categorical features as weight keys"
```

---

## Task 4: Pure `resolve_weight_diagnostics`

**Files:**
- Modify: `src/recsys_tfb/pipelines/training/nodes.py` (add function; ensure `import json` at top)
- Test: `tests/test_pipelines/test_training/test_nodes.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_pipelines/test_training/test_nodes.py`:

```python
def test_resolve_weight_diagnostics_unmatched(tmp_path):
    import pandas as pd
    from recsys_tfb.io.handles import ParquetHandle
    from recsys_tfb.pipelines.training.nodes import resolve_weight_diagnostics

    # train parquet: feature seg stored as int codes (0=mass,1=hnw), prod raw.
    pdf = pd.DataFrame({
        "cust_segment_typ_2a": [0, 1, 0],
        "prod_name": ["a", "a", "b"],
        "label": [1, 0, 1],
    })
    p = tmp_path / "train.parquet"
    pdf.to_parquet(p)
    handle = ParquetHandle(path=str(p))

    params = {
        "schema": {"columns": {"time": "snap_date", "entity": ["cust_id"],
                               "item": "prod_name", "label": "label"}},
        "training": {"sample_weight_keys": ["cust_segment_typ_2a"],
                     "sample_weights": {"mass": 2.0, "aff": 3.0}},  # aff absent
    }
    prep = {"category_mappings": {"cust_segment_typ_2a": ["mass", "hnw", "aff"]}}

    diag = resolve_weight_diagnostics(handle, params, prep)
    assert diag["enabled"] is True
    assert diag["weight_keys"] == ["cust_segment_typ_2a"]
    assert diag["n_weight_entries"] == 2
    assert diag["unmatched_keys"] == ["aff"]  # no row has segment 'aff'


def test_resolve_weight_diagnostics_disabled(tmp_path):
    import pandas as pd
    from recsys_tfb.io.handles import ParquetHandle
    from recsys_tfb.pipelines.training.nodes import resolve_weight_diagnostics
    p = tmp_path / "t.parquet"
    pd.DataFrame({"prod_name": ["a"], "label": [1]}).to_parquet(p)
    params = {"schema": {"columns": {"time": "snap_date", "entity": ["cust_id"],
              "item": "prod_name", "label": "label"}}, "training": {}}
    diag = resolve_weight_diagnostics(ParquetHandle(path=str(p)), params, {})
    assert diag == {"enabled": False, "weight_keys": ["prod_name"],
                    "n_weight_entries": 0, "unmatched_keys": []}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_pipelines/test_training/test_nodes.py -k resolve_weight_diagnostics -q`
Expected: FAIL — `cannot import name 'resolve_weight_diagnostics'`.

- [ ] **Step 3: Write minimal implementation**

Ensure `import json` is present at the top of `src/recsys_tfb/pipelines/training/nodes.py` (add if absent). Add:

```python
def resolve_weight_diagnostics(
    train_handle, parameters: dict, preprocessor_metadata: dict,
) -> dict:
    """Data-driven sample_weight diagnostic for the model manifest.

    Reports configured sample_weights entries that match zero train rows
    (``unmatched_keys``) — covers label / identity / feature / encoding
    mismatch + unknown-category typos. Reads only the weight-key columns of the
    train parquet (cheap distinct).
    """
    import pyarrow.dataset as pads

    from recsys_tfb.io.extract import _composite_key_series, _translate_weight_table

    training = parameters.get("training", {}) or {}
    sw = training.get("sample_weights") or {}
    weight_keys = training.get("sample_weight_keys") or [get_schema(parameters)["item"]]
    diag = {"enabled": bool(sw), "weight_keys": list(weight_keys),
            "n_weight_entries": len(sw), "unmatched_keys": []}
    if not sw:
        return diag

    category_mappings = (preprocessor_metadata or {}).get("category_mappings", {}) or {}
    identity_cols = get_schema(parameters)["identity_columns"]

    ds = pads.dataset(train_handle.path, format="parquet")
    if any(k not in ds.schema.names for k in weight_keys):
        diag["unmatched_keys"] = sorted(str(k) for k in sw)
        return diag
    pdf = ds.to_table(columns=list(weight_keys)).to_pandas().drop_duplicates()
    present = set(_composite_key_series(pdf, weight_keys).tolist())

    unmatched = []
    for key in sw:
        one, _ = _translate_weight_table(
            {key: sw[key]}, weight_keys, category_mappings, identity_cols)
        if not one or next(iter(one)) not in present:
            unmatched.append(str(key))
    diag["unmatched_keys"] = sorted(unmatched)
    return diag
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m pytest tests/test_pipelines/test_training/test_nodes.py -k resolve_weight_diagnostics -q`
Expected: PASS (2 passed).

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/pipelines/training/nodes.py tests/test_pipelines/test_training/test_nodes.py
git commit -m "feat(training): resolve_weight_diagnostics (data-driven unmatched_keys)"
```

---

## Task 5: `persist_sample_weight_report` node + pipeline wiring

**Files:**
- Modify: `src/recsys_tfb/pipelines/training/nodes.py` (add node)
- Modify: `src/recsys_tfb/pipelines/training/pipeline.py` (import + Node)
- Test: `tests/test_pipelines/test_training/test_nodes.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_pipelines/test_training/test_nodes.py`:

```python
def test_persist_sample_weight_report_writes_json(tmp_path, monkeypatch):
    import json
    import pandas as pd
    from recsys_tfb.io.handles import ParquetHandle
    from recsys_tfb.pipelines.training import nodes as N

    p = tmp_path / "train.parquet"
    pd.DataFrame({"cust_segment_typ_2a": [0, 1], "prod_name": ["a", "a"],
                  "label": [1, 0]}).to_parquet(p)
    version_dir = tmp_path / "models" / "abc123"
    # node resolves version dir via diagnostics_dir(...).parent
    monkeypatch.setattr(N, "diagnostics_dir", lambda params: version_dir / "diagnostics")

    params = {"schema": {"columns": {"time": "snap_date", "entity": ["cust_id"],
              "item": "prod_name", "label": "label"}},
              "training": {"sample_weight_keys": ["cust_segment_typ_2a"],
                           "sample_weights": {"mass": 2.0}}}
    prep = {"category_mappings": {"cust_segment_typ_2a": ["mass", "hnw"]}}

    diag = N.persist_sample_weight_report(ParquetHandle(path=str(p)), prep, params)
    report = json.loads((version_dir / "sample_weight_report.json").read_text())
    assert report == diag
    assert report["enabled"] is True and report["unmatched_keys"] == []
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_pipelines/test_training/test_nodes.py -k persist_sample_weight_report -q`
Expected: FAIL — `module 'recsys_tfb.pipelines.training.nodes' has no attribute 'diagnostics_dir'` or `persist_sample_weight_report`.

- [ ] **Step 3: Write minimal implementation**

In `src/recsys_tfb/pipelines/training/nodes.py`, add a module-level import so `monkeypatch.setattr(N, "diagnostics_dir", ...)` works, then the node:

```python
from recsys_tfb.pipelines.training.diagnostics import diagnostics_dir
```

```python
def persist_sample_weight_report(
    train_parquet_handle, preprocessor_metadata: dict, parameters: dict,
) -> dict:
    """Compute + persist the sample_weight diagnostic next to the model artifact.

    Always runs (not gated by the lgb .bin cache) so the report reflects the
    current config every run. Writes ``sample_weight_report.json`` into the model
    version dir (so it appears in the manifest's artifacts list) and returns the
    diagnostic dict.
    """
    diag = resolve_weight_diagnostics(
        train_parquet_handle, parameters, preprocessor_metadata)
    version_dir = diagnostics_dir(parameters).parent
    version_dir.mkdir(parents=True, exist_ok=True)
    report_path = version_dir / "sample_weight_report.json"
    with open(report_path, "w") as f:
        json.dump(diag, f, indent=2, ensure_ascii=False, default=str)
    logger.info(
        "Wrote sample_weight report: %s (enabled=%s unmatched=%d)",
        report_path, diag["enabled"], len(diag["unmatched_keys"]),
    )
    return diag
```

In `src/recsys_tfb/pipelines/training/pipeline.py`, add `persist_sample_weight_report` to the `from recsys_tfb.pipelines.training.nodes import (...)` block, and append a node after `prepare_lgb_train_inputs` is defined (it must NOT be gated by the lgb cache, so give it its own node reading the train handle directly):

```python
    nodes.append(
        Node(
            persist_sample_weight_report,
            inputs=["train_parquet_handle", "preprocessor", "parameters"],
            outputs="sample_weight_report",
        ),
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m pytest tests/test_pipelines/test_training/test_nodes.py -k persist_sample_weight_report -q`
Expected: PASS (1 passed).

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/pipelines/training/nodes.py src/recsys_tfb/pipelines/training/pipeline.py tests/test_pipelines/test_training/test_nodes.py
git commit -m "feat(training): persist_sample_weight_report node + pipeline wiring"
```

---

## Task 6: Surface `sample_weight` in the training manifest

**Files:**
- Modify: `src/recsys_tfb/__main__.py` (factor `_sample_weight_extra`; pass `extra_metadata` in training manifest)
- Test: `tests/test_pipelines/test_training/test_nodes.py` (or a `tests/test_main` module if present — use the existing __main__ test location)

- [ ] **Step 1: Write the failing test**

Add a focused test for the helper (avoids invoking the full Typer command):

```python
def test_sample_weight_extra_reads_report(tmp_path):
    import json
    from recsys_tfb.__main__ import _sample_weight_extra
    vdir = tmp_path / "models" / "mv"
    vdir.mkdir(parents=True)
    (vdir / "sample_weight_report.json").write_text(
        json.dumps({"enabled": True, "weight_keys": ["prod_name"],
                    "n_weight_entries": 1, "unmatched_keys": []}))
    assert _sample_weight_extra(vdir) == {
        "sample_weight": {"enabled": True, "weight_keys": ["prod_name"],
                          "n_weight_entries": 1, "unmatched_keys": []}}


def test_sample_weight_extra_absent_returns_none(tmp_path):
    from recsys_tfb.__main__ import _sample_weight_extra
    assert _sample_weight_extra(tmp_path) is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_pipelines/test_training/test_nodes.py -k sample_weight_extra -q`
Expected: FAIL — `cannot import name '_sample_weight_extra'`.

- [ ] **Step 3: Write minimal implementation**

In `src/recsys_tfb/__main__.py`, add the helper near `_dir_artifacts` (line ~164):

```python
def _sample_weight_extra(version_dir: Path) -> Optional[dict]:
    """Read sample_weight_report.json (if present) into manifest extra_metadata."""
    report = version_dir / "sample_weight_report.json"
    if not report.exists():
        return None
    with open(report) as f:
        return {"sample_weight": json.load(f)}
```

Then in `training()` post-run (the `_write_pipeline_manifest(...)` call at ~line 467), pass the extra:

```python
    _write_pipeline_manifest(
        version_dir=version_dir,
        metadata_kwargs=metadata_kwargs,
        run_id=run_context.run_id,
        extra_metadata=_sample_weight_extra(version_dir),
        symlink_target=None,
        params_name="parameters_training",
        params_dict=params_training,
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_pipelines/test_training/test_nodes.py -k sample_weight_extra -q`
Expected: PASS (2 passed).

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/__main__.py tests/test_pipelines/test_training/test_nodes.py
git commit -m "feat(training): surface sample_weight diagnostic in model manifest"
```

---

## Task 7: Update config docs

**Files:**
- Modify: `conf/base/parameters_training.yaml` (the `sample_weight_keys` comment block, ~lines 40-48)

- [ ] **Step 1: Edit the comment**

Update the comment that currently says each weight key must be in `identity ∪ {label} ∪ dataset.carry_columns` to also allow declared categorical features. Replace the relevant lines with:

```yaml
  # sample_weight_keys: weight key 由哪些欄位組成（順序即 "|" 串接順序）。
  #   每個欄位必須 ∈ identity(snap_date,cust_id,prod_name) ∪ {label} ∪
  #   dataset.carry_columns ∪ dataset.prepare_model_input.categorical_columns。
  #   categorical feature（如 cust_segment_typ_2a）在 model_input 是「編碼後 int」，
  #   但 sample_weights 仍用人類可讀字串：runtime 由 _translate_weight_table 依
  #   category_mappings 翻成 code 後比對（encode-aware）。raw 值（identity/label/
  #   carry）維持字串。打錯/不存在的值 → 不中（weight 1.0），會記入 manifest.json
  #   的 sample_weight.unmatched_keys。
```

- [ ] **Step 2: Verify the file still parses**

Run: `.venv/bin/python -c "import yaml; yaml.safe_load(open('conf/base/parameters_training.yaml'))"`
Expected: no output (valid YAML).

- [ ] **Step 3: Commit**

```bash
git add conf/base/parameters_training.yaml
git commit -m "docs(training): document encode-aware sample_weight_keys"
```

---

## Final verification

- [ ] **Run the touched test suites**

Run: `.venv/bin/python -m pytest tests/test_io/test_extract.py tests/test_core/test_consistency.py tests/test_pipelines/test_training/test_nodes.py -q`
Expected: all PASS.

- [ ] **Rebuild the graphify code graph** (per CLAUDE.md, after modifying code files)

Run: `python3 -c "from graphify.watch import _rebuild_code; from pathlib import Path; _rebuild_code(Path('.'))"`

- [ ] **Open PR** for `feat/encode-aware-sample-weight`.

---

## Self-Review

**Spec coverage:**
- §3 translation mechanism → Task 1 (`_translate_weight_table`) + Task 2 (`_row_weights_from_pdf`). ✓
- §4.2 signature change + threading → Task 2. ✓
- §4.3 A9a widening → Task 3. ✓
- §4.4 `resolve_weight_diagnostics` + manifest persistence → Tasks 4 (function), 5 (node + report.json + artifacts), 6 (manifest `sample_weight` block via extra_metadata). ✓
- §5 label / identity / carry passthrough → covered by Task 1 `test_identity_component_passthrough` + Task 2 unchanged identity behavior (label/identity not in category_mappings → passthrough). ✓
- §6 backward compat → Task 2 updates existing observability tests to pass `{}` and asserts identical behavior; existing `TestExtractWithWeights` (identity/carry keys) unchanged. ✓
- §7 unknown handling (warn + 1.0) → Task 2 `test_unknown_feature_value_warns_and_all_ones`. ✓

**Placeholder scan:** No TBD/TODO; every code step has complete code. The Task 3 note about `_base()` schema setup tells the engineer to mirror existing same-file tests (concrete instruction, not a placeholder).

**Type consistency:** `_translate_weight_table(sample_weights, weight_keys, category_mappings, identity_columns) -> (dict, dict)` used identically in Task 2 (`_row_weights_from_pdf`) and Task 4 (`resolve_weight_diagnostics`). `resolve_weight_diagnostics(train_handle, parameters, preprocessor_metadata)` and `persist_sample_weight_report(train_parquet_handle, preprocessor_metadata, parameters)` arg orders are stated explicitly and matched in tests. Manifest dict keys (`enabled`/`weight_keys`/`n_weight_entries`/`unmatched_keys`) consistent across Tasks 4-6.
