"""Single source of truth for config / data consistency invariants.

Every invariant is defined ONCE here as a pure predicate. Layer-1 config-static
validation, Layer-2 preprocessing guards, and the test_product_consistency lint
all call these predicates — no duplicated definitions, no message drift.

All errors subclass ValueError so existing ``except ValueError`` call sites
(__main__._load_config_and_setup) and existing tests keep working unchanged.

Invariant legend
----------------
Code comments across this module, ``core/schema.py``,
``pipelines/dataset/nodes.py`` and ``pipelines/source_etl/checks.py`` reference
invariants by ID. This docstring is the canonical legend.

Layer 1 — config-static (implemented here; aggregated by
``validate_config_consistency``, run at CLI entry):

* A1 — a column declared in BOTH ``drop_columns`` and ``categorical_columns``
  (contradictory role). Predicate: ``config_role_conflicts``.
* A2 — ``categorical_columns`` omits ``schema.item``. Predicate:
  ``item_missing_from_categorical`` (runtime backstop: ``_spark.py`` item
  guard).
* A3 — an identity categorical (``schema.item``) is declared in
  ``categorical_columns`` but absent from ``schema.categorical_values``.
  Predicate: ``resolved_item_values`` (also delegated to by
  ``schema.validate_schema_config``; runtime backstop: ``_spark.py``
  identity-cat guard, which raises ``DataConsistencyError``).
* A4 — ``inference.products`` ≠ ``schema.categorical_values[item]``.
  Predicate: ``inference_products_mismatch``.
* A5 — a ``sample_ratio_overrides`` key references an item value absent from
  ``schema.categorical_values[item]``. Predicate: ``override_unknown_items``.
* A6 — the hardcoded item lists across YAML/SQL/synthetic-data disagree.
  Enforced by the ``tests/test_pipelines/test_source_etl/
  test_product_consistency.py`` lint (consumes ``resolved_item_values``),
  not a predicate here.
* A7 — a ranking ``training.algorithm_params.objective``
  (``lambdarank``/``rank_xendcg``) paired with a non-ranking ``metric`` or an
  undefined query group (empty ``schema.entity``). Predicate:
  ``ranking_objective_conflicts``.
* A8 — ``training.search_space`` declarative schema validity: must be an
  ordered list of ParamSpec maps; each needs ``name`` (unique) + ``type`` ∈
  {int,float,categorical}; numeric ``low < high``; positive ``step``;
  ``log: true`` ⟹ ``low > 0`` and no ``step``; categorical needs non-empty
  ``choices``. ``when`` / string-expression bounds are rejected until
  Phase 3. Predicate: ``search_space_errors``.
* A9 — ``training.sample_weights`` integrity (keys are '|'-joined
  ``training.sample_weight_keys`` values), split into:
    - A9a — a ``sample_weight_keys`` column ∉ identity ∪ {label} ∪
      ``dataset.carry_columns`` (cross-file: the column would be absent from
      or int-encoded in the train model_input parquet, so the weight silently
      no-ops). Predicate: ``weight_key_columns_unavailable``.
    - A9b — a ``sample_weights`` key whose '|'-segment count ≠
      ``len(sample_weight_keys)`` (silently never matches). Predicate:
      ``weight_key_arity_mismatch``.
    - A9c — a ``sample_weights`` key whose product component (when
      ``schema.item`` is a weight key) ∉ ``resolved_item_values`` (mirrors A5).
      Predicate: ``weight_unknown_items``.
* A10 — an ``evaluation.segment_columns`` entry has no ``evaluation.
  segment_sources`` entry providing it (matching ``segment_column``); the
  per-segment report section would silently never render. Predicate:
  ``segment_columns_without_source``.
* A11 — every ``evaluation.compare_sources[*]`` is well-formed:
  ``kind`` ∈ {model_version, external_hive}; ``label`` required; ranked
  by-kind required fields (``model_version`` for model_version, optional
  ``source`` ∈ {enriched_eval_predictions, ranked_predictions,
  training_eval_predictions} — default ``enriched_eval_predictions``;
  ``table`` + ``columns`` (one key per schema role: every column in
  ``identity_columns`` — time + entity + item — plus score, resolved via
  ``get_schema``, NOT the literal cust_id/snap_date/prod_name names) +
  ``prod_mapping`` + ``unmapped_policy`` ∈ {fail, drop} for
  external_hive); ``model_version`` kind must NOT declare
  ``columns``/``prod_mapping`` (config leak guard). Predicate:
  ``compare_source_well_formed_errors``.
* A12 — ``--compare X`` / ``--compare-only X`` resolves to a key in
  ``compare_sources``. Predicate: ``compare_source_key_exists`` (raises
  ``ConfigConsistencyError`` directly; not aggregated by validate).
* A13 — ``--compare`` and ``--compare-only`` are mutually exclusive (only
  one or neither). Predicate: ``compare_mutual_exclusive_errors``.
* A14 — ``schema.item`` appears in ``training.feature_selection.exclude``.
  Training-stage feature selection must never drop the item column (for a
  ranking task the item must stay a model feature; mirrors A2/A7). Predicate:
  ``feature_selection_excludes_item``.
* A15 — ``evaluation.metric`` / ``evaluation.diagnosis`` parameter domains:
  ``weight_alpha`` ∈ [0,1]; ``k`` null or int ≥ 1; ``min_positives`` ≥ 0;
  ``shrinkage_k`` ≥ 0; ``diagnosis.sample.max_queries`` ≥ 1;
  ``diagnosis.sample.min_pos_queries_per_item`` ≥ 1;
  ``diagnosis.ci.n_boot`` ≥ 1; ``diagnosis.item_ability.top_n`` ≥ 0;
  ``diagnosis.ci.enabled`` and every
  ``diagnosis.<name>.enabled`` for ``name`` in
  ``diagnosis.metric.contract.DIAGNOSES`` must be a real bool (a quoted YAML
  ``"false"`` is truthy and would silently enable the node); and
  ``evaluation.segment_columns`` must not use the sampler's reserved names
  ``stratum`` / ``inclusion_weight``. Predicate:
  ``diagnosis_metric_param_errors``. Registry diagnoses stay in A15 rather
  than getting their own code: their ``enabled`` flag is what decides whether
  the shared diagnosis sample is drawn, i.e. the same invariant family as
  ``ci.enabled``.
* A16 — retired 2026-07-17 with the reconciliation layer. The code is NOT
  renumbered: existing docs and plans cite invariants by number, so reusing
  A16 or shifting A17+ would silently repoint those references.
* A17 — retired 2026-07-19 with the quadrant diagnosis layer (threshold-based
  bucketing discarded continuous information; superseded by a scatter view).
  The code is NOT renumbered: existing docs and plans cite invariants by
  number, so reusing A17 or shifting A18+ would silently repoint those
  references.
* A18 — retired 2026-07-22 with the offset_sweep diagnosis layer. The code is
  NOT renumbered: existing docs and plans cite invariants by number, so reusing
  A18 or shifting A19+ would silently repoint those references.
* A19 — evaluation.diagnosis.suppression.top_examples must be a non-negative
  int (enabled is covered by A15). Predicate: ``suppression_param_errors``.
* A20 — training-side ``diagnostics.*`` parameter domains:
  ``diagnostics.shap.background`` ∈ {global, per_item};
  ``diagnostics.gain_ledger.enabled`` and ``diagnostics.shap.
  quadrant_enabled`` are bool; ``diagnostics.shap.quadrant_top_k_decision`` /
  ``quadrant_sample_per_cell`` / ``quadrant_min_rows`` are integers >= 1.
  Predicate: ``training_diagnostics_param_errors``.
* A21 — every ``--rebuild-dates`` value is a well-formed ISO date AND a member
  of the month list the command it was passed to can process:
  ``dataset.test_snap_dates`` for ``dataset`` and ``training``,
  ``inference.snap_dates`` for ``inference``. Every pipeline only ever
  processes configured months (ADR-0002 for dataset partitions, #130 for
  predictions, ADR-0010 for scoring chunks), so an unconfigured value would
  silently do nothing and leave the operator believing a month was recomputed.
  Predicates: ``resolved_rebuild_dates`` / ``resolved_inference_rebuild_dates``
  (raises ``ConfigConsistencyError`` directly and returns the normalised list;
  not aggregated by ``validate`` — it reads a CLI flag, which
  ``validate_config_consistency`` never sees. Mirrors A12).
* A22 — under ``--post-training``, ``evaluation.snap_date`` must be a member of
  ``dataset.test_snap_dates``. Post-training evaluation reads
  ``training_eval_predictions``, which accumulates every month ever predicted
  for a ``model_version`` (test dates left the version identity in ADR-0001),
  so an unlisted month can still return rows and produce a normal-looking
  report for a month the current config does not evaluate. An unset or
  unparseable ``evaluation.snap_date``, and an empty ``test_snap_dates``, are
  rejected by the same predicate (each with its own wording — they need
  different fixes). Applies to every ``--post-training`` run including
  ``--compare-only``: that mode re-reads ``enriched_eval_predictions``, which
  is partition-filtered by ``model_version`` alone and so accumulates months
  the same way. Predicate: ``post_training_snap_date_errors`` (returns errors;
  the evaluation command raises). NOT aggregated by
  ``validate_config_consistency``: that runs at CLI entry and cannot see
  ``--post-training``, and the default monitoring mode reads inference output
  whose month legitimately need not be a test month — wiring it there would
  block valid monitoring runs. Wired like A13.
* A23 — ``dataset.train_snap_dates`` required, a list, and non-empty. Only
  the dataset pipeline reads the key, so this is wired on the dataset command
  rather than aggregated (see the predicate for what aggregating it costs).
  Predicate: ``train_snap_dates_errors``.
* A24 — the four ``dataset.{train,calibration,val,test}_snap_dates`` splits
  must be mutually disjoint. A month in two splits trains the model and then
  measures it, so every metric from the second split silently becomes an
  in-sample number and nothing downstream notices. Dates are compared as
  calendar days (``pd.Timestamp``), so ``"2026-1-31"`` and ``"2026-01-31"``
  collide — a deliberate tightening over the string comparison this replaced
  (ADR-0008 section 3). Note this is only about *comparison*: a snap_date
  still has to be written ``YYYY-MM-DD`` to satisfy A21/A22, which reject
  anything ``_iso_date`` cannot read. Predicate: ``date_split_overlap_errors``
  (returns errors; the dataset command raises). NOT aggregated by
  ``validate_config_consistency``: that runs at the entry of every command
  while only the dataset pipeline reads these keys — the precedent, and the
  9-blocked-test measurement behind it, is issue #158. Wired like A21/A22.
  Deliberately has NO runtime backstop, unlike A2/A3/B6: this is a pure
  config predicate with nothing data-dependent to re-check, and the node-body
  call it replaced is exactly what ADR-0008 section 3 set out to remove.

* A25 — training-side HPO / finalize parameter domains:
  ``training.hpo_objective`` ∈ ``HPO_OBJECTIVES`` and
  ``training.final_model_strategy`` ∈ ``FINAL_MODEL_STRATEGIES``; an absent key
  keeps the node's own default and is clean, but an explicit YAML ``null`` is
  rejected (``dict.get`` hands the node ``None``, not the default). Predicate:
  ``training_hpo_finalize_param_errors``. Aggregated like A20 (one code per
  parameter family): both keys are optional, so the check costs a config that
  never names them nothing, while a typo in either is otherwise only found by
  the node that reads it — and ``final_model_strategy`` is read *after* the
  whole HPO search has run, so a typo there costs the entire search. The two
  value tuples are defined in this module rather than in the training pipeline
  so the gate and the node that dispatches on the value cannot drift apart.
* A26 — ``dataset.test_snap_dates`` must not spell one month two ways.
  ``"2026-01-31"`` and ``"20260131"`` are one month to the training cache
  (which keys on the ``YYYYMMDD`` directory name) but two different Hive
  partition values, so at most one of them can be right: the run produces two
  cache entries pointing at one directory, hands that directory to pyarrow
  twice, and every row of the month is counted twice in the predictions.
  Repeats of the *same* literal stay legal — they collapse to one entry and
  change nothing. Predicate: ``duplicate_test_month_errors`` (returns errors;
  the training command raises). NOT aggregated by
  ``validate_config_consistency``, for A24's reason: that gate runs at the
  entry of every command while the harm is training-only — the dataset
  pipeline normalises its months through ``pd.Timestamp`` into a set
  (``pipelines/dataset/month_plans.plan_incremental_snap_dates``), so two
  spellings collapse there harmlessly. Replaces the node-body check in
  ``cache_test_model_input`` (ADR-0014).
* A27 — the inference scoring grid ``inference.snap_dates`` x
  ``inference.entity_buckets`` x ``inference.products`` must not be
  degenerate: the two lists non-empty, and ``entity_buckets`` — when the key
  is present at all — a number >= 1. One code for three keys because they are
  one parameter family (A25's precedent) and because a config that empties two
  of them should cost one run to fix. Absent ``entity_buckets`` takes
  ``scoping.DEFAULT_ENTITY_BUCKETS`` and is clean; an explicit YAML ``null``
  is not absent and is rejected, mirroring A25. The healthy-window *bounds*
  (ADR-0010 section 4) stay a ``chunk_plans`` warning — one bucket is legal.
  Predicate: ``inference_grid_errors`` (returns errors; the inference command
  raises). NOT aggregated by ``validate_config_consistency``, for A24's
  reason: that gate runs at the entry of every command while these three keys
  are read by the inference pipeline alone. Replaces four node-body raises
  (``steps/scoping.py`` snap_dates, ``steps/chunk_plans.py`` entity_buckets /
  snap_dates / products) that each fired only *after*
  ``build_inference_population_features`` had run a full Spark pass, and that
  aborted on the first one instead of collecting all three; those four stay as
  runtime backstops, labelled in their docstrings (issue #200).
  ADR-0014 also *considered* A27 for "``schema.entity`` is exactly one column"
  and withdrew it — writing "not finished" down as "not supported". That
  meaning is not in force and must not be revived here.
* A28 — the prediction write target must declare every ``schema.entity``
  column. ``HiveTableDataset.save`` ends with ``df.select(*declared)``, so an
  entity column the catalog entry never declared is dropped there in silence:
  the table stays valid and the published rows identify the wrong thing, while
  every consumer downstream groups on the full entity tuple. Predicate:
  ``entity_columns_declared_errors`` (returns errors; the training command
  raises). **This module still never reads the catalog** — the command asks
  the dataset object for its ``declared_columns`` and passes the answer in, so
  the predicate stays pure. NOT aggregated by ``validate_config_consistency``,
  for A24's reason: that gate takes parameters alone and runs at the entry of
  every command, while this needs the resolved catalog and the harm is
  training-only. Replaces the node-body guard ADR-0014 first placed in
  ``predict_and_write_test_predictions``; wiring it at the command is what
  makes a catalog typo cost a startup rather than a whole HPO search.
* A29 — ``dataset.train_split_keys`` / ``dataset.val_sample_keys``, when
  declared, must each be a non-empty subset of ``schema.entity``. They declare
  the unit the train/dev split and the val draw group on; undeclared means the
  whole entity. Predicate: ``entity_grouping_key_errors``. Aggregated by
  ``validate_config_consistency`` — unlike A24/A26 this is not one pipeline's
  concern: ``train_split_keys`` also feeds ``train_variant_id``, so a typo
  changes artifact paths for training and inference too. Why two keys rather
  than one, and which version ID each moves:
  docs/adr/0016-split-unit-declared-by-two-keys.md.
* A30 — ``--env`` must name a directory that exists under ``conf/``.
  ``ConfigLoader._load`` deep-merges ``conf/<env>`` over ``conf/base``, and
  ``_load_yaml_dir`` returns an empty dict for a directory that is not there
  (``core/config.py``), so a mistyped env name is not an error — it is an empty
  overlay. The run proceeds on ``conf/base`` alone and prints nothing, which is
  the one failure shape an operator (or an agent told to "run with ``--env X``")
  cannot see: the run succeeds. Predicate: ``resolved_env_dir`` (raises
  ``ConfigConsistencyError`` directly and returns the path; NOT aggregated by
  ``validate_config_consistency`` — it reads the ``--env`` flag and the
  filesystem, neither of which that gate is given. Mirrors A12/A21). Two limits
  stated so nobody reads more into the gate than it does: (a) it checks
  existence only, and ``conf/sql`` / ``conf/spark-local`` exist without being
  overlay layers (F6 in docs/agents/architecture-constraints.md), so
  ``--env sql`` still passes — the gate catches typos, not deliberate nonsense;
  (b) an empty ``--env`` is rejected explicitly, because ``conf_dir / ""``
  resolves back to ``conf/`` itself, which *is* a directory and would otherwise
  pass while merging nothing — the exact silent degradation this invariant
  exists to stop. ``conf/local/`` is committed (empty, ``.gitkeep``) so the
  default ``--env local`` passes; see issue #153.
* A31 — the two ``dataset`` numeric-storage keys hold declarable values:
  ``numeric_feature_storage_type`` ∈ ``NUMERIC_STORAGE_TYPES`` (the storage type
  every numeric model feature converges on) and ``numeric_precision_policy`` ∈
  ``PRECISION_POLICIES`` (what B8 does when a column cannot survive it). An
  absent key keeps the module's default and is clean; an explicit YAML ``null``
  is rejected, because a present key is part of the version payload and the null
  form would move ``base_dataset_version`` while changing no behaviour (A25/A29's
  reasoning). One code for two keys: they are one parameter family (A25's
  precedent). Predicate: ``numeric_storage_param_errors``. Aggregated by
  ``validate_config_consistency`` rather than hung off the dataset command like
  A24/A26 — ``numeric_feature_storage_type`` feeds ``base_dataset_version``, so a
  typo in it moves the artifact paths training and inference resolve too, which
  is A29's reason for aggregating.

* A32 — each of the three source tables the dataset pipeline reads
  (``DATASET_SOURCE_TABLES``: sample_pool / label_table / feature_table) must
  declare ``quality_checks.max_duplicate_key_ratio``, at a value in [0, 1).
  ``OutputChecker.run_all`` gates the entire primary-key check on that key
  being present — declaring ``primary_key`` alone runs no value check — so
  deleting one config line silently turns off both verdicts the check produces:
  duplicate keys and the NULL key columns whose diagnosis issue #289 added.
  ADR-0006 records the accident already happening once: ``feature_table`` had a
  primary key and no ``quality_checks``, so ``select_train_keys``'s comment
  ("PK enforced upstream by source_etl's max_duplicate_key_ratio") was true for
  ``sample_pool`` and false for ``feature_table``, unnoticed for half a year.
  Scope is those three tables and not "every table with a ``primary_key``":
  five feature tables omit ``quality_checks`` deliberately,
  and checking them would add five scans ADR-0006 decided against. Predicate:
  ``dataset_source_quality_check_errors`` (pure — parameters only, no Spark, no
  new load path: ``ConfigLoader.get_parameters`` already merges every
  ``parameters*.yaml``). Aggregated by ``validate_config_consistency``, like
  A29/A31, because all three tables feed one pipeline's inputs and the operator
  should learn at the next CLI entry. Today it is a no-op — every one of the
  three complies; it exists for the day one stops. Deliberate residual: deleting
  ``primary_key`` too escapes the gate, which is a visible retirement of the key
  rather than the silent shape above.

Layer 1 invariants that hang off a single command instead of the aggregator,
because they need context the aggregator never sees: A12/A13 and A21 (CLI
flags), A22 (``--post-training``), A24/A26 (config keys whose harm belongs
to one pipeline), A28 (the resolved catalog), A30 (``--env`` + the filesystem).

Layer 2 — data-stage validation (B1 + B5 + B6 + B7 + B8 + B9 implemented and
wired):

* B1 — sample_pool items ↔ declared items must be equal; label items ⊆
  declared items (unknown item values corrupt training or violate invariants).
  Predicate: ``item_coverage_errors`` (pure, no Spark); wired via
  ``validate_data_consistency`` (``pipelines/dataset/nodes.py``) as the
  first node of the dataset pipeline. B3 — a declared item has zero positives over
  the train window — intentionally NOT reported by ``item_coverage_errors``
  (deferred).
* B2 — label-window leakage columns reach features (specified but DEFERRED).
* B5 — a column declared in ``dataset.prepare_model_input.categorical_columns``
  is a continuous-numeric type (decimal/double/float) in feature_table. decimal
  collects to Python ``decimal.Decimal`` (not JSON-serializable → the opaque
  ``fit_preprocessor_metadata`` save crash this gate front-runs); double/float
  serialize but are near-certain mis-tags. Predicate:
  ``categorical_dtype_errors`` (pure, no Spark); wired via
  ``validate_data_consistency`` alongside B1 (reads ``feature_table.dtypes``,
  metastore metadata only — no scan).
* B6 — a feature column that is non-numeric (string / binary / date / timestamp /
  complex) and is NOT declared categorical (so never integer-encoded): it becomes
  an ``object``-dtype model feature → driver OOM at ``pdf_to_X`` ``to_numpy`` and
  a downstream LightGBM float-cast error. Predicate: ``nonnumeric_feature_errors``
  (with the ``spark_dtype_is_numeric`` classifier). Wired at TWO call sites — the
  dataset gate ``validate_data_consistency`` (prevents a rebuilt dataset baking it
  in) and a training-read backstop in ``io/extract.py`` (fails fast on an
  already-built parquet, before the expensive pandas read). B4 is unused.

  **The two sites classify different frames, so what "numeric" buys differs.**
  ``spark_dtype_is_numeric`` reads ``feature_table``, *before* the cast, so what
  it admits is "the cast converts this to a number" — not "this is already a
  number". ``boolean`` and ``decimal`` both force ``object`` dtype if they reach
  pandas un-cast, and are admitted only because
  ``preprocessing.cast_numeric_features_to_storage_type`` covers them (#283);
  ``TestB6AdmissionIsBackedByTheCast`` is what keeps those two in step.

  The backstop's own classifier (``_assert_feature_dtypes_numeric``,
  ``io/extract.py``) reads model_input, *after* the cast, and there that
  justification does not transfer: since #283 a ``boolean`` or ``decimal``
  feature column in model_input can only mean the cast was skipped, i.e. exactly
  the OOM this backstop exists to catch, yet it still classifies both as
  numeric. **Registered, not fixed** — tightening it is a change to what the
  gate rejects, and the remedies ``nonnumeric_feature_errors`` offers ("declare
  it categorical", "drop it") are the wrong advice for a column whose real fix
  is to rebuild the dataset. #283 deliberately left it alone.
* B7 — a column cannot be both carried and a model feature. When one is named in
  ``dataset.carry_columns`` and also exists in feature_table, the keys frame and
  the preprocessed feature frame each bring a copy into the
  ``build_model_input`` join and Spark raises ``Reference 'x' is ambiguous``.
  Two resolutions are valid and they are not interchangeable: adding it to
  ``dataset.prepare_model_input.drop_columns`` keeps the carry and gives up the
  feature, while removing it from ``dataset.carry_columns`` keeps the feature
  and forces any sample-weight key to come from elsewhere. Only the config
  author knows which the column is for, so the gate reports the collision
  instead of prescribing one fix — naming only the drop would steer every
  reader into silently losing a feature and rebuilding the dataset for it. The
  rule is stated nowhere in the config, and B6 does not cover it (B6 only fires
  on non-numeric undeclared feature columns). Identity columns and the label are
  exempt — they cannot
  collide however they are configured, so flagging them would force a
  version-busting config edit that changes nothing. Predicate:
  ``carry_column_collision_errors`` (pure, no Spark); wired via
  ``validate_data_consistency`` alongside B1/B5/B6, reusing the same
  ``feature_table.dtypes`` read (metastore metadata — no scan).
  Numbering continues past the unused B4 rather than backfilling it, so a
  future reader never sees B4 reappear and wonders whether it was revived.
  See ADR-0004.
* B8 — a feature column whose ``max(|value|)`` is larger than the declared
  ``dataset.numeric_feature_storage_type`` can hold at that column's own
  resolution, so the cast in ``build_model_input`` would map two distinct inputs
  onto one value and silently change the ranking. Predicate:
  ``numeric_precision_errors`` (pure, no Spark — it takes a column →
  ``ColumnPrecision`` mapping); classifier: ``spark_dtype_value_step``; bound:
  ``exact_value_limit`` over ``SIGNIFICAND_BITS``. Wired in
  ``validate_numeric_precision`` (``pipelines/dataset/nodes.py``), which runs
  after ``preprocessed_feature_table`` lands and before any ``build_model_input``
  reads it back — the cast is downstream, so gating after that write still gates
  before any narrowed value is stored.

  Two properties are deliberate rather than incidental. **Scope is whatever the
  cast actually converts** — the node intersects
  ``preprocessing.castable_numeric_feature_columns`` (the same selector the cast
  itself uses) with the dtypes that state a grid step, so the gate widens by
  itself the day the cast widens and there is never a window where it covers
  less than the cast does. Issue #283 is where that paid out: widening the cast
  to every numeric feature column widened this gate to the integer and boolean
  columns in the same edit, with no second list to update. That intersection is
  now the Decimal, integer and boolean columns; Float and Double state no grid,
  so nothing can be claimed about them.
  **The facts come from parquet footer statistics, not from an aggregation** —
  the dataset gates' cost invariant is zero scans (ADR-0006, and its amendment
  for this gate), and a ``max(abs(...))`` over the months would have changed
  that.

* B9 — a model feature column in ``train_model_input`` whose stored type is not
  the one ``dataset.numeric_feature_storage_type`` declares, either because the
  columns disagree with each other or because they agree on the wrong type. The
  training read pre-allocates one matrix for all of them, so the widest column
  sets the dtype of every column: one ``int64`` among 1,000 ``float32`` columns
  doubles a 24,000,000-row matrix from 89 GiB to 179 GiB, which a 128 GiB driver
  cannot allocate at all. Predicate: ``feature_storage_type_errors``; classifier:
  ``_arrow_storage_name`` (``io/extract.py``, pyarrow types). Wired as a
  training-read backstop in ``io/extract.py`` beside B6's, reading the parquet
  schema only — no data. Deferred identity categoricals are exempt: they are
  stored raw by contract and encoded per batch during the read.

  **This is what B6's registered gap resolves to.** B6's backstop still admits a
  ``boolean`` or ``decimal`` model_input feature column even though, since #283,
  either can only mean the cast was skipped. B9 rejects both — not by tightening
  B6's classifier, but by asking a different question (is it the declared type?)
  whose honest remedy is the dataset rebuild B6 could not prescribe.

Layer 3 — specified but DEFERRED (NOT implemented in this module yet); see
the plan doc for the full table:

* C1 — produced sample_pool/label distinct item ≠ config (source_etl
  runtime pre-flight).

B8: where the bound comes from, and why "did anything collide" is wrong
-----------------------------------------------------------------------
A column's *dtype* already states how fine its values are. An integer column's
values are 1 apart; a ``decimal(p, s)`` column's are ``10**-s`` apart — decimal
is exact fixed-point, every value an integer multiple of that step, **not an
approximate type**. Call that spacing the column's grid step ``g``. Narrowing is
safe exactly while no two neighbours on that grid land on the same stored value.

Representable neighbours sit ``ulp(x)`` apart, and ``ulp`` doubles at every power
of two. So the bound is the top of the last binade whose spacing is still no
wider than ``g``::

    max(|x|) <= 2 ** (floor(log2(g)) + significand_bits)

float32 carries 24 significand bits, float64 carries 53. For ``g = 1`` this is
2^24 = 16,777,216 — the number issue #281 states, and equality passes because
2^24 itself is representable and so is every integer below it. For
``decimal(18,2)`` (``g = 0.01``) it is 131,072.

The ``floor`` is the whole correction, not a rounding convenience. The obvious
``g * 2**bits`` reads right and is wrong for every step that is not a power of
two: it hands ``decimal(18,2)`` a bound of 167,772, where float32's spacing is
already 0.015625 and two values 0.01 apart collided long ago. Measured, 200,000
distinct values spaced 0.01 apart: at 167,772 only 128,000 survive
``astype(float32)``; at 131,072 all 200,000 do, and one step above it they do
not.

That decimal is exact is the load-bearing fact. Reading it as "already
approximate, therefore exempt" leaves the gate covering nothing at all, because
decimal is what the cast converts. Measured: 2,000 distinct ``decimal(18,2)``
values near 1e7 collapse to **21** after float32 — 1,979 rows that used to be
different entities becoming one.

**Why the gate is not "did any two values collide".** That test looks stricter
and is in fact useless: a ``double`` ratio column on [0, 1] holds far more
distinct values than float32 has, so it collapses too — 200,000 distinct values
on [0, 1] keep 199,209. A collision test therefore fires on almost every
approximate column in the table, and an alarm that is always on teaches the
operator nothing. It also asks the wrong question. LightGBM is histogram-based
(``max_bin=256``), so a column that was an approximation to begin with losing
its low-order bits changes no split it would otherwise have made; what changes a
split is a value from a known grid landing on a different grid point. That is
what this bound detects and a collision test cannot separate from the harmless
case.
"""

from __future__ import annotations

import datetime as _datetime
import math
from collections.abc import Mapping
from pathlib import Path
from typing import NamedTuple

import pandas as pd

from recsys_tfb.core.group_utils import RANKING_OBJECTIVES
from recsys_tfb.core.schema import ENTITY_GROUPING_KEYS, get_schema

#: ``parameters`` key the CLI hands the A21-validated ``--rebuild-dates`` value
#: to nodes under (values normalised to ``YYYY-MM-DD`` by
#: :func:`resolved_rebuild_dates`). It lives beside the predicate that produces
#: the value because two pipelines read it — dataset's test-branch nodes and
#: training's cache/predict nodes — and pipelines never import each other.
REBUILD_SNAP_DATES_KEY = "_rebuild_snap_dates"


class ConsistencyError(ValueError):
    """Base for all consistency failures (subclasses ValueError by design)."""


class ConfigConsistencyError(ConsistencyError):
    """Config self-contradiction detectable without data (Layer 1)."""


class DataConsistencyError(ConsistencyError):
    """Config disagrees with the actual data (Layer 2)."""


def _prepare_model_input(parameters: dict) -> dict:
    return (parameters.get("dataset", {}) or {}).get("prepare_model_input", {}) or {}


def resolved_item_values(parameters: dict) -> list[str]:
    """Canonical sorted list of valid item values (the single source).

    Reads ``schema.categorical_values[schema.item]``. Raises
    ``ConfigConsistencyError`` when the item column is a declared categorical
    (in prepare_model_input.categorical_columns) but has no category list —
    this is invariant A3, defined here once.

    Returns ``[]`` when the item column is not a declared categorical (or
    ``categorical_columns`` is absent). Callers relying on this as the single
    source of valid item values must ensure ``item_missing_from_categorical``
    (invariant A2) is validated upstream — ``validate_config_consistency``
    does this.
    """
    schema = get_schema(parameters)
    item = schema["item"]
    cat_values = schema.get("categorical_values", {}) or {}
    declared_cats = _prepare_model_input(parameters).get("categorical_columns")
    if declared_cats is not None and item in declared_cats and item not in cat_values:
        raise ConfigConsistencyError(
            f"schema.item={item!r} is in dataset.prepare_model_input."
            f"categorical_columns but has no schema.categorical_values[{item!r}] "
            f"declaration. Add the full value list under "
            f"schema.categorical_values.{item} in parameters.yaml."
        )
    return sorted(cat_values.get(item, []))


def config_role_conflicts(parameters: dict) -> list[str]:
    """Columns declared in BOTH drop_columns and categorical_columns (A1).

    A column in both lists is an illegal, environment-divergent config state
    (silent 'drop wins' in prod, misleading fail-loud in dev). Returned sorted;
    empty list means OK.
    """
    pmi = _prepare_model_input(parameters)
    drop = set(pmi.get("drop_columns", []) or [])
    cat = set(pmi.get("categorical_columns", []) or [])
    return sorted(drop & cat)


def feature_selection_excludes_item(parameters: dict) -> bool:
    """schema.item is listed in training.feature_selection.exclude (A14).

    Training-stage feature selection drops features at model build time without
    rebuilding the dataset. It must never drop the item column: for a ranking
    task the item must remain a model feature (mirrors A2/A7). Returns True when
    the item is in the exclude list.
    """
    item = get_schema(parameters)["item"]
    fs = (parameters.get("training", {}) or {}).get("feature_selection") or {}
    return item in (fs.get("exclude") or [])


def inference_products_mismatch(parameters: dict) -> dict:
    """Symmetric diff between inference.products and resolved_item_values (A4).

    Empty 'inference' section → no mismatch (inference not configured here).
    """
    declared = set(resolved_item_values(parameters))
    inf = parameters.get("inference") or {}
    if "products" not in inf:
        return {"only_in_inference": [], "only_in_categorical": []}
    products = set(inf.get("products") or [])
    return {
        "only_in_inference": sorted(products - declared),
        "only_in_categorical": sorted(declared - products),
    }


def override_unknown_items(parameters: dict) -> list[str]:
    """sample_ratio_overrides keys whose item component ∉ resolved_item_values (A5).

    Override keys are '|'-joined sample_group_keys values. If schema.item is not
    a sample_group_key there is no item component → nothing to check.
    """
    schema = get_schema(parameters)
    item = schema["item"]
    ds = parameters.get("dataset", {}) or {}
    group_keys = ds.get("sample_group_keys", [])
    if item not in group_keys:
        return []
    idx = group_keys.index(item)
    declared = set(resolved_item_values(parameters))
    bad: set[str] = set()
    for key in (ds.get("sample_ratio_overrides") or {}):
        parts = str(key).split("|")
        if idx < len(parts) and parts[idx] not in declared:
            bad.add(parts[idx])
    return sorted(bad)


def item_missing_from_categorical(parameters: dict) -> bool:
    """True if schema.item is absent from an explicitly-set categorical_columns (A2).

    When the key is absent, the codebase default ([schema.item]) includes it,
    so that case is OK.
    """
    schema = get_schema(parameters)
    item = schema["item"]
    declared = _prepare_model_input(parameters).get("categorical_columns")
    if declared is None:
        return False
    return item not in declared


# Eval metrics LightGBM accepts for a learning-to-rank objective. Anything
# else (e.g. binary_logloss) makes ranking early-stopping silently
# meaningless. Kept here (not in group_utils) because it is a config-policy
# fact owned by the consistency layer.
RANKING_METRICS: frozenset[str] = frozenset({"ndcg", "map", "lambdarank"})


def ranking_objective_conflicts(parameters: dict) -> list[str]:
    """A7 — a ranking objective requires a ranking metric and a query group.

    ``lambdarank``/``rank_xendcg`` cannot early-stop on a binary metric
    (silently meaningless) and need a per-query group. The query group is
    ``schema['time'] + schema['entity']``; ``entity`` must be non-empty. An
    *unset* metric is allowed — it is defaulted to ``ndcg`` at train time by
    ``group_utils.default_metric_for_objective``. Returns collect-all error
    strings; empty list means OK.
    """
    training = parameters.get("training", {}) or {}
    ap = training.get("algorithm_params", {}) or {}
    objective = ap.get("objective")
    if objective not in RANKING_OBJECTIVES:
        return []

    errors: list[str] = []

    metric = ap.get("metric")
    if metric is not None and str(metric) not in RANKING_METRICS:
        errors.append(
            f"training.algorithm_params.objective={objective!r} is a ranking "
            f"objective but metric={metric!r} is not a ranking metric. Set "
            f"training.algorithm_params.metric to one of "
            f"{sorted(RANKING_METRICS)} (e.g. 'ndcg'), or remove it to default "
            f"to 'ndcg'."
        )

    schema = get_schema(parameters)
    if not schema.get("entity"):
        errors.append(
            f"training.algorithm_params.objective={objective!r} is a ranking "
            f"objective but the query group (schema.columns.time + entity) is "
            f"undefined: schema 'entity' is empty. A ranking objective needs a "
            f"per-query group."
        )

    return errors


_SS_TYPES = frozenset({"int", "float", "categorical"})


def _is_number(v) -> bool:
    """True for a real int/float bound; bool excluded (``low: true`` in YAML
    is a typo, not the integer 1 — fail loud, never silently accept)."""
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def search_space_errors(parameters: dict) -> list[str]:
    """A8 — declarative ``training.search_space`` schema validity (collect-all).

    Phase 2 supports literal numeric int/float bounds and categorical
    ``choices``. ``when`` and string (expression) bounds are parsed by the
    search_space module but **rejected here fail-loud** until Phase 3 — never
    silently ignored. Empty/absent search_space is OK. Returns error strings.
    """
    training = parameters.get("training", {}) or {}
    if "search_space" not in training:
        return []
    space = training["search_space"]
    errors: list[str] = []

    if not isinstance(space, list):
        return [
            "training.search_space must be a list of ParamSpec maps "
            f"(got {type(space).__name__}). Migrate the old dict form to an "
            "ordered list: [{name, type, low, high, ...}, ...]."
        ]

    seen: set = set()
    for i, item in enumerate(space):
        if not isinstance(item, dict):
            errors.append(f"search_space[{i}] must be a map, got {type(item).__name__}.")
            continue
        name = item.get("name")
        ptype = item.get("type")
        tag = f"search_space[{i}]" + (f" ({name})" if name else "")

        if not name or not isinstance(name, str):
            errors.append(f"{tag}: missing/invalid required 'name' (string).")
        elif name in seen:
            errors.append(f"{tag}: duplicate name {name!r}.")
        else:
            seen.add(name)

        if ptype not in _SS_TYPES:
            errors.append(
                f"{tag}: type={ptype!r} invalid; must be one of "
                f"{sorted(_SS_TYPES)}."
            )

        if "when" in item:
            errors.append(
                f"{tag}: 'when' (conditional search space) is implemented in "
                f"Phase 3; not yet supported."
            )

        if ptype in ("int", "float"):
            low, high, step = item.get("low"), item.get("high"), item.get("step")
            for k, v in (("low", low), ("high", high)):
                if isinstance(v, str):
                    errors.append(
                        f"{tag}: expression-valued '{k}' is implemented in "
                        f"Phase 3; not yet supported (use a number)."
                    )
            if isinstance(step, str):
                errors.append(
                    f"{tag}: expression-valued 'step' is implemented in "
                    f"Phase 3; not yet supported (use a number)."
                )
            for k, v in (("low", low), ("high", high)):
                if not isinstance(v, str) and not _is_number(v):
                    errors.append(
                        f"{tag}: '{k}' must be a number (got "
                        f"{type(v).__name__}: {v!r})."
                    )
            if step is not None and not isinstance(step, str) and not _is_number(step):
                errors.append(
                    f"{tag}: 'step' must be a number (got "
                    f"{type(step).__name__}: {step!r})."
                )
            if _is_number(low) and _is_number(high) and not (low < high):
                errors.append(f"{tag}: low ({low}) must be < high ({high}).")
            if _is_number(step) and step <= 0:
                errors.append(f"{tag}: step must be positive (got {step}).")
            log = bool(item.get("log", False))
            if log and _is_number(low) and low <= 0:
                errors.append(
                    f"{tag}: log: true requires a positive low (got {low})."
                )
            if log and step is not None:
                errors.append(
                    f"{tag}: log: true and step are mutually exclusive "
                    f"(Optuna forbids it)."
                )
        elif ptype == "categorical":
            choices = item.get("choices")
            if not isinstance(choices, list) or len(choices) == 0:
                errors.append(f"{tag}: categorical requires a non-empty 'choices' list.")

    return errors


def weight_key_columns_unavailable(parameters: dict) -> list[str]:
    """training.sample_weight_keys columns absent from train model_input (A9a).

    The train/train_dev model_input parquet physically contains only identity
    columns, the label, dataset.carry_columns, and *encoded* features. A weight
    key must therefore be one of identity ∪ {label} ∪ carry_columns ∪ declared
    categorical columns — the raw-valued columns (encode-aware lookup translates
    declared categorical columns at runtime). Anything else is either physically
    absent (weight silently no-ops at 1.0) or int-encoded (key never matches).
    This is a cross-file dependency: sample_weight_keys lives in
    parameters_training.yaml but carry_columns lives in parameters_dataset.yaml.
    Returns sorted offending columns; empty means OK.
    """
    schema = get_schema(parameters)
    dataset_cfg = parameters.get("dataset", {}) or {}
    # Route through the file's own _prepare_model_input helper (as sibling
    # predicates do) and default only when the key is absent — matching
    # prepare_model_input_config, so an explicit `categorical_columns: []`
    # is honoured rather than silently coerced to [schema["item"]].
    declared_cats = _prepare_model_input(parameters).get("categorical_columns")
    categorical_cols = declared_cats if declared_cats is not None else [schema["item"]]
    available = (
        set(schema["identity_columns"])
        | {schema["label"]}
        | set(dataset_cfg.get("carry_columns") or [])
        | set(categorical_cols)
    )
    keys = (parameters.get("training", {}) or {}).get("sample_weight_keys") or []
    return sorted(k for k in keys if k not in available)


def weight_key_arity_mismatch(parameters: dict) -> list[str]:
    """training.sample_weights keys whose '|'-segment count != key arity (A9b).

    Each weight-table key is sample_weight_keys values joined with '|', so it
    must have exactly len(sample_weight_keys) segments. A miscounted key
    silently never matches any row. Returns sorted offending keys; empty
    means OK. No keys configured (arity 0) → nothing to check.
    """
    training = parameters.get("training", {}) or {}
    n = len(training.get("sample_weight_keys") or [])
    if n == 0:
        return []
    weights = training.get("sample_weights") or {}
    return sorted(str(k) for k in weights if len(str(k).split("|")) != n)


def weight_unknown_items(parameters: dict) -> list[str]:
    """training.sample_weights keys whose product component ∉ resolved_item_values (A9c).

    Weight-table keys are '|'-joined sample_weight_keys values. If schema.item
    is not a weight key there is no product component → nothing to check
    (mirrors A5's item-only check in override_unknown_items). Only keys whose
    segment count matches the key arity are inspected; arity errors are
    reported separately by weight_key_arity_mismatch.
    """
    training = parameters.get("training", {}) or {}
    keys = training.get("sample_weight_keys") or []
    item = get_schema(parameters)["item"]
    if item not in keys:
        return []
    idx = keys.index(item)
    weights = training.get("sample_weights") or {}
    declared = set(resolved_item_values(parameters))
    bad: set[str] = set()
    for key in weights:
        parts = str(key).split("|")
        if len(parts) == len(keys) and parts[idx] not in declared:
            bad.add(parts[idx])
    return sorted(bad)


def segment_columns_without_source(parameters: dict) -> list[str]:
    """evaluation.segment_columns entries with no providing segment_source (A10).

    Every column in ``evaluation.segment_columns`` must be delivered by some
    ``evaluation.segment_sources`` entry's ``segment_column``. Otherwise the
    metric layer silently produces no per_segment results and the report
    drops the per-segment section without warning. Returns sorted offending
    columns; empty list means OK.
    """
    ev = parameters.get("evaluation", {}) or {}
    seg_cols = ev.get("segment_columns", []) or []
    sources = (ev.get("segment_sources", {}) or {}).values()
    provided = {(cfg or {}).get("segment_column") for cfg in sources}
    return sorted(c for c in seg_cols if c not in provided)


def diagnosis_metric_param_errors(parameters: dict) -> list[str]:
    """evaluation.metric / evaluation.diagnosis parameter domains (A15).

    Absent blocks are fine (all keys have behavior-preserving defaults);
    present values must be in-domain, else the metric family silently
    degenerates (e.g. alpha>1 over-concentrates on hot items) or the
    bootstrap is undefined (n_boot<1).
    """
    errors: list[str] = []
    ev = parameters.get("evaluation", {}) or {}
    metric = ev.get("metric", {}) or {}
    diag = ev.get("diagnosis", {}) or {}
    sample = diag.get("sample", {}) or {}
    ci = diag.get("ci", {}) or {}

    alpha = metric.get("weight_alpha", 0.0)
    if not (_is_number(alpha) and 0.0 <= float(alpha) <= 1.0):
        errors.append(
            f"evaluation.metric.weight_alpha={alpha!r} must be a number in "
            f"[0, 1] (0 = equal-weight macro, 1 = positive-count weighting)."
        )
    k = metric.get("k", None)
    if k is not None and not (
        isinstance(k, int) and not isinstance(k, bool) and k >= 1
    ):
        errors.append(
            f"evaluation.metric.k={k!r} must be null (no truncation) or an "
            f"int >= 1."
        )
    mp = metric.get("min_positives", 0)
    if not (isinstance(mp, int) and not isinstance(mp, bool) and mp >= 0):
        errors.append(
            f"evaluation.metric.min_positives={mp!r} must be an int >= 0."
        )
    sk = metric.get("shrinkage_k", 0)
    if not (_is_number(sk) and float(sk) >= 0.0):
        errors.append(
            f"evaluation.metric.shrinkage_k={sk!r} must be a number >= 0."
        )

    for key, val, floor in (
        ("evaluation.diagnosis.sample.max_queries",
         sample.get("max_queries", 200000), 1),
        ("evaluation.diagnosis.sample.min_pos_queries_per_item",
         sample.get("min_pos_queries_per_item", 50), 1),
        ("evaluation.diagnosis.ci.n_boot", ci.get("n_boot", 200), 1),
        ("evaluation.diagnosis.item_ability.top_n",
         (diag.get("item_ability", {}) or {}).get("top_n", 30), 0),
    ):
        if not (isinstance(val, int) and not isinstance(val, bool)
                and val >= floor):
            errors.append(f"{key}={val!r} must be an int >= {floor}.")

    en = ci.get("enabled", True)
    if not isinstance(en, bool):
        errors.append(
            f"evaluation.diagnosis.ci.enabled={en!r} must be a boolean "
            f"(YAML true/false; a quoted string like \"false\" is truthy and "
            f"would silently enable the node)."
        )

    # Same YAML trap for every registry diagnosis (diagnosis.metric.contract.
    # DIAGNOSES). These belong to A15 rather than a new code: their `enabled`
    # is what decides whether the SHARED diagnosis sample gets drawn at all,
    # so it is another member of the same invariant as ci.enabled — not a
    # separate concern.
    #
    # Imported lazily: core/ must not gain an import-time dependency on
    # diagnosis/ (the layering claim is that diagnosis sits above the core
    # config layer, not beside it).
    from recsys_tfb.diagnosis.metric.contract import DIAGNOSES

    for name in DIAGNOSES:
        val = (diag.get(name, {}) or {}).get("enabled", True)
        if not isinstance(val, bool):
            errors.append(
                f"evaluation.diagnosis.{name}.enabled={val!r} must be a "
                f"boolean (YAML true/false; a quoted string like \"false\" is "
                f"truthy and would silently enable the node)."
            )

    # ``draw_diagnosis_sample`` adds its own 'stratum' / 'inclusion_weight'
    # columns to the sample, then joins them onto the predictions by query
    # key. A segment column of the same name would be duplicated by that join
    # and blow up far downstream with an opaque pandas error ("Grouper for
    # 'stratum' not 1-dimensional"). Catching it here means the CLI rejects
    # the config in ~1s instead of 2-4 minutes into a Spark job.
    reserved = {"stratum", "inclusion_weight"}
    bad_seg = sorted(
        set(ev.get("segment_columns", []) or []) & reserved
    )
    for col in bad_seg:
        errors.append(
            f"evaluation.segment_columns entry {col!r} is a reserved column "
            f"name: the diagnosis sampler creates its own 'stratum' and "
            f"'inclusion_weight' columns, so a segment column of the same "
            f"name would collide in the sample join. Rename it in the source "
            f"table or drop it from evaluation.segment_columns."
        )
    return errors


def suppression_param_errors(parameters: dict) -> list[str]:
    """evaluation.diagnosis.suppression.top_examples parameter domain (A19).

    ``enabled`` is intentionally NOT re-validated here: A15
    (``diagnosis_metric_param_errors``) already walks every name in
    ``diagnosis.metric.contract.DIAGNOSES`` — including ``suppression`` once
    it is registered — and validates its ``enabled`` flag there. Checking it
    again here would raise two error messages for the same bad value.
    """
    errors: list[str] = []
    diag = ((parameters.get("evaluation", {}) or {})
            .get("diagnosis", {}) or {})
    cfg = diag.get("suppression", {}) or {}
    top_examples = cfg.get("top_examples", 50)
    if not (
        isinstance(top_examples, int) and not isinstance(top_examples, bool)
        and top_examples >= 0
    ):
        errors.append(
            f"evaluation.diagnosis.suppression.top_examples={top_examples!r} "
            "must be a non-negative int."
        )
    return errors


def training_diagnostics_param_errors(parameters: dict) -> list[str]:
    """A20 — training-side ``diagnostics.*`` parameter domains.

    Covers every ``diagnostics.*`` key consumed via bare truthiness/int
    comparison by the training-side diagnostics nodes, where a wrong YAML
    type is silently accepted rather than raising:
    ``diagnostics.shap.background`` must be ``global`` or ``per_item``;
    ``diagnostics.gain_ledger.enabled`` and ``diagnostics.shap.
    quadrant_enabled`` must be bool (a quoted YAML string like "false" is
    truthy in Python and would silently enable the node —
    ``shap_cases.py``/``population_spark.py`` both read
    ``cfg.get("quadrant_enabled", True)`` bare); ``diagnostics.shap.
    quadrant_top_k_decision`` / ``quadrant_sample_per_cell`` /
    ``quadrant_min_rows`` must be integers >= 1. Absent keys use
    behavior-preserving defaults. Returns collect-all error strings; empty
    means OK.
    """
    errors: list[str] = []
    diag = parameters.get("diagnostics", {}) or {}
    shap_cfg = diag.get("shap", {}) or {}
    bg = shap_cfg.get("background", "global")
    if bg not in ("global", "per_item"):
        errors.append(
            f"A20: diagnostics.shap.background must be 'global' or 'per_item' "
            f"(got {bg!r})."
        )
    gl_en = (diag.get("gain_ledger", {}) or {}).get("enabled", True)
    if not isinstance(gl_en, bool):
        errors.append(
            f"A20: diagnostics.gain_ledger.enabled={gl_en!r} must be a bool "
            f"(true/false without quotes in YAML)."
        )
    q_en = shap_cfg.get("quadrant_enabled", True)
    if not isinstance(q_en, bool):
        errors.append(
            f"A20: diagnostics.shap.quadrant_enabled={q_en!r} must be a bool "
            f"(true/false without quotes in YAML)."
        )
    for key, default in (
        ("quadrant_top_k_decision", 1),
        ("quadrant_sample_per_cell", 30),
        ("quadrant_min_rows", 10),
    ):
        v = shap_cfg.get(key, default)
        if not (isinstance(v, int) and not isinstance(v, bool) and v >= 1):
            errors.append(
                f"A20: diagnostics.shap.{key}={v!r} must be an integer >= 1."
            )
    return errors


#: Values ``training.hpo_objective`` may take (A25). Defined here rather than
#: in the training pipeline because the entry gate and the node that dispatches
#: on the value must not be able to disagree about what is admissible:
#: ``pipelines/training/nodes.py`` imports this tuple for its own fail-loud
#: dispatch, so adding an objective is one edit rather than two.
HPO_OBJECTIVES = ("mean_ap", "macro_per_item_map")

#: Values ``training.final_model_strategy`` may take (A25). ``hpo_best`` passes
#: the HPO winner through unchanged; ``refit_on_full`` retrains on
#: train + train_dev at the winner's best_iteration.
FINAL_MODEL_STRATEGIES = ("hpo_best", "refit_on_full")


def training_hpo_finalize_param_errors(parameters: dict) -> list[str]:
    """A25 — training-side HPO / finalize parameter domains.

    Covers the two ``training.*`` keys whose value is a name the pipeline
    dispatches on: ``hpo_objective`` (which score an HPO trial is judged by)
    and ``final_model_strategy`` (how the shipped model is produced from the
    HPO winner). Neither is data-dependent, so both are decidable at CLI entry.

    Why they are worth a code of their own rather than being left to the nodes
    that read them: ``final_model_strategy`` is read by ``finalize_model``,
    which runs *after* the whole Optuna search — so a typo costs the entire
    search before it is reported. ``hpo_objective`` is read at the top of
    ``tune_hyperparameters``, which is cheap by comparison, but it belongs to
    the same family and shares the fix (retype the value), so splitting them
    across two codes would only make the config author read two messages.

    An *absent* key is clean: both nodes read it with a behaviour-preserving
    default (``mean_ap`` / ``hpo_best``). An explicit YAML ``null`` is NOT the
    same thing and is rejected — ``dict.get(key, default)`` returns ``None``
    when the key is present and null, so the default never applies, and
    ``finalize_model`` would read ``None``, fail its ``== "hpo_best"`` test and
    silently run a full ``refit_on_full``. Skipping on ``value is None`` would
    map null and ``"refit_on_full"`` onto one outcome, which is exactly the
    collision this gate exists to prevent. Returns collect-all error strings;
    empty means OK.
    """
    errors: list[str] = []
    training = parameters.get("training", {}) or {}
    for key, admitted in (
        ("hpo_objective", HPO_OBJECTIVES),
        ("final_model_strategy", FINAL_MODEL_STRATEGIES),
    ):
        if key not in training:
            continue
        value = training[key]
        if value not in admitted:
            errors.append(
                f"A25: training.{key}={value!r} is not a value this pipeline "
                f"can run. Allowed: {', '.join(admitted)}."
            )
    return errors


def entity_grouping_key_errors(parameters: dict) -> list[str]:
    """A29 — ``dataset.train_split_keys`` / ``val_sample_keys`` shape.

    Each, when declared, must be a non-empty subset of ``schema.entity``.
    Order and duplicates are not policed: both keys only ever reach
    ``spark_bucket``/``distinct``/a join key list, none of which read order,
    and a repeated column changes no result.

    Why a subset and not any column. Restricting to ``entity`` keeps these
    keys a statement about *how coarse the entity is*. Admitting an arbitrary
    column (split by ``region``, say) turns them into a general grouping
    mechanism whose interaction with the query group nothing here checks —
    a separate feature, not a looser bound on this one.

    Why config-static rather than a node guard. A misspelled column name is
    knowable from ``parameters`` alone, and the node that would notice sits
    behind a two-to-four-minute Spark warm-up. Aggregated by
    ``validate_config_consistency`` so a user who fumbles both keys sees both.
    """
    errors: list[str] = []
    ds = parameters.get("dataset") or {}
    entity = get_schema(parameters)["entity"]

    for key in ENTITY_GROUPING_KEYS:
        if key not in ds:
            continue
        value = ds[key]
        if value is None:
            # An absent key and a key written `train_split_keys:` with no value
            # behave identically at runtime — both resolve to the whole entity —
            # but they are NOT the same artifact. A present key is part of the
            # version payload, so writing the null form moves train_variant_id
            # (or base_dataset_version) and rebuilds everything under it while
            # changing no behaviour at all. Rejecting it is what keeps "omit to
            # get the default" the only spelling of the default.
            errors.append(
                f"A29: dataset.{key} is present but empty. Delete the line to "
                f"use the whole schema.entity={entity}; leaving it null changes "
                f"nothing about the split but still busts the version ID the "
                f"key feeds, rebuilding artifacts for no reason."
            )
            continue
        if not isinstance(value, list) or not all(isinstance(c, str) for c in value):
            errors.append(
                f"A29: dataset.{key}={value!r} must be a list of column names "
                f"(a non-empty subset of schema.entity={entity})."
            )
            continue
        if not value:
            errors.append(
                f"A29: dataset.{key} is an empty list. Omit the key to use the "
                f"whole schema.entity={entity}; an empty list would ask for a "
                f"split with no unit at all."
            )
            continue
        unknown = [c for c in value if c not in entity]
        if unknown:
            errors.append(
                f"A29: dataset.{key} names column(s) {unknown} that are not in "
                f"schema.entity={entity}. This key declares how coarse the "
                f"entity is, so it can only name entity columns — fix the "
                f"spelling, or add the column to schema.entity."
            )
    return errors


#: Legal ``dataset.numeric_feature_storage_type`` values, and the default an
#: absent key resolves to. Defined here rather than in the dataset pipeline so
#: the gate (A31) and the node that dispatches on the value cannot drift apart
#: — the reason A25 keeps ``HPO_OBJECTIVES`` in this module.
NUMERIC_STORAGE_TYPES: tuple[str, ...] = ("float32", "float64")
DEFAULT_NUMERIC_STORAGE_TYPE = "float32"

#: Legal ``dataset.numeric_precision_policy`` values. ``block`` stops the run on
#: a column that cannot survive the declared storage type; ``truncate`` logs a
#: warning and proceeds, accepting the loss deliberately. Named for what happens
#: to the *data*, not for the log level: the operator is choosing between a
#: failed run and truncated values.
PRECISION_POLICIES: tuple[str, ...] = ("block", "truncate")
DEFAULT_PRECISION_POLICY = "block"


def resolved_numeric_storage(parameters: dict) -> tuple[str, str]:
    """``(storage_type, precision_policy)`` with the defaults applied.

    One resolver rather than two ``dict.get`` calls at each reader, so "what an
    absent key means" has a single definition. Callers may assume the values are
    legal: A31 rejects anything else at CLI entry, including the explicit YAML
    ``null`` that ``dict.get(key, default)`` would otherwise hand back as None.
    """
    ds = parameters.get("dataset") or {}
    storage = ds.get("numeric_feature_storage_type") or DEFAULT_NUMERIC_STORAGE_TYPE
    policy = ds.get("numeric_precision_policy") or DEFAULT_PRECISION_POLICY
    return storage, policy


def numeric_storage_param_errors(parameters: dict) -> list[str]:
    """A31 — the two ``dataset`` numeric-storage keys hold declarable values.

    Both keys are optional and an absent key is clean, so a config that never
    names them pays nothing. An explicit YAML ``null`` is rejected rather than
    read as absent: it is *present* in the version payload, so writing the null
    form of ``numeric_feature_storage_type`` moves ``base_dataset_version`` and
    rebuilds every artifact under it while changing no behaviour (A29's null
    branch, same reasoning).

    One code for two keys because they are one parameter family (A25's
    precedent): the declaration and the gate that enforces it are read by the
    same node, and a config that fumbles both should cost one run to fix.

    Aggregated by ``validate_config_consistency`` rather than hung off the
    dataset command like A24/A26. ``numeric_feature_storage_type`` feeds
    ``base_dataset_version``, so a typo in it moves the artifact paths training
    and inference resolve too — this is not one pipeline's concern (A29's
    reasoning).
    """
    errors: list[str] = []
    ds = parameters.get("dataset") or {}
    for key, legal, default in (
        ("numeric_feature_storage_type", NUMERIC_STORAGE_TYPES,
         DEFAULT_NUMERIC_STORAGE_TYPE),
        ("numeric_precision_policy", PRECISION_POLICIES,
         DEFAULT_PRECISION_POLICY),
    ):
        if key not in ds:
            continue
        value = ds[key]
        if value in legal:
            continue
        errors.append(
            f"A31: dataset.{key}={value!r} is not a declarable value. Use one "
            f"of {list(legal)}, or delete the line to take the default "
            f"{default!r}."
        )
    return errors


#: The source tables the dataset pipeline reads — the three inputs of
#: ``validate_data_consistency``. A32 requires each to keep the quality check
#: below; the other tables the ETL stages produce are deliberately out of scope
#: (ADR-0006 checks the terminal table only).
DATASET_SOURCE_TABLES: tuple[str, ...] = ("sample_pool", "label_table", "feature_table")

#: The ``quality_checks`` key that switches ``source_etl``'s primary-key check
#: on. ``OutputChecker.run_all`` runs ``check_primary_key`` only when this key is
#: present, so it gates BOTH verdicts the check produces — duplicate keys and
#: NULL key columns (issue #289).
PRIMARY_KEY_CHECK = "max_duplicate_key_ratio"


def _etl_table_declarations(parameters: dict) -> list[tuple[str, Mapping]]:
    """``(stage, table)`` for every table any ``*_etl`` stage declares.

    Scanned by ``name`` rather than indexed: ``<stage>.tables[N]`` drifts the
    moment a table is added or reordered, and the gate would then be reading
    a different table than it reports on.
    """
    found: list[tuple[str, Mapping]] = []
    for stage, block in parameters.items():
        if not isinstance(stage, str) or not stage.endswith("_etl"):
            continue
        if not isinstance(block, Mapping):
            continue
        for entry in block.get("tables") or []:
            if isinstance(entry, Mapping) and entry.get("name"):
                found.append((stage, entry))
    return found


def dataset_source_quality_check_errors(parameters: dict) -> list[str]:
    """A32 — the dataset pipeline's three source tables keep their key check.

    ``OutputChecker.run_all`` gates the whole primary-key check on
    ``"max_duplicate_key_ratio" in quality_checks``: declaring ``primary_key``
    alone runs no *value* check at all. So deleting one line of config turns off
    both the duplicate check and the NULL diagnosis (issue #289) with nothing to
    show for it. ADR-0006 records this exact accident already happening once:
    ``feature_table`` declared a primary key and no ``quality_checks``, so
    ``select_train_keys``'s comment ("PK enforced upstream by source_etl's
    max_duplicate_key_ratio", ``pipelines/dataset/nodes.py``) was true of
    ``sample_pool`` — the table it is written about — and false of
    ``feature_table``, for half a year.

    Scope is the three tables the dataset pipeline actually reads. The
    alternative — "a declared ``primary_key`` implies an unconditional check" —
    was measured and rejected: five feature tables declare a key with no
    ``quality_checks`` *on purpose*, because duplicate keys reach the terminal
    table through the join fan-out and one aggregation there catches what six
    would (ADR-0006). Making the check unconditional adds those five scans back
    and overturns that decision; it would have to change the ADR first.

    A table no ``*_etl`` stage declares is skipped, and that is the second known
    residual: renaming or deleting one of the three entries escapes A32 the same
    way. It is skipped rather than reported because the gate has to stay usable
    on a config that declares one ETL stage and not the others (every minimal
    parameters dict in the test suite is that shape), and because the escape is
    not the silent failure A32 is for — a source table nothing produces stops
    the dataset pipeline at the Hive read. The threshold's *value* is in scope,
    though — the failure A32 exists to stop is the check being off while looking
    on, and ``max_duplicate_key_ratio: 1.0`` is exactly that (the measured ratio
    is always below 1). An explicit YAML null passes ``run_all``'s ``in`` test
    and then raises comparing a float to ``None`` — after the scan.

    Pure: takes ``parameters`` only, so it costs no Spark and no new load path
    (``ConfigLoader.get_parameters`` already merges every ``parameters*.yaml``,
    ETL stages included). Aggregated by ``validate_config_consistency`` rather
    than hung off one command: all three tables feed the dataset pipeline, and
    the operator who broke the config should learn at the next CLI entry.

    Known residuals, both deliberate: deleting ``primary_key`` as well escapes
    this gate (that is retiring the key declaration outright — a visible config
    edit rather than the silent shape above, registered in issue #289), and so
    does removing the table's whole declaration (paragraph above).
    """
    errors: list[str] = []
    declared = _etl_table_declarations(parameters)
    for name in DATASET_SOURCE_TABLES:
        for stage, entry in declared:
            if entry.get("name") != name:
                continue
            where = f"{stage}.tables[name={name!r}]"
            checks = entry.get("quality_checks") or {}
            if PRIMARY_KEY_CHECK not in checks:
                errors.append(
                    f"A32: {where} declares no quality_checks."
                    f"{PRIMARY_KEY_CHECK}. The dataset pipeline reads {name}, "
                    f"and that key is what makes source_etl check the primary "
                    f"key at all — without it neither duplicate keys nor NULL "
                    f"key columns are looked at, and nothing says so. Add:\n"
                    f"      quality_checks:\n"
                    f"        {PRIMARY_KEY_CHECK}: 0.0"
                )
                continue
            value = checks[PRIMARY_KEY_CHECK]
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                errors.append(
                    f"A32: {where} sets quality_checks.{PRIMARY_KEY_CHECK}="
                    f"{value!r}, which is not a ratio. A key written with no "
                    f"value is present enough for source_etl to run the check "
                    f"and then fail comparing a float to it, after the scan. "
                    f"Write a number in [0, 1) — normally 0.0."
                )
            elif not 0 <= value < 1:
                errors.append(
                    f"A32: {where} sets quality_checks.{PRIMARY_KEY_CHECK}="
                    f"{value!r}, which no measured ratio can be judged against: "
                    f"the ratio is always in [0, 1), so 1 or above never fails "
                    f"(the check is off while still declared) and below 0 never "
                    f"passes. Use a value in [0, 1) — normally 0.0."
                )
    return errors


def validate_config_consistency(parameters: dict) -> None:
    """Layer-1 config-static gate. Collects ALL failures, raises once.

    Collect-all (not fail-on-first) so a user fixes every problem in one pass.
    """
    errors: list[str] = []

    for col in config_role_conflicts(parameters):
        errors.append(
            f"{col!r} is declared in BOTH "
            f"dataset.prepare_model_input.drop_columns and categorical_columns "
            f"— contradictory intent. Resolve by choosing one:\n"
            f"    - want it as a feature  -> remove from drop_columns\n"
            f"    - want it excluded      -> remove from categorical_columns"
        )

    if item_missing_from_categorical(parameters):
        item = get_schema(parameters)["item"]
        errors.append(
            f"schema.item={item!r} is missing from "
            f"dataset.prepare_model_input.categorical_columns. For a ranking "
            f"task the item must be a model feature; add {item!r} back."
        )

    mm = inference_products_mismatch(parameters)
    if mm["only_in_inference"] or mm["only_in_categorical"]:
        errors.append(
            f"inference.products disagrees with schema.categorical_values"
            f"[item]: only_in_inference={mm['only_in_inference']}, "
            f"only_in_categorical={mm['only_in_categorical']}. They must be "
            f"identical sets."
        )

    unknown = override_unknown_items(parameters)
    if unknown:
        errors.append(
            f"sample_ratio_overrides references item value(s) {unknown} "
            f"absent from schema.categorical_values[item] — the override "
            f"silently never matches. Fix the key(s) or declare the value(s)."
        )

    for msg in ranking_objective_conflicts(parameters):
        errors.append(msg)

    cols_bad = weight_key_columns_unavailable(parameters)
    if cols_bad:
        errors.append(
            f"training.sample_weight_keys column(s) {cols_bad} are not in the "
            f"train model_input parquet (identity ∪ {{label}} ∪ "
            f"dataset.carry_columns) — the weight would silently never match. "
            f"Add them to dataset.carry_columns and re-run the dataset "
            f"pipeline (this busts base_dataset_version)."
        )

    arity_bad = weight_key_arity_mismatch(parameters)
    if arity_bad:
        n = len((parameters.get("training", {}) or {}).get("sample_weight_keys") or [])
        errors.append(
            f"training.sample_weights key(s) {arity_bad} do not have "
            f"{n} '|'-separated segment(s) to match "
            f"sample_weight_keys — the weight silently never matches. "
            f"Fix the key(s) or sample_weight_keys."
        )

    unknown_w = weight_unknown_items(parameters)
    if unknown_w:
        errors.append(
            f"training.sample_weights references product value(s) {unknown_w} "
            f"absent from schema.categorical_values[item] — the weight "
            f"silently never matches. Fix the key(s) or declare the value(s)."
        )

    for msg in search_space_errors(parameters):
        errors.append(msg)

    if feature_selection_excludes_item(parameters):
        item = get_schema(parameters)["item"]
        errors.append(
            f"schema.item={item!r} is listed in "
            f"training.feature_selection.exclude. The item column must remain a "
            f"model feature (ranking invariant); remove it from the exclude list."
        )

    errors.extend(compare_source_well_formed_errors(parameters))

    seg_no_src = segment_columns_without_source(parameters)
    if seg_no_src:
        errors.append(
            f"evaluation.segment_columns entries {seg_no_src} have no "
            f"evaluation.segment_sources entry providing them (no "
            f"segment_source has a matching segment_column). The per-segment "
            f"report section would silently never render. Add a "
            f"segment_sources entry for each, or remove them from "
            f"segment_columns."
        )

    errors.extend(diagnosis_metric_param_errors(parameters))

    errors.extend(suppression_param_errors(parameters))

    errors.extend(training_diagnostics_param_errors(parameters))

    errors.extend(training_hpo_finalize_param_errors(parameters))

    errors.extend(entity_grouping_key_errors(parameters))

    errors.extend(numeric_storage_param_errors(parameters))

    errors.extend(dataset_source_quality_check_errors(parameters))

    if errors:
        raise ConfigConsistencyError(
            "Config consistency check failed (" + str(len(errors))
            + " issue(s)):\n- " + "\n- ".join(errors)
        )


def item_coverage_errors(
    item: str,
    declared: list[str],
    sample_pool_items: set[str],
    label_items: set[str],
) -> list[str]:
    """B1 invariant — the single definition.

    sample_pool ↔ declared must be EQUAL (both directions are hard errors):
    a value the data has but config does not encodes to -1 (same code as
    null) and corrupts training/scoring; a value config declares but
    sample_pool never produces can never be scored.

    label_table: only ``label_items - declared`` is an error (label business
    logic produced an unknown item). ``declared - label_items`` is B3
    (zero-positive), deferred — intentionally NOT reported here.

    Keys off the passed ``item`` only; never hardcodes 'prod_name'. Returns
    collect-all error strings; empty list means OK.
    """
    declared_set = set(declared)
    errors: list[str] = []

    sp_unknown = sorted(sample_pool_items - declared_set)
    if sp_unknown:
        errors.append(
            f"sample_pool has item value(s) {sp_unknown} not in "
            f"schema.categorical_values[{item!r}] — these encode to -1 "
            f"(same code as null) and silently corrupt training/scoring. Add "
            f"them to schema.categorical_values.{item} in parameters.yaml, or "
            f"fix sample_pool.sql."
        )

    sp_missing = sorted(declared_set - sample_pool_items)
    if sp_missing:
        errors.append(
            f"schema.categorical_values[{item!r}] declares value(s) "
            f"{sp_missing} that sample_pool never produces — they can never "
            f"be scored/recommended (silent). Remove them from config, or fix "
            f"sample_pool.sql to emit them."
        )

    lb_unknown = sorted(label_items - declared_set)
    if lb_unknown:
        errors.append(
            f"label_table has item value(s) {lb_unknown} not in "
            f"schema.categorical_values[{item!r}] — label business logic "
            f"(label_*.sql) produced an item the model config does not know. "
            f"Reconcile label_*.sql with schema.categorical_values.{item}."
        )

    return errors


# Spark DataFrame.dtypes simpleString forms that are continuous-numeric and
# therefore an illegal type for a declared categorical (B5). decimal carries a
# precision/scale suffix ("decimal(15,0)"), so it is matched by prefix below.
_CONTINUOUS_NUMERIC_DTYPES = {"double", "float"}


def categorical_dtype_errors(
    categorical_cols: list[str],
    feature_table_dtypes: dict[str, str],
) -> list[str]:
    """B5 invariant — the single definition.

    A column declared in ``dataset.prepare_model_input.categorical_columns``
    must not be a continuous-numeric type (``decimal`` / ``double`` / ``float``)
    in ``feature_table``:

    - ``decimal`` collects to Python ``decimal.Decimal``, which is not
      JSON-serializable — ``fit_preprocessor_metadata`` crashes when saving the
      preprocessor metadata, but only after the full per-column ``distinct()``
      pass (the opaque, expensive failure this gate replaces).
    - ``double`` / ``float`` serialize fine but a continuous value used as a
      category is almost always a mis-tag, and float-equality lookup in the
      ``F.create_map`` encoding is fragile.

    ``feature_table_dtypes`` maps a feature_table column name to its Spark
    ``DataFrame.dtypes`` simpleString (e.g. ``"decimal(15,0)"``, ``"double"``,
    ``"string"``). Identity categoricals (``schema.item``) come from
    ``schema.categorical_values`` rather than feature_table, so they are absent
    from this mapping and correctly skipped. Pure (no Spark): the Layer-2 gate
    passes ``dict(feature_table.dtypes)`` in. Returns collect-all error strings
    sorted by column; empty list means OK.
    """
    errors: list[str] = []
    for col in sorted(categorical_cols):
        dt = feature_table_dtypes.get(col)
        if dt is None:
            continue  # identity categorical / not a feature_table column
        if dt.startswith("decimal") or dt in _CONTINUOUS_NUMERIC_DTYPES:
            errors.append(
                f"categorical column {col!r} is a continuous-numeric type "
                f"(type={dt}) in feature_table — a decimal categorical is not "
                f"JSON-serializable (fit_preprocessor_metadata save crashes) "
                f"and a double/float categorical is almost always a mis-tag. "
                f"If {col!r} is a numeric feature, remove it from "
                f"dataset.prepare_model_input.categorical_columns; if it is not "
                f"a model feature, add it to "
                f"dataset.prepare_model_input.drop_columns."
            )
    return errors


# ---------------------------------------------------------------------------
# B6 — non-numeric feature column that will not be encoded (object-dtype OOM)
# ---------------------------------------------------------------------------

# Spark ``DataFrame.dtypes`` simpleStrings for the types the cast in
# ``build_model_input`` converts to the declared storage type, so that what
# reaches pandas is a number. ``decimal(p,s)`` is the only parametric one
# (special-cased below). Whitelist, NOT blacklist: an unknown type
# (char/varchar/void/null/…) must be treated as non-numeric so it is never
# silently passed by the B6 gate (fail-safe).
_NUMERIC_SPARK_TYPES = frozenset(
    {"tinyint", "smallint", "int", "bigint", "float", "double", "boolean"}
)


def spark_dtype_is_numeric(simple_string: str) -> bool:
    """True iff a Spark ``DataFrame.dtypes`` simpleString denotes a type that
    reaches the model as a number (int / float / decimal / boolean). Every other
    type — string / binary / date / timestamp / char / varchar / void / null /
    complex — forces ``object`` dtype (the B6 footgun) and returns False. Pure
    string classification (no Spark import).

    **Read the admission as "the cast converts it", not "it is already safe".**
    Two of the admitted types would poison a pandas frame if they arrived
    un-cast, which is why the earlier wording here — that these types "survive
    ``DataFrame.values`` into a numeric numpy matrix" — was false for them:

    * ``boolean`` mixed with float32 gives ``object``, not a numeric matrix
      (measured, pandas 1.5.3: pandas' ``find_common_type`` refuses to mix bool
      with a numeric type — numpy on its own would give float32). That is
      exactly the boxed-object OOM B6 exists to prevent.
    * ``decimal`` materialises as Python ``decimal.Decimal`` objects, the case
      that originally OOM-killed the val read.

    What makes admitting them correct is downstream, not intrinsic:
    ``preprocessing.cast_numeric_features_to_storage_type`` converts **every**
    type in this whitelist to the declared storage type before any pandas frame
    exists. The claim and the cast stand or fall together, which is what
    ``tests/test_core/test_consistency.py::TestB6AdmissionIsBackedByTheCast``
    pins: shrink the cast's coverage and this classifier's admission goes red
    rather than becoming quietly untrue again (issue #283).
    """
    dt = simple_string.strip().lower()
    return dt.startswith("decimal") or dt in _NUMERIC_SPARK_TYPES


_UNIT_STEP_SPARK_TYPES = frozenset(
    {"tinyint", "smallint", "int", "bigint", "boolean"}
)


def spark_dtype_value_step(simple_string: str) -> float | None:
    """The smallest gap between two distinct values of a Spark dtype, or None.

    This is the one fact B8 needs about a column's *type*: its values sit on a
    grid, and the grid's spacing is what decides whether narrowing can make two
    of them collide (see the module docstring's B8 section).

    * integer types and ``boolean`` -> ``1.0``. Boolean's values are 0 and 1,
      which is why the gate never has to special-case it.
    * ``decimal(p, s)`` -> ``10 ** -s``. **Decimal is exact fixed-point, not an
      approximate type** — every value is an integer multiple of ``10**-s``, so
      it has a grid like any integer column, just a finer one. Reading it as
      "already approximate, therefore exempt" is the mistake that would leave
      the gate covering nothing at all: decimal is what the cast converts.
    * ``float`` / ``double`` -> ``None``. These genuinely have no grid the
      config knows about, so no bound can be stated and the gate says nothing
      about them. Their values are already approximations of something else;
      narrowing them loses low-order bits, which is the harmless case the gate
      is deliberately not built to flag.
    * every non-numeric type -> ``None`` (B5/B6 are what speak about those).

    Pure string classification (no Spark import), mirroring
    ``spark_dtype_is_numeric``.
    """
    dt = simple_string.strip().lower()
    if dt in _UNIT_STEP_SPARK_TYPES:
        return 1.0
    if dt.startswith("decimal"):
        # simpleString is always "decimal(p,s)"; a bare "decimal" would be
        # decimal(10,0) by Spark's default, hence the scale-0 fallback.
        scale = 0
        if "," in dt and dt.endswith(")"):
            scale = int(dt[dt.index(",") + 1:-1])
        return 10.0 ** -scale
    return None


#: Significand width of each declarable storage type, in bits. Keyed by the same
#: strings ``NUMERIC_STORAGE_TYPES`` declares, and a test pins the two sets equal
#: so a third storage type cannot be declarable without a bound.
SIGNIFICAND_BITS: dict[str, int] = {
    "float32": 24,
    "float64": 53,
}


def exact_value_limit(value_step: float, storage_type: str) -> float:
    """Largest ``max(|x|)`` a column of this grid spacing survives intact.

    Representable neighbours sit ``ulp(x)`` apart, and ``ulp`` doubles at every
    power of two, so the answer is the top of the last binade whose spacing is
    still no wider than the column's own step:
    ``2 ** (floor(log2(step)) + significand_bits)``.

    The floor is not a rounding convenience — it is the whole correction. A
    plain ``step * 2**bits`` reads right and is wrong for every step that is not
    a power of two: ``decimal(18,2)`` would get 167,772, where float32's spacing
    is already 0.015625 and two values 0.01 apart have long since collided
    (measured: 200,000 distinct values -> 128,000). The floor gives 131,072,
    where the measurement is lossless and one step above it is not.

    For ``value_step == 1`` this reduces to ``2 ** bits`` — 2^24 for float32,
    the bound issue #281 states.
    """
    return 2.0 ** (math.floor(math.log2(value_step)) + SIGNIFICAND_BITS[storage_type])


class ColumnPrecision(NamedTuple):
    """What B8 needs to know about one column: how big it gets, how fine it is.

    ``max_abs`` is the largest absolute value the column holds over the months
    this run is about to add, or ``None`` when that could not be established.
    A column with no non-null values reports ``0.0`` — a column with no values
    cannot lose one. ``value_step`` is its dtype's grid spacing
    (``spark_dtype_value_step``).

    One type rather than two parallel mappings: the pair always travels
    together, and a caller that got them out of step would produce a bound for
    the wrong column silently.
    """

    max_abs: float | None
    value_step: float


def numeric_precision_rows(
    by_column: Mapping[str, ColumnPrecision],
    storage_type: str,
) -> list[dict]:
    """One row per checked column: what it holds, what it may hold, how close.

    The companion to :func:`numeric_precision_errors`. That function answers
    "which columns fail"; this one answers "how much room does each column have
    left", which is the question an operator actually has *before* one fails —
    a column at 0.99 of its limit passes today and stops the pipeline the month
    a larger value arrives, and nothing in a pass/fail message says so.

    ``headroom`` is ``limit / max_abs`` — how many times larger the column could
    get before colliding. ``None`` when the column is empty (nothing to divide)
    or unmeasured. Rows are sorted by headroom ascending, so the column closest
    to breaching reads first whether the report is skimmed or truncated.

    Pure, and separate from the errors predicate rather than folded into it: the
    two have different audiences (a report that is always produced vs a message
    that appears only on failure) and folding them would make the report's shape
    a side effect of how the error strings are worded.
    """
    rows: list[dict] = []
    for col in sorted(by_column):
        max_abs, value_step = by_column[col]
        limit = exact_value_limit(value_step, storage_type)
        if max_abs is None:
            verdict, headroom = "unmeasured", None
        elif max_abs > limit:
            verdict, headroom = "breach", (limit / max_abs)
        else:
            verdict = "ok"
            headroom = None if max_abs == 0 else limit / max_abs
        rows.append({
            "column": col,
            "value_step": value_step,
            "max_abs": max_abs,
            "limit": limit,
            "headroom": headroom,
            "verdict": verdict,
        })
    return sorted(
        rows,
        key=lambda r: (r["headroom"] is None, r["headroom"] or 0.0, r["column"]),
    )


def numeric_precision_errors(
    by_column: Mapping[str, ColumnPrecision],
    storage_type: str,
) -> list[str]:
    """B8 invariant — the single definition.

    Pure — no Spark, no parquet, no filesystem. Everything data-dependent is the
    caller's to gather, which is what makes the rule testable by handing it a
    dict, and what keeps the "how do we get max(|x|) without scanning" question
    out of the definition of what the rule *is*.

    ``max_abs is None`` is an error, not a pass. The gate's promise is that a
    column it lets through survives the cast; a column it cannot measure is one
    it cannot make that promise about, and passing it would turn the gate into a
    check that silently covers an unknown subset. The message names the policy
    key because the alternative — an unrecoverable run for a reason unrelated to
    correctness — is worse than an operator who deliberately accepts the risk.

    "Declare float64" became a real remedy in #283 and is offered last, not
    first. Before #283 it was offered nowhere, because
    the cast (then named ``cast_feature_floats_to_float32``, a name that no
    longer exists) wrote float32 unconditionally: taking that
    advice would have rebuilt every artifact, changed no stored value, and only
    raised this gate's own bound — silencing the alarm instead of fixing the
    loss. Now the cast reads the key, so the widening is real. It is still the
    last resort: it doubles every feature column to buy headroom for the few
    that need it, where changing the column's representation upstream costs
    nothing per row. See the module docstring's B8 section.

    Collect-all and sorted by column: two runs of the same config read the same
    way, and one fix pass clears every offender.
    """
    if storage_type not in SIGNIFICAND_BITS:
        raise ConfigConsistencyError(
            f"numeric_precision_errors got storage_type={storage_type!r}, which "
            f"has no representability bound. A31 should have rejected it at CLI "
            f"entry; reaching here means the gate was called around it."
        )
    errors: list[str] = []
    for col in sorted(by_column):
        max_abs, value_step = by_column[col]
        if max_abs is None:
            errors.append(
                f"B8: feature column {col!r} carries no parquet min/max "
                f"statistics, so this gate cannot prove its values survive "
                f"{storage_type}. Either fix the writer so the column gets "
                f"statistics, or set dataset.numeric_precision_policy: truncate "
                f"to proceed without the proof."
            )
            continue
        limit = exact_value_limit(value_step, storage_type)
        if max_abs > limit:
            errors.append(
                f"B8: feature column {col!r} reaches max(|value|)={max_abs:,.10g}, "
                f"above the {limit:,.10g} that {storage_type} can hold at this "
                f"column's resolution (its values are {value_step:,.10g} apart). "
                f"Two distinct values would collapse onto one, which can "
                f"change the ranking. Three fixes, cheapest first: change "
                f"the column's representation upstream so it needs fewer steps "
                f"(drop unused decimal places, change the unit, bucket or "
                f"difference it); declare "
                f"dataset.numeric_feature_storage_type: float64, which widens "
                f"every feature column and so doubles the model input for this "
                f"one column's sake; or set "
                f"dataset.numeric_precision_policy: truncate to accept the "
                f"loss deliberately."
            )
    return errors


def nonnumeric_feature_errors(
    feature_kinds: dict[str, str],
    will_be_encoded: set[str],
) -> list[str]:
    """B6 invariant — the single definition.

    A *feature* column that is non-numeric AND will not be encoded to numeric
    downstream forces ``DataFrame.values`` into ``object`` dtype: every cell
    becomes a boxed Python object (~34 B/cell vs 8 B for float64), exploding
    driver memory (OOM at ``pdf_to_X`` ``to_numpy``) and later failing
    LightGBM's float cast. Prevented by declaring the column categorical (so it
    is integer-encoded) or dropping it.

    ``feature_kinds`` maps each *feature* column to ``"numeric"`` or
    ``"nonnumeric"``; the caller classifies using its own dtype vocabulary
    (Spark simpleString via :func:`spark_dtype_is_numeric` at the dataset gate,
    or pyarrow types at the training-read backstop). ``will_be_encoded`` is the
    set of feature columns that are non-numeric now but become numeric
    downstream (declared categoricals, incl. deferred identity categoricals).
    Returns collect-all error strings sorted by column; empty means OK.
    """
    errors: list[str] = []
    for col in sorted(feature_kinds):
        if feature_kinds[col] != "numeric" and col not in will_be_encoded:
            errors.append(
                f"feature column {col!r} is non-numeric and is not declared "
                f"categorical, so it would become an un-encoded object-dtype "
                f"model feature (OOM at pdf_to_X.to_numpy, then a LightGBM "
                f"float-cast error). If {col!r} is a categorical feature, add it "
                f"to dataset.prepare_model_input.categorical_columns (it is then "
                f"integer-encoded); if it is not a model feature, add it to "
                f"dataset.prepare_model_input.drop_columns."
            )
    return errors


# ---------------------------------------------------------------------------
# B9 — model_input feature columns must all be the declared storage type
# ---------------------------------------------------------------------------


def feature_storage_type_errors(
    feature_types: dict[str, str],
    declared: str,
) -> list[str]:
    """B9 invariant — the single definition.

    Every model feature column in ``train_model_input`` must be stored as the
    one type ``dataset.numeric_feature_storage_type`` declares. Two distinct
    failures, one rule:

    * **heterogeneous** — one ``int64`` column among ``float32`` ones. The
      training read allocates *one* matrix, so a single wider column decides the
      dtype of every other column: 1,000 float32 columns cost 4 B/cell, and the
      same frame with one int64 among them costs 8 B/cell. At 24,000,000 rows
      that is the difference between 89 GiB and 179 GiB on a 128 GiB driver —
      not "OOMs partway", but cannot be allocated at all.
    * **homogeneous but wrong** — every column ``float64`` under a ``float32``
      declaration. Nothing collides and nothing is lost, so no value-level gate
      can see it; it just silently costs twice the memory the declaration
      promised, and makes the declaration a comment rather than a fact.

    ``feature_types`` maps each feature column to its storage type spelled in
    the *declaration's* vocabulary (``"float32"`` / ``"float64"`` / whatever
    else the file actually holds); the caller classifies, as it does for B6.
    Deferred identity categoricals are the caller's exemption to make — they
    are stored raw (a string ``prod_name``) by contract and encoded per batch at
    read time, so they are never in this mapping.

    Why this is a *data* gate and not a config one: nothing in the config is
    wrong when it fires. The parquet was built by an older dataset run, before
    the declaration changed or before the cast covered that type — so the remedy
    is always a dataset rebuild, and the message says so rather than naming a
    key to edit.

    Returns collect-all error strings sorted by column; empty means OK.
    """
    errors: list[str] = []
    for col in sorted(feature_types):
        actual = feature_types[col]
        if actual == declared:
            continue
        errors.append(
            f"B9: feature column {col!r} is stored as {actual} but "
            f"dataset.numeric_feature_storage_type declares {declared}. The "
            f"training read allocates one matrix for all feature columns, so a "
            f"single column of another type sets the dtype for every column. "
            f"Rebuild the dataset (the cast in build_model_input is what "
            f"converges the types); do not widen the declaration to match a "
            f"stale parquet."
        )
    return errors


# ---------------------------------------------------------------------------
# B7 — a carry column that also lives in feature_table must be dropped
# ---------------------------------------------------------------------------


def carry_column_collision_errors(
    carry_columns: list[str],
    feature_table_columns: set[str] | list[str],
    drop_columns: list[str],
    identity_columns: list[str],
    label_column: str,
) -> list[str]:
    """B7 invariant — the single definition.

    ``dataset.carry_columns`` names columns the key-selecting nodes pull out of
    **sample_pool** on top of the identity key; ``prepare_model_input.
    drop_columns`` is a blacklist over **feature_table** columns. They act on
    different tables, so listing one name in both is not a contradiction — but
    a column that is carried *and* exists in feature_table ends up on both sides
    of the ``build_model_input`` join, and Spark fails with an opaque
    ``Reference 'x' is ambiguous``.

    The invariant is therefore an **exclusion, not an obligation**: a column may
    be carried or may be a model feature, never both. Two edits satisfy it and
    they mean different things —

    - add it to ``drop_columns``: still carried, no longer a feature;
    - remove it from ``carry_columns``: still a feature, and whatever
      sample-weight key needed it must come from another column.

    Which one is right depends on what the column is *for*, which this predicate
    cannot know, so the error states both. Prescribing only the drop (the way
    this rule was first written) would push every reader into quietly dropping a
    model feature and rebuilding the dataset to do it. The rule is written
    nowhere in the config, which is why it is a gate. See ADR-0004.

    Not covered by B6: B6 only fires on a *non-numeric* undeclared feature
    column, so a numeric carry column (or one declared categorical) sails past
    it and hits the ambiguous-reference crash instead. Where B6 does happen to
    fire on the same column its advice is actively misleading — it offers
    "declare it categorical" as a fix, which keeps the collision.

    ``identity_columns`` and ``label_column`` are excluded because they cannot
    collide however they are configured, so flagging them would demand a
    config edit that changes nothing while busting ``base_dataset_version``:

    - an identity column named in ``carry_columns`` is *not* copied a second
      time — a split's output columns are the identity key plus only the
      carry entries not already in it (``key_output_columns``,
      ``pipelines/dataset/steps/sampling.py``), and the base
      key is coalesced by the join itself.
    - the label and non-categorical identity columns are excluded from
      ``feature_columns`` by ``compute_feature_columns`` regardless of
      ``drop_columns``, so they never reach the feature side of the join.

    Verified by running the real ``build_model_input`` both ways: with an
    identity column carried and undropped it completes normally, with a
    non-identity one it raises ``Reference 'cust_segment_typ' is ambiguous``.

    ``feature_table_columns`` is any container of feature_table's column names
    (the gate hands in the keys of the ``feature_table.dtypes`` mapping it has
    already read — metastore metadata, no scan). Pure (no Spark). Returns
    collect-all error strings sorted by column; empty list means OK.
    """
    dropped = set(drop_columns)
    in_feature_table = set(feature_table_columns)
    cannot_collide = set(identity_columns) | {label_column}
    errors: list[str] = []
    for col in sorted((set(carry_columns) & in_feature_table) - cannot_collide):
        if col in dropped:
            continue
        errors.append(
            f"column {col!r} is in dataset.carry_columns and is also a column of "
            f"feature_table, so build_model_input would join two frames that "
            f"both carry {col!r} and Spark fails with "
            f"\"Reference '{col}' is ambiguous\". A column can be carried or be "
            f"a model feature, not both — pick one, in parameters_dataset.yaml: "
            f"(a) if {col!r} is metadata you weight by, add it to "
            f"dataset.prepare_model_input.drop_columns and keep it in "
            f"dataset.carry_columns — appearing in both keys is then the "
            f"intended configuration, not a contradiction; "
            f"(b) if {col!r} is a model feature, remove it from "
            f"dataset.carry_columns and source any sample-weight key that needed "
            f"it from another column. Choosing (a) for a column you actually "
            f"wanted as a feature silently drops it from the model and rebuilds "
            f"the dataset, so check which one {col!r} is before editing."
        )
    return errors


# ---------------------------------------------------------------------------
# A11/A12/A13 — compare-source predicates (multi-model comparison feature)
# ---------------------------------------------------------------------------

_COMPARE_KINDS = {"model_version", "external_hive"}
_VALID_UNMAPPED = {"fail", "drop"}
# Same-stack Hive tables a model_version compare source may read from.
# Mirror of evaluation.comparison.sources.MODEL_VERSION_SOURCES; A11 is the
# config-static gate, the loader checks again at read time.
_VALID_MODEL_VERSION_SOURCES = {
    "enriched_eval_predictions",
    "ranked_predictions",
    "training_eval_predictions",
}


def _required_external_columns(parameters: dict) -> set[str]:
    """Columns an ``external_hive`` compare source must declare, as schema roles.

    Every entry is a *role* resolved through :func:`get_schema` — the identity
    columns (time + entity + item) plus score — never a literal column name.
    ``entity`` is a list, so a multi-column entity requires all of its columns.
    """
    schema = get_schema(parameters)
    return set(schema["identity_columns"]) | {schema["score"]}


def compare_source_well_formed_errors(parameters: dict) -> list[str]:
    """(A11) Each evaluation.compare_sources[*] is well-formed.

    Returns list of error messages (empty when all sources valid).
    """
    sources = (
        (parameters.get("evaluation", {}) or {}).get("compare_sources", {}) or {}
    )
    errs: list[str] = []
    for key, src in sources.items():
        if not isinstance(src, dict):
            errs.append(f"(A11) compare_sources[{key!r}] must be a dict, got {type(src).__name__}")
            continue
        if "kind" not in src:
            errs.append(f"(A11) compare_sources[{key!r}] missing 'kind'")
            continue
        kind = src["kind"]
        if kind not in _COMPARE_KINDS:
            errs.append(
                f"(A11) compare_sources[{key!r}].kind={kind!r} not in {sorted(_COMPARE_KINDS)}"
            )
            continue
        if "label" not in src:
            errs.append(f"(A11) compare_sources[{key!r}] missing 'label'")
        if kind == "model_version":
            if "model_version" not in src:
                errs.append(f"(A11) compare_sources[{key!r}] kind=model_version missing 'model_version'")
            if "columns" in src:
                errs.append(
                    f"(A11) compare_sources[{key!r}] kind=model_version must not declare 'columns' "
                    "(same-stack source uses ranked_predictions schema)"
                )
            if "prod_mapping" in src:
                errs.append(
                    f"(A11) compare_sources[{key!r}] kind=model_version must not declare 'prod_mapping' "
                    "(same-stack source uses identical prod universe)"
                )
            if "source" in src and src["source"] not in _VALID_MODEL_VERSION_SOURCES:
                errs.append(
                    f"(A11) compare_sources[{key!r}].source={src['source']!r} "
                    f"not in {sorted(_VALID_MODEL_VERSION_SOURCES)}"
                )
        elif kind == "external_hive":
            if "table" not in src:
                errs.append(f"(A11) compare_sources[{key!r}] kind=external_hive missing 'table'")
            cols = src.get("columns", {}) or {}
            missing = _required_external_columns(parameters) - set(cols.keys())
            if missing:
                errs.append(
                    f"(A11) compare_sources[{key!r}].columns missing required keys: {sorted(missing)}"
                )
            if not src.get("prod_mapping"):
                errs.append(f"(A11) compare_sources[{key!r}] kind=external_hive missing 'prod_mapping'")
            policy = src.get("unmapped_policy", "fail")
            if policy not in _VALID_UNMAPPED:
                errs.append(
                    f"(A11) compare_sources[{key!r}].unmapped_policy={policy!r} "
                    f"not in {sorted(_VALID_UNMAPPED)}"
                )
    return errs


def compare_source_key_exists(parameters: dict, key: str | None) -> dict | None:
    """(A12) Resolve `key` against evaluation.compare_sources or raise.

    Returns the source dict, or None when `key` is None.
    """
    if key is None:
        return None
    sources = (
        (parameters.get("evaluation", {}) or {}).get("compare_sources", {}) or {}
    )
    if key not in sources:
        available = sorted(sources.keys())
        raise ConfigConsistencyError(
            f"(A12) --compare/--compare-only key {key!r} not in "
            f"evaluation.compare_sources. Available: {available}"
        )
    return sources[key]


def resolved_env_dir(conf_dir: Path | str, env: str) -> Path:
    """(A30) Resolve the ``conf/<env>`` overlay directory or raise.

    Returns the directory :class:`~recsys_tfb.core.config.ConfigLoader` will
    deep-merge over ``conf/base``. Raises ``ConfigConsistencyError`` when it is
    absent: the loader reads a missing overlay as an empty one, so without this
    a typo'd ``--env`` runs the whole pipeline on base config and reports
    nothing at all.

    Takes ``conf_dir`` as an argument rather than resolving ``Path.cwd()``
    itself so the predicate stays pure and testable — the CLI passes the same
    path it then hands to ``ConfigLoader``, which is what makes the gate and the
    loader unable to disagree about which directory is meant.
    """
    root = Path(conf_dir)
    name = (env or "").strip()
    if not name:
        raise ConfigConsistencyError(
            f"(A30) --env is empty. It must name a directory under {root} — "
            f"the layer deep-merged over conf/base. An empty value resolves "
            f"back to {root} itself, which merges nothing."
        )
    env_dir = root / name
    if env_dir.is_dir():
        return env_dir
    existing = (
        sorted(child.name for child in root.iterdir() if child.is_dir())
        if root.is_dir()
        else []
    )
    raise ConfigConsistencyError(
        f"(A30) --env={env!r} points at {env_dir}, which does not exist. "
        f"conf/<env> is deep-merged over conf/base and a missing directory "
        f"merges nothing, so the run would silently use conf/base alone. "
        f"Directories under {root}: {existing} (of these, only env overlays "
        f"are valid here — conf/sql and conf/spark-local are not)."
    )


def _iso_date(value) -> str | None:
    """Normalise a snap_date to ``YYYY-MM-DD``; ``None`` when unparseable.

    Handles the three forms a snap_date reaches us in: a quoted yaml string, an
    unquoted yaml scalar (PyYAML builds a ``datetime.date``), and a
    ``pd.Timestamp`` (a ``datetime`` subclass) held by a caller.
    """
    if isinstance(value, _datetime.datetime):
        return value.date().isoformat()
    if isinstance(value, _datetime.date):
        return value.isoformat()
    if isinstance(value, str):
        text = value.strip()
        for parse in (_datetime.date.fromisoformat, _datetime.datetime.fromisoformat):
            try:
                return _iso_date(parse(text))
            except ValueError:
                continue
        # ``datetime.fromisoformat`` is the wider of the two on Python 3.10 and
        # covers "2026-01-31T00:00:00" / "2026-01-31 00:00:00" — forms that
        # ``pd.Timestamp`` accepts everywhere else in the pipeline, so rejecting
        # them here would make A21 stricter than the code it guards.
        return None
    return None


def _resolved_rebuild_dates(
    declared, rebuild_dates, source: str
) -> list[str]:
    """(A21) Normalise ``--rebuild-dates`` against a configured month list.

    Returns the sorted, deduplicated, ``YYYY-MM-DD`` list, or ``[]`` when the
    flag was not passed. Raises ``ConfigConsistencyError`` when any value is
    malformed or names a month ``declared`` does not list.

    Why fail loud rather than ignore: a pipeline only ever processes configured
    months, so an unconfigured ``--rebuild-dates`` value would be a silent
    no-op — the operator would come away believing a stale month had been
    recomputed. That is the exact failure mode ADR-0002's escape hatch exists
    to prevent, so it must not have a silent edge.

    ``source`` names the config key in the error messages. Three commands take
    this flag against two different keys (``dataset`` and ``training`` against
    ``dataset.test_snap_dates``, ``inference`` against ``inference.snap_dates``
    — the two wrappers below), and a message naming the wrong one sends the
    operator to the wrong yaml block.
    """
    if not rebuild_dates:
        return []

    declared = declared or []
    # Silently dropping an unparseable configured value would make the subset
    # check below compare against an incomplete set, and the resulting message
    # ("configured: []") would point the operator at the flag when the fault is
    # in the yaml. Name the real culprit instead.
    unreadable = [d for d in declared if _iso_date(d) is None]
    if unreadable:
        raise ConfigConsistencyError(
            f"(A21) {source} holds unreadable date(s) "
            f"{unreadable!r}. Expected YYYY-MM-DD."
        )
    configured = {_iso_date(d) for d in declared}

    malformed = [d for d in rebuild_dates if _iso_date(d) is None]
    if malformed:
        raise ConfigConsistencyError(
            f"(A21) --rebuild-dates got non-ISO value(s) {malformed!r}. "
            "Expected YYYY-MM-DD."
        )

    requested = sorted({_iso_date(d) for d in rebuild_dates})
    unknown = [d for d in requested if d not in configured]
    if unknown:
        raise ConfigConsistencyError(
            f"(A21) --rebuild-dates names month(s) {unknown} that are not in "
            f"{source} (configured: {sorted(configured)}). "
            "Only a configured month can be rebuilt — the pipeline never "
            f"processes a month the config does not list, so this would have "
            f"been a silent no-op. Add it to {source} first, or "
            "drop it from --rebuild-dates."
        )
    return requested


def resolved_rebuild_dates(parameters: dict, rebuild_dates) -> list[str]:
    """(A21) ``--rebuild-dates`` for the dataset and training commands."""
    return _resolved_rebuild_dates(
        (parameters.get("dataset", {}) or {}).get("test_snap_dates"),
        rebuild_dates,
        "dataset.test_snap_dates",
    )


def resolved_inference_rebuild_dates(parameters: dict, rebuild_dates) -> list[str]:
    """(A21) ``--rebuild-dates`` for the inference command.

    Scoped to ``inference.snap_dates`` rather than ``dataset.test_snap_dates``:
    inference's resume unit is a ``(snap_date, entity_bucket, item)`` partition
    of ``unranked_predictions``, and the months it can touch at all are the ones
    it is configured to score. Naming a month outside that list would be the
    same silent no-op A21 exists to reject.
    """
    return _resolved_rebuild_dates(
        (parameters.get("inference", {}) or {}).get("snap_dates"),
        rebuild_dates,
        "inference.snap_dates",
    )


def compare_mutual_exclusive_errors(compare: str | None, compare_only: str | None) -> list[str]:
    """(A13) --compare and --compare-only must not be passed together."""
    if compare is not None and compare_only is not None:
        return [
            f"(A13) --compare={compare!r} and --compare-only={compare_only!r} "
            "are mutually exclusive — pass at most one"
        ]
    return []


def post_training_snap_date_errors(parameters: dict, post_training: bool) -> list[str]:
    """(A22) Under ``--post-training``, evaluation.snap_date must be a test month.

    Returns error strings (empty list when fine); the CLI raises. Wired like
    A13 — it lives here, but the evaluation command calls it explicitly and
    hands it the mode flag; the date handling mirrors A21. It is deliberately
    NOT aggregated by ``validate_config_consistency``: that runs at CLI entry
    and never sees ``--post-training``, and the default monitoring mode reads
    inference output whose month legitimately need not be a test month, so an
    unconditional predicate would block a valid run.

    Reads the nested ``parameters['evaluation']['snap_date']`` — the same shape
    ``evaluation.nodes_spark.prepare_eval_data`` reads, deliberately without
    the CLI's flat-config fallback, so the value this guard checks cannot
    diverge from the value the run filters on.

    Why this must fail loud rather than lean on the downstream empty-result
    guard in ``prepare_eval_data``: post-training evaluation reads
    ``training_eval_predictions``, which holds every month ever predicted for
    this ``model_version`` — and since ADR-0001 took test dates out of the
    version identity, months accumulate there across config edits. A snap_date
    the config no longer lists therefore still finds rows, and the run produces
    a report that looks entirely normal while measuring a month the current
    config does not evaluate. The empty-result guard only fires on zero rows,
    and only after a Spark session and a full table read.
    """
    if not post_training:
        return []

    declared = (parameters.get("dataset", {}) or {}).get("test_snap_dates") or []
    # Same reasoning as A21: silently dropping an unparseable configured value
    # would make the membership test below compare against an incomplete set
    # and point the operator at evaluation.snap_date when the fault is in the
    # dataset yaml. Name the real culprit instead.
    unreadable = [d for d in declared if _iso_date(d) is None]
    if unreadable:
        return [
            f"(A22) dataset.test_snap_dates holds unreadable date(s) "
            f"{unreadable!r}. Expected YYYY-MM-DD."
        ]
    if not declared:
        # Distinct wording from the membership branch below: "you configured no
        # test months at all" and "you picked the wrong one of several" call for
        # different fixes, and the membership message would render the useless
        # "dataset.test_snap_dates: []".
        return [
            "(A22) --post-training needs a configured test month, but "
            "dataset.test_snap_dates is empty. Add the month you want to "
            "evaluate and run dataset + predict for it first."
        ]
    configured = sorted({_iso_date(d) for d in declared})

    raw = (parameters.get("evaluation", {}) or {}).get("snap_date")
    snap = _iso_date(raw)
    if snap is None:
        return [
            f"(A22) evaluation.snap_date={raw!r} is not a readable ISO date "
            f"(YYYY-MM-DD). --post-training evaluates exactly one configured "
            f"test month; set it to one of {configured}."
        ]

    if snap not in configured:
        return [
            f"(A22) evaluation.snap_date={snap!r} is not a test month "
            f"(dataset.test_snap_dates: {configured}). --post-training reads "
            "training_eval_predictions, which accumulates every month ever "
            "predicted for this model_version, so an unlisted month can still "
            "return rows and yield a normal-looking report for a month this "
            "config does not evaluate. Add it to dataset.test_snap_dates and "
            "rerun dataset + predict, or point evaluation.snap_date at a "
            "configured month. (Monitoring mode — no --post-training — is not "
            "subject to this rule.)"
        ]
    return []


#: The four ``dataset.*_snap_dates`` splits A24 keeps disjoint, in the order
#: their pairs are reported.
_DATE_SPLIT_NAMES = ("train", "calibration", "val", "test")


def _split_day_labels(values) -> dict:
    """Map each configured snap_date to the calendar day it names.

    Returns ``{comparison key: literal as written}``. The key is a
    midnight-normalised ``pd.Timestamp``, so two literals naming the same day
    compare equal however they were spelled; the value keeps the operator's own
    text, because that is what they will search their yaml for.

    Parsing is ``pd.Timestamp`` rather than this module's ``_iso_date`` because
    on Python 3.10 ``date.fromisoformat`` rejects ``"2026-1-31"`` outright, and
    A24 has to be able to see that it means the same day as ``"2026-01-31"``.
    Note this makes A24 no *looser* than A21/A22 — they truncate to the
    calendar day too (``_iso_date`` returns ``YYYY-MM-DD``), so a snap_date
    carrying a time-of-day is one day to all three.

    Only the forms ``_iso_date`` accepts may reach the parser. ``pd.Timestamp``
    reads a bare int as NANOSECONDS since the epoch, so an unquoted yaml
    ``20260131`` would silently become 1970-01-01 — and two different months
    written that way would both land there and be reported as an overlap that
    does not exist. Anything else keeps its raw text as the key, tagged so it
    can never collide with a real day: two unreadable literals then overlap
    only when byte-identical, which is what the string comparison this
    predicate replaced would have said. Judging a literal *malformed* belongs
    to the per-key predicates (A21/A23), not to a disjointness check.
    """
    labels: dict = {}
    for value in values:
        day = None
        if isinstance(value, (str, _datetime.date)):
            # _datetime.date also covers datetime and pd.Timestamp (subclasses).
            try:
                day = pd.Timestamp(value)
            except (ValueError, TypeError, OverflowError):
                day = None
        if day is None or day is pd.NaT:
            labels[("unparsed", str(value))] = str(value)
        else:
            labels[day.normalize()] = str(value)
    return labels


def train_snap_dates_errors(parameters: dict) -> list[str]:
    """(A23) ``dataset.train_snap_dates`` must be present, a list, and non-empty.

    Returns error strings (empty list when fine); the dataset command raises.
    Not aggregated by :func:`validate_config_consistency` — that gate runs at
    the entry of *every* command, and only the dataset pipeline reads this key,
    so aggregating it rejects a perfectly good ``feature_etl`` / ``source_etl``
    / ``inference`` config (issue #158 measured 9 unrelated tests blocked by
    exactly that). Wired on the dataset command like A21/A24.

    Four branches because they are four different fixes, and each carries
    wording the others do not:

    * **absent** — three sites index this key bare
      (``select_train_keys``, ``collect_dataset_snap_dates``,
      ``fit_preprocessor_metadata``), so today it is a raw ``KeyError`` raised
      inside a Spark node: 2-4 minutes of cold start before a message that
      names neither the config key nor the fix.
    * **not a list** — a bare string is iterable, so none of those three sites
      raise. They walk it character by character and try to read ``"2"`` as a
      date.
    * **empty** — the branch with no downstream guard at all. ``select_train_keys``
      calls ``restrict_to_months_or_all``, which leaves the pool **whole**
      rather than empty, so training silently draws from every month in
      ``sample_pool`` — the test months included, which turns their metrics
      into in-sample numbers. A24 cannot see it: an empty set overlaps nothing.
      ``split_train_keys``' degenerate guard (ADR-0005) only fires when
      ``train_dev_ratio != 0``, and ``0`` is a legal setting.
    * **unparseable entry** — fix that one entry, so the message names the
      offending literals and not the good ones.
    """
    ds = parameters.get("dataset", {}) or {}
    if "train_snap_dates" not in ds:
        return [
            "(A23) dataset.train_snap_dates is absent — it is required. Add "
            "the months to train on to conf/base/parameters_dataset.yaml; "
            "without it the run fails inside a Spark node minutes later."
        ]

    configured = ds["train_snap_dates"]
    if isinstance(configured, str) or not isinstance(configured, (list, tuple)):
        return [
            f"(A23) dataset.train_snap_dates must be a list of dates, got "
            f"{type(configured).__name__} {configured!r}. A bare string is "
            f"iterable, so nothing downstream raises — it is read one "
            f"character at a time."
        ]

    if not configured:
        return [
            "(A23) dataset.train_snap_dates is empty. That is not 'train on "
            "nothing': the month filter is skipped entirely and training draws "
            "from every month in sample_pool, test months included, which "
            "makes their metrics in-sample. Name the months to train on."
        ]

    # Named `unparseable_entries`, not `unreadable`: A21 already binds that
    # name in this module, and a mutation script anchoring on it would hit two
    # places (#158).
    unparseable_entries = [
        entry for entry in configured if _iso_date(entry) is None
    ]
    if unparseable_entries:
        return [
            f"(A23) dataset.train_snap_dates has {len(unparseable_entries)} "
            f"entry/entries that are not dates: {unparseable_entries!r}. "
            f"Write each as YYYY-MM-DD."
        ]
    return []


def date_split_overlap_errors(parameters: dict) -> list[str]:
    """(A24) The four dataset snap_date splits must be mutually disjoint.

    Returns error strings (empty list when fine); the dataset command raises.
    One error per overlapping pair, so a config with several collisions gets
    fixed in one pass rather than one run per pair.

    A month in two splits is not a config the pipeline can honour: the same
    rows would train the model and then measure it, and every metric computed
    from the second split silently becomes an in-sample number. Nothing
    downstream notices — the run succeeds and the report looks normal.
    """
    ds = parameters.get("dataset", {}) or {}
    days = {
        name: _split_day_labels(ds.get(f"{name}_snap_dates") or [])
        for name in _DATE_SPLIT_NAMES
    }

    errors: list[str] = []
    for i, a in enumerate(_DATE_SPLIT_NAMES):
        for b in _DATE_SPLIT_NAMES[i + 1:]:
            common = days[a].keys() & days[b].keys()
            if common:
                # Each side's OWN literal, not the shared normalised form: the
                # operator's next move is to grep their yaml, and a config that
                # wrote "2026-1-31" contains no "2026-01-31" to find.
                in_a = sorted(days[a][key] for key in common)
                in_b = sorted(days[b][key] for key in common)
                errors.append(
                    f"(A24) dataset.{a}_snap_dates {in_a} and "
                    f"dataset.{b}_snap_dates {in_b} name the same calendar "
                    f"day — a snap_date belongs to exactly one split. The "
                    f"comparison is by day, not by text, so the same date "
                    f"spelled two ways still collides. Drop it from whichever "
                    f"split should not own it."
                )
    return errors


def _test_month_key(value) -> str:
    """The month key the training cache uses, for one configured literal.

    Mirrors ``pipelines/training/steps/predict_months.py::month_dir``. Two
    routes were
    available and both were rejected, so the copy is a choice, not an
    oversight: importing the original back is a cycle (that module already
    imports this one), and moving the original *here* would put a cache-path
    helper in the invariants module — the directory layout is the training
    pipeline's concern, and this module only has to agree with it. (Contrast
    ``HPO_OBJECTIVES`` just above, which is imported rather than copied: a
    value domain *is* an invariant, so it belongs here.) The agreement is
    pinned by a test that runs both over the same literals, so a change to the
    cache's notion of "same month" fails loudly instead of leaving A26 quietly
    checking the wrong thing.

    Comparison is by this key and not by calendar day on purpose. A day-based
    key would also group ``"2026-1-31"`` with ``"2026-01-31"``, but those two
    produce *different* cache directories, so the second one finds no rows in
    Hive and the existing per-month precheck (``require_months_are_cached``)
    reports it by name. That failure is loud already; this predicate exists for
    the silent one.
    """
    return str(value).strip().replace("-", "")


def duplicate_test_month_errors(parameters: dict) -> list[str]:
    """(A26) ``dataset.test_snap_dates`` must not spell one month two ways.

    Returns error strings (empty list when fine); the training command raises.
    One error per colliding month, naming every spelling of it, so a config
    with several collisions is fixed in one pass.

    Two different literals that resolve to one cache month are a config the
    pipeline cannot honour: ``cache_test_model_input`` would key two entries on
    one directory and ``handle_paths`` would hand that directory to pyarrow
    twice, doubling every row of that month in the predictions — and
    ``configured_months`` would silently keep whichever literal it met
    first. Nothing downstream notices: the run succeeds and the report looks
    normal, just with one month's numbers computed off doubled rows.

    Repeats of the *same* literal are legal and stay legal — they collapse to
    one cache entry and change nothing. Only a difference in spelling is
    ambiguous, because then the Hive partition value differs between them and
    at most one of the two can be the month that was actually written.
    """
    configured = (parameters.get("dataset") or {}).get("test_snap_dates") or []

    spellings_by_month: dict[str, list[str]] = {}
    for value in configured:
        literal = str(value)
        spellings = spellings_by_month.setdefault(_test_month_key(value), [])
        if literal not in spellings:
            spellings.append(literal)

    errors: list[str] = []
    for month in sorted(spellings_by_month):
        spellings = sorted(spellings_by_month[month])
        if len(spellings) < 2:
            continue
        errors.append(
            f"(A26) dataset.test_snap_dates spells one month more than one "
            f"way: {spellings} all resolve to {month!r}. The Hive partition "
            f"value differs between them, so only one can be right, and the "
            f"local cache would hand the same directory to pyarrow once per "
            f"spelling — doubling that month's rows. Keep the ISO form "
            f"(YYYY-MM-DD) and drop the others."
        )
    return errors


def inference_grid_errors(parameters: dict) -> list[str]:
    """(A27) the inference scoring grid must not be degenerate.

    Returns error strings (empty list when fine); the inference command raises.
    One error per degenerate axis, each naming the full config key path, since
    the operator's next move is to grep their yaml. See the legend for why one
    code covers three keys and why this is not aggregated.

    Anything ``int()`` cannot read is *reported* rather than raised: raising
    out of a collect-all predicate would hide the other two axes, which is the
    behaviour this invariant exists to remove. The messages the node-body
    raises used carry over near-verbatim, consequence clause included — the
    grid is the same grid whichever layer notices it is empty.
    """
    inf = parameters.get("inference") or {}
    errors: list[str] = []

    if not (inf.get("snap_dates") or []):
        errors.append(
            "(A27) inference.snap_dates is empty; there is nothing to score. "
            "Refusing to rank or validate an unrestricted table (every "
            "historical month would be republished)."
        )

    if "entity_buckets" in inf:
        try:
            n_buckets = int(inf["entity_buckets"])
        except (TypeError, ValueError):
            errors.append(
                f"(A27) inference.entity_buckets must be a whole number at "
                f"least 1, got {inf['entity_buckets']!r}, which is not a "
                f"number at all. Remove the key to take the default."
            )
        else:
            if n_buckets < 1:
                errors.append(
                    f"(A27) inference.entity_buckets must be at least 1, got "
                    f"{n_buckets}. Zero buckets means zero chunks, which "
                    f"would look like a successful run that scored nobody."
                )

    if not (inf.get("products") or []):
        errors.append(
            "(A27) inference.products is empty; there is nothing to rank."
        )

    return errors


def entity_columns_declared_errors(
    parameters: dict,
    declared_columns: list[str] | None,
    target_name: str,
) -> list[str]:
    """(A28) the prediction write target must declare every ``schema.entity`` column.

    Returns error strings (empty list when fine); the training command raises.

    ``schema.entity`` is a list by design and the predict node writes all of
    it, but ``HiveTableDataset.save`` ends with ``df.select(*declared)`` — an
    entity column its catalog entry never declared is dropped there with no
    error, no warning and no log line. The table stays perfectly valid; the
    published rows just identify the wrong thing. Every downstream consumer
    groups on the full entity tuple (``evaluation/metrics_spark`` uses
    ``[time] + entity``), so the whole run's metrics silently answer a
    different question.

    **This module never reads the catalog** — the caller does, and passes what
    it read. ``declared_columns`` is the answer from the dataset object's
    :attr:`~recsys_tfb.io.hive_table_dataset.HiveTableDataset.declared_columns`
    (the CLI already asks dataset objects about themselves this way for
    ``existing_partition_values``). Keeping the predicate pure is what lets it
    live here at all: the aggregator cannot see a catalog, but a command can,
    exactly as A12/A13/A21/A22/A24/A26 see things the aggregator cannot.

    ``None`` means the entry infers its schema from the DataFrame
    (``columns: "auto"``), which drops nothing — there is no declaration to
    fall short of, so there is nothing to report.

    NOT aggregated by :func:`validate_config_consistency`: that gate takes
    parameters alone and runs at the entry of every command, while this needs
    the resolved catalog and the harm is training-only. Wired like A24/A26.

    Deliberately has NO runtime backstop in the node (as A24): once the command
    has compared the two declarations there is nothing data-dependent left to
    re-check, and a node-body copy is exactly what ADR-0014 set out to remove.
    """
    if declared_columns is None:
        return []

    entity_cols = get_schema(parameters)["entity"]
    missing = [c for c in entity_cols if c not in declared_columns]
    if not missing:
        return []
    return [
        f"(A28) catalog entry {target_name!r} does not declare entity "
        f"column(s) {missing}; schema.entity is {entity_cols}. A Hive save "
        f"keeps only declared columns, so those columns would be dropped from "
        f"every written row without an error. Add them to that entry's "
        f"`columns:`."
    ]
