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
  ``categorical_columns`` but absent from ``schema.categorical_values``. The
  cell may be the list or ``from_train_data`` (#379, the item list counted
  from the train months); absent is refused either way, so forgetting it
  never switches modes silently. Predicate: ``require_item_list_declared``
  (run by ``schema.validate_schema_config`` and ``resolved_item_values``;
  runtime backstop: ``require_declared_categoricals`` in
  ``pipelines/dataset/steps/categoricals.py``, run by
  ``fit_preprocessor_metadata``, which raises ``DataConsistencyError``).
  With a counted list ``resolved_item_values`` refuses instead of returning
  the string's letters; every reader asks ``item_list_counted_from_data``
  first.
* A4 — ``inference.products`` ≠ ``schema.categorical_values[item]``. Only for
  a listed item list; a counted one is A52's. Predicate:
  ``inference_products_mismatch``.
* A5 — a ``sample_ratio_overrides`` key references an item value absent from
  the item list. Predicate: ``override_unknown_items``; the message both
  sites raise: ``override_unknown_item_errors``. With a counted list (#379)
  there is none at the CLI entry: the predicate takes the list as an
  argument, and ``fit_preprocessor_metadata`` runs it once it has counted
  the list (a slice that skips the fit skips it too).
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
      ``dataset.carry_columns`` ∪ declared categorical columns (cross-file:
      the column would be absent from the train model_input parquet, so the
      weight silently no-ops). Predicate: ``weight_key_columns_unavailable``.
    - A9b — a ``sample_weights`` key whose '|'-segment count ≠
      ``len(sample_weight_keys)`` (silently never matches). Predicate:
      ``weight_key_arity_mismatch``.
    - A9c — a ``sample_weights`` key whose product component (when
      ``schema.item`` is a weight key) ∉ the item list (mirrors A5).
      Predicate: ``weight_unknown_items``; the message both sites raise:
      ``weight_unknown_item_errors``. With a counted list (#379) it runs
      in training's ``select_features`` against the preprocessor's list; that
      node's output is memory-only, so every training slice runs it.
* A10 — an ``evaluation.segment_sources.<key>`` override has a key that is not
  in ``evaluation.segment_columns``, is incomplete (``table``,
  ``key_columns``, ``segment_column`` all required), or delivers a
  ``segment_column`` other than its key. A column without an override is not an error: it comes from
  the run mode's population table, and whether that table has the column is
  read from the metastore at run time by ``prepare_eval_data`` (ADR-0020
  bug 6), which this layer cannot see. Predicate:
  ``segment_source_override_errors``.
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
* A22 — under ``--post-training``, every date of ``evaluation.snap_date`` (one
  date or several, #374) must be a member of ``dataset.test_snap_dates``; with
  several, the message names the ones that are not. Post-training evaluation reads
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
* A24 — the three ``dataset.{train,val,test}_snap_dates`` splits
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
  ``training.hpo_objective`` ∈ the metric registry's names
  (``evaluation/metric_registry.py::METRIC_NAMES``) and
  ``training.final_model_strategy`` ∈ ``FINAL_MODEL_STRATEGIES``; an absent key
  keeps the node's own default and is clean, but an explicit YAML ``null`` is
  rejected (``dict.get`` hands the node ``None``, not the default). Predicate:
  ``training_hpo_finalize_param_errors``. Aggregated like A20 (one code per
  parameter family): both keys are optional, so the check costs a config that
  never names them nothing, while a typo in either is otherwise only found by
  the node that reads it — and ``final_model_strategy`` is read *after* the
  whole HPO search has run, so a typo there costs the entire search. Neither
  value domain is defined in the training pipeline, so the gate and the node
  that dispatches on the value cannot drift apart: the strategies are a tuple
  in this module, the objectives are the registry's rows (ADR-0028).
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
  is present at all — a number >= 1. With the item list counted from the data
  (#379) the item axis is the preprocessor's list, and its emptiness is
  checked where it exists (``predict_and_write_scores``, through
  ``inference.steps.chunk_plans.plan_scoring_chunks``). One code for three keys because they are
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
  primary key and no ``quality_checks``, so ``select_sample_keys``'s comment
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
* A33 — **MIGRATION-PERIOD CHECK, DELETE ME.** ``evaluation.product_categories``
  was renamed to ``evaluation.item_categories`` (#327): the framework does not
  speak the example deployment's business vocabulary. The rename is hard — no
  dual keys — so a conf that still spells the old name loses the whole category
  evaluation in silence: ``metrics_spark.hand_category_mapping`` reads the new
  name, gets ``{}``, returns ``None``, and every category section vanishes from
  the report while the run succeeds. This predicate turns that into a message
  naming the new key. Predicate: ``legacy_evaluation_key_errors``. Aggregated by
  ``validate_config_consistency`` so an operator learns at the next CLI entry,
  whichever command they run.

  **Retirement condition (this is a tool, not a guard):** delete the predicate,
  its wiring, its tests and this legend entry once the user confirms the company
  environment's own ``conf/`` has been updated. A permanent check here would be a
  compatibility layer for a key this repo no longer has, and the next reader
  would have to work out which of the two names is real. Leave the code slot
  ``A33`` retired-not-renumbered afterwards, per A16/A17/A18.
* A34 — ``evaluation.report.sections`` declares exactly the switches the
  report reads: its key set equals ``EVALUATION_REPORT_SECTIONS``, the constant
  ``evaluation/report_builder._section_on`` checks every name against. Both
  directions (ADR-0019 decision 6): a declared key nothing reads is a switch
  that does nothing (four shipped that way until #351), and a name the report
  reads that no conf declares is a section on by default with no line to turn
  it off (``diagnosis_links``). Containment in one direction passes one of the
  two. The constant lives in ``core/`` because ``core/`` must not import the
  report layer. A ``sections`` block that declares no switch (absent, null or
  empty) is not checked, a visible opt-out to every default; once any switch
  is declared the block is checked in full. Predicate:
  ``report_section_key_errors`` (returns errors; the evaluation command raises
  before Spark starts, collected with A22). NOT aggregated by
  ``validate_config_consistency``, for A24's reason: that gate runs at the
  entry of every command while only evaluation reads these keys, so a conf
  still carrying a dead switch must not stop dataset, training or inference
  (issue #158).
* A35 — the four source ETL commands' repeatable ``--var key=value`` flag
  (feature_etl/label_etl/sample_pool_etl/inference_population_etl, #370) must
  be well-formed, declared, and safe to merge onto the stage's YAML
  ``variables``: (a) every item has a ``=``; (b) every name is already
  declared in ``variables`` — ``SQLRunner``/``SQLRenderer`` only ever
  substitute declared names, so a typo'd flag is otherwise a silent no-op;
  (c) ``--var target_date=...`` is rejected — ``target_date`` is bound per
  iteration from ``--target-dates``/``target_dates``
  (``sql_runner.py``'s ``_table_variables``) and would silently overwrite it;
  (d) ``--var target_db=...`` is rejected — which Hive database gets written
  is meant to leave a file diff in the YAML, and the dataset pipeline
  downstream reads a different key (``hive.db``) that ``--var`` would not
  change, so overriding it here would look effective while quietly
  disconnecting the two; (e) the same name passed to ``--var`` more than once
  (no "last wins" is defined); (f) a YAML ``variables`` value of ``null``
  (``~`` — "must come from --var") with no matching ``--var`` this run,
  checked unconditionally regardless of ``--restart-from`` (a table skipped
  this run may still be reached by a later ``--restart-from`` run against the
  same config); (g) a YAML ``variables`` value that is neither a string nor
  null (a number, bool, list, ...) — ``SQLRenderer.render`` does
  ``str.replace`` and would raise a raw ``TypeError`` mid-render, possibly
  after earlier tables already wrote; (h) YAML ``variables`` declaring
  ``target_date`` at all, even a string — it is silently overwritten per date
  regardless, so the key only misleads a reader of the YAML — and because
  (h) owns this name unconditionally, ``target_date`` is skipped entirely by
  (g)/(f)/(k) so they cannot also fire and hand out contradictory advice for
  the same key; (i) YAML ``variables.target_db: ~`` — null asks for a
  ``--var`` override, but (d) means ``target_db`` can never be set that way,
  so it is unsatisfiable by construction; (j) checked first and
  structurally: ``variables`` itself must be a mapping (absent is fine) or
  the checks above cannot even run; (k) a variable's FINAL value (the YAML
  string, or the ``--var`` override when given) referencing another user
  variable via ``${...}`` — render substitutes one key at a time in the
  dict's iteration order, so whether such a reference expands depends on key
  order the config author cannot see, and an unexpanded one is not caught as
  "unresolved" either: a real Spark session's own ``${...}`` substitution
  (``spark.sql.variable.substitute``, on by default) silently turns it into
  an empty string rather than raising (measured against local Spark 3.3.2
  during this ticket's review). ``${target_date}`` is exempt from (k) even
  inside another variable's value — ``_table_variables`` always substitutes
  it last and it can never be declared in the YAML (h), so a reference to it
  cannot be affected by order — but a value mixing ``${target_date}`` with
  any OTHER reference is still rejected, and (k)'s message never prints the
  value, only the rejected ``${...}`` fragment(s), since the value may carry
  a secret pulled in via a YAML ``${env.X}``. (b) is reported once per
  distinct name, not once per repetition — a name repeated three times gets
  one (e) "passed 3 times" plus at most one (b)/(c)/(d), not three of each.
  Predicates: ``etl_cli_var_errors`` (returns errors; the ETL command raises
  before Spark starts) and the shared parsing/merge helpers
  ``parse_etl_var_flags`` / ``merged_etl_variables`` — one ``KEY=VALUE`` split
  implementation for both the gate and the actual override, so a change to
  the split rule cannot leave them disagreeing about what a flag means. NOT
  aggregated by ``validate_config_consistency``, for A21/A30's reason: it
  reads the ``--var`` CLI flags, which that gate is never given. Checked
  before the new SQL-side backstop this issue also adds:
  ``SQLRunner.check_renders`` renders (without touching Spark) every table a
  run would touch, for every target date, sharing ``_table_variables`` with
  the real run so a residual ``${...}`` (any name, not only word characters —
  ``SQLRenderer.render``'s unresolved-variable regex was widened for this)
  or a missing SQL file is caught in the same pass, before the Spark cold
  start.
* A36 — training needs at least one ``dataset.test_snap_dates`` month.
  Without one, nothing in the training pipeline objects early:
  ``cache_test_model_input`` loops zero times and returns ``{}``, and the run
  goes through the whole HPO search and the final fit before
  ``predict_and_write_test_predictions`` fails inside ``open_parquet_dataset``
  with a message that never names this key (#133). Absent, ``null`` and ``[]``
  fail the same check; the message says whether the key was absent or empty.
  Predicate: ``missing_test_month_errors`` (returns errors; the training
  command raises before Spark starts, collected with A26 — same key). NOT
  aggregated by ``validate_config_consistency``, for A24's reason: the dataset
  command reads an absent or empty list as "no test months" and runs, and
  ``test_snap_dates`` is in no version ID (``versioning.COVERAGE_ONLY_KEYS``).
  Blocks every training invocation, sliced or not, ``--list-nodes`` and
  ``--dry-run`` included — the predicate's docstring says why that is
  intended. No runtime backstop in the node, like A24/A28. Issue #133 calls
  this A29; that code was taken by the time it was built.
* A37 — a conf still spelling a config key that named the calibration
  mechanism removed in #411. Nothing reads these keys any more, so left in
  place they produce an uncalibrated model, a successful run, and no signal
  that the setting was ignored. **The key's presence is the failure, whatever
  the value**: they sit inside the subtrees hashed into
  ``base_dataset_version`` / ``model_version``, so a conf that keeps
  ``calibration: {enabled: false}`` computes different version IDs than one
  that deleted it, and requiring deletion is what makes two upgraded conf trees
  agree. Every retired key is listed in one message — the fix is a single edit.
  Keys: :data:`RETIRED_CALIBRATION_KEYS` — the two the calibrator itself read
  (#413) plus the four that configured the calibration data split (#414).
  Predicate: ``retired_calibration_key_errors``, aggregated by
  ``validate_config_consistency`` — unlike A24/A36 the harm belongs to no
  single pipeline, because the version IDs every command resolves are computed
  from these subtrees. Not a migration tool with a delete-by date, unlike A33:
  the mechanism is gone, not renamed.
* A38 — an optional-role column (``schema.columns.occasion`` /
  ``schema.columns.event``) declared in
  ``dataset.prepare_model_input.categorical_columns``. Identity columns have
  one way of becoming model features — being listed there — and ``schema.item``
  uses it (A2 *requires* it to). The optional roles deliberately do not get
  that exit: an impression id is a row label, and a model that splits on it
  memorises which impressions were clicked (ADR-0025). A second-resolution
  ``event`` timestamp is worse than useless rather than merely useless — it
  correlates with the fatigue effect the generator puts in the example data,
  so it trains and evaluates well and generalises to nothing. "Which hour the
  impression happened" as a feature is a column computed in a feature table,
  not the identity column reused. Predicate:
  ``optional_role_as_feature_errors``. Aggregated by
  ``validate_config_consistency``, like A1/A2: it takes parameters alone, and
  the mistake costs a whole training run to find otherwise — the column
  reaches ``model_input``, is encoded, and the run succeeds.
* A39 — the prediction write target must declare every optional-role column,
  for A28's reason and with A28's shape. ``HiveTableDataset.save`` ends with
  ``df.select(*declared)``, so an ``event`` column the catalog entry never
  declared is dropped in silence: the published table then holds several rows
  per item that nothing can tell apart, and evaluation's identity duplicate
  check raises on data that was correct when written. Separate code from A28
  rather than a widened one: A28's message says "schema.entity is ...", and a
  user who never declared an optional role should never read about one.
  Predicate: ``optional_role_columns_declared_errors``. NOT aggregated, for
  A28's reason — it needs the resolved catalog — and wired beside it.
* A40 — evaluation's **monitoring** mode (no ``--post-training``) with an
  optional role declared. Monitoring reads ``ranked_predictions``, which
  offline inference writes: inference builds its candidates as entity x item
  and ignores the optional roles entirely (ADR-0025 decision 1), so its rows
  carry no ``event`` column while ``label_table`` does. Joining the two on
  identity then matches nothing, or — if the join key silently narrows to the
  columns both sides have — matches one prediction row against every
  impression and inflates each query group. **Either way the answer is wrong
  rather than missing**, which is ADR-0021 decision 5's reason for stopping at
  the entry instead of letting the run produce a report. ``--post-training``
  is fully supported: it reads ``training_eval_predictions``, which A39 makes
  carry the columns. Predicate: ``optional_role_monitoring_errors`` (returns
  errors; the evaluation command raises, collected with A22/A34). NOT
  aggregated by ``validate_config_consistency``, for A22's reason: that gate
  runs at the entry of every command and cannot see ``--post-training``.
* A41 — a ``model_version`` compare source reading ``ranked_predictions``
  while an optional role is declared. That table is offline inference's
  output — the entity x item grid, no optional-role column (ADR-0025
  decision 1) — so it cannot be matched to this run's rows: A40's reason, in
  compare mode. Left to run, ``--compare`` failed deep in Spark on an
  unresolved column naming neither the role nor the source (#428). Separate
  code from A11 rather than a widened one, for A39's reason: A11 is about a
  source being well-formed, and a user who declared no optional role should
  never read about one. Predicate: ``optional_role_compare_source_errors``.
  Aggregated by ``validate_config_consistency``, beside A11: it takes
  parameters alone.
* A42 — ``evaluation.prediction_quality`` parameter domains (ADR-0024):
  ``n_bins`` and ``n_display_bins`` are ints >= 1 and the second divides the
  first (every display bin merges the same number of fine bins); ``top_n`` is
  an int >= 0; no other key (the on/off switch is
  ``evaluation.report.sections.prediction_quality``, so an ``enabled`` here
  would switch nothing). Defaults: ``PREDICTION_QUALITY_DEFAULTS``. Predicate:
  ``prediction_quality_param_errors`` (returns errors; the evaluation command
  raises, collected with A22/A34). NOT aggregated, for A34's reason.
* A43 — a conf still spelling ``evaluation.report.diagnostics.include_calibration``
  or ``.n_calibration_bins``. They configured ``calibration_bins``, a score-bin
  table on ``[0, 1]`` that nothing read; #381 removed it and kept one bin table,
  the prediction-quality family's. A37's shape: **presence is the failure,
  whatever the value** — ``false`` switches off nothing that still exists, and
  the keys sit under ``evaluation.report.diagnostics``, a fingerprinted subtree
  (``COMPUTED_KEYS``), so a conf that keeps them is not the conf that deleted
  them. Keys: :data:`RETIRED_CALIBRATION_BIN_KEYS`. Predicate:
  ``retired_calibration_bin_key_errors`` (returns errors; the evaluation
  command raises, collected with A22/A34/A42). NOT aggregated, unlike A37: the
  harm belongs to evaluation alone (A34's reason, issue #158).
* A44 — each ``dataset.{train,val,test}_zero_positive_group_ratio`` (ADR-0025
  decision 3: the share of a split's query groups holding no positive that
  the dataset pipeline keeps) is a number in [0, 1]. Absent is clean and
  resolves to today's behaviour (``ZERO_POSITIVE_GROUP_RATIO_DEFAULTS``: train
  1, val 0, test 0); an explicit YAML ``null`` and a boolean are rejected, for
  A31's reason. Predicate: ``zero_positive_group_ratio_errors``; resolver:
  ``resolved_zero_positive_group_ratio``. Aggregated by
  ``validate_config_consistency``, for A29/A31's reason: the train key feeds
  ``train_variant_id`` and the other two feed ``base_dataset_version``.
* A45 — the prediction write target must declare
  ``ZERO_POSITIVE_GROUP_WEIGHT_COL`` while ``dataset.test_zero_positive_group_ratio``
  is above 0, for A28/A39's reason: an undeclared column is dropped by
  ``HiveTableDataset.save`` in silence, and evaluation would then count each
  kept zero-positive group once instead of ``1 / r`` times. Only the test ratio
  counts — the table holds test predictions alone. Predicate:
  ``zero_positive_group_weight_declared_errors``. NOT aggregated, for A28's
  reason — it needs the resolved catalog — and wired beside A28/A39. Runtime
  backstop on training's test scoring: the binary-prediction passes in
  ``evaluation/metrics_spark.py`` raise on a missing column or a NULL weight
  rather than count each row once.
* A46 — ``evaluation.report.sections.prediction_quality`` on under
  ``--post-training`` while ``dataset.test_zero_positive_group_ratio`` is 0 (its
  default). That table is the test table scored, and the dataset pipeline
  dropped every test query group holding no positive before anything scored
  it, so the family — every row a binary prediction — comes out biased high
  and its "every row" population note is false. ADR-0024 decision 3's "before
  the filter" only reaches the filter inside evaluation; this is the one
  upstream of it. Monitoring mode is unaffected: nothing upstream filtered its
  rows. Predicate: ``prediction_quality_population_errors`` (returns errors;
  the evaluation command raises, collected with A22/A34/A42/A43). The switch
  is read by ``prediction_quality_on``, shared with the node that computes the
  family. NOT aggregated, for A22's reason: that gate cannot see
  ``--post-training``.
* A47 — offline inference with a candidate-level feature table declared
  (catalog entry ``candidate_feature_table``, ADR-0026). The inference pipeline
  reads ``feature_table`` alone, while a model trained under this
  configuration has the candidate-level columns among its features. Let
  through, the run starts Spark, reads the population and only then stops on
  ``Missing feature columns`` (``build_inference_population_features``),
  naming neither the table nor the way out. So it stops at the entry
  (ADR-0022 decision 4): the gate buys an early, explained stop, not
  protection from a silent wrong answer. Predicate:
  ``candidate_feature_table_inference_errors`` (takes whether the catalog
  declares the entry, as A40 takes its flag; the inference command raises).
  NOT aggregated: it needs the catalog, and the harm belongs to inference
  alone.
* A48 — ``training.hpo_objective`` in the registry's binary-prediction family
  (``metric_registry.BINARY_PREDICTION_METRICS``: ``pooled_average_precision``
  / ``macro_per_item_average_precision``, #430)
  while ``dataset.val_zero_positive_group_ratio`` is 0 (its default). Those
  objectives score every val row as a binary prediction, and at 0 the dataset
  pipeline dropped every val query group holding no positive, so average
  precision would be computed on a population filtered by the label it
  scores — A46's reason, on val instead of test. A ratio A44 rejects is left
  to A44. Predicate: ``hpo_objective_population_errors``. Aggregated by
  ``validate_config_consistency``, unlike A46: the ratio takes effect in the
  dataset pipeline, so the dataset command has to stop too.
* A49 — ``evaluation.query_filter.drop_all_positive_groups`` (#376, the
  all-positive query group filter) must be a ``bool`` when present; the block
  itself, when present, must be a mapping with no other key. The sole reader,
  ``evaluation/metrics.py::drop_all_positive_groups``, takes the value with
  plain truthiness and never raises, so a typo'd value or key would silently
  do nothing instead of failing loudly. Not under ``evaluation.metric``, A42's
  reason: a different reader (``metric_params``) drops unknown keys there
  without complaint. Predicate: ``query_filter_param_errors`` (returns
  errors; the evaluation command raises, collected with
  A22/A34/A40/A42/A43/A46). NOT aggregated, A42's reason (issue #158): only
  evaluation reads this key.
* A50 — ``evaluation.baseline.score`` (#397, the popularity baseline's score)
  must be one of ``BASELINE_SCORES`` (``count`` / ``rate``) when present;
  absent or ``null`` means ``count``. ``evaluation.baseline`` itself declares
  no key but ``BASELINE_KEYS``. The reader
  (``evaluation/baselines.py::baseline_score``) falls back to ``count`` on any
  falsy value and compares with ``==``, so a typo in the value (``rates``) or
  the key (``scroe``) would silently keep the count mode. Predicate:
  ``baseline_score_errors``. NOT aggregated, A42's reason (issue #158): only
  evaluation reads this key.
* A51 — ``evaluation.item_categories.column`` (#379, each item's category read
  off a sample_pool column): not together with a non-null ``mapping``, a
  non-empty string, and — while ``enabled`` — ``unmapped`` (if written) is
  ``singleton`` and the run is ``--post-training``, the one mode whose
  population is sample_pool (``--compare-only`` without the flag is
  monitoring, A22/A40's precedent). Column mode is read by
  ``item_category_column``, shared with the nodes that build and read the
  table. Predicate: ``item_category_column_errors`` (takes the flag, as A40
  does; the evaluation command raises, collected with A22/A34/A40/A42/A43/
  A46/A49/A50). NOT aggregated, for A22's reason: that gate cannot see
  ``--post-training``. Runtime backstops: ``prepare_eval_data`` raises
  ``RuntimeError`` when column mode meets a population other than
  sample_pool, and ``metrics_spark.hand_category_mapping`` refuses column
  mode (the table has to be passed in, never read from the conf).
* A52 — ``inference.products`` written while the item list is counted from
  the data (#379, ``schema.categorical_values[<item>]: from_train_data``).
  Offline inference scores every entity against the preprocessor's list then;
  a hand-written product list beside it is a second, stale-able source of the
  same list. Predicate: ``inference_products_with_counted_items_errors``.
  Aggregated by ``validate_config_consistency``, beside A4 (the same key's
  check for a listed item list).
* A53 — the top-level ``test_metrics`` block (ADR-0028): what training scores
  on test and over which time values. ``snap_date`` (the scored months; unset
  means every ``dataset.test_snap_dates`` month, :func:`scoring_snap_dates`)
  must not be an empty list, must be a subset of ``dataset.test_snap_dates``
  spelled as there (the prediction table is filtered by that text), and must
  not spell one month two ways (A26's rule). ``metrics`` / ``selection_metric``
  must name metrics in the registry (``evaluation/metric_registry.py``); no key
  outside the three, A50's reason. Predicate:
  ``scoring_param_errors`` (returns errors; the training command raises,
  collected with A26/A36 before Spark starts; ``scripts/promote_model.py``
  runs the same three, since the block gives it the metric and the months it
  ranks versions by). NOT aggregated, A24's reason (issue #158): no pipeline
  but training reads the block, which is in no version ID.
* A54 — a binary-prediction metric the run must record on test (the
  selection metric, written or following the HPO objective, or a name in
  ``test_metrics.metrics``) while test keeps no query group holding no
  positive: ``dataset.test_zero_positive_group_ratio`` is 0 in the config, or
  in the dataset version training reads (ADR-0028 decision 4). The number
  would be another population's than the val score HPO chose by. The HPO
  objective alone is withheld with a reason instead of stopping anything.
  Predicate: ``binary_test_metrics_verdict`` (errors and withheld reasons
  from one decision). Aggregated by ``validate_config_consistency``, for
  A48's reason — the ratio takes effect in the dataset pipeline; wired again
  at the training entry once the dataset version is resolved, against that
  version's manifest (the only place that knows it), and read by
  ``compute_test_metrics`` for the withheld reasons and as the runtime
  backstop.
* A55 — ``--only-test-months`` needs the train version the config names to
  have landed: ``train_model_input`` must hold partitions under this run's
  ``base_dataset_version`` and ``train_variant_id`` (ADR-0029 decision 12).
  The mode runs no train build, so it can only reuse a train version built
  earlier; changing a train-only key (the train sampling settings, and
  ``carry_columns``) moves ``train_variant_id`` alone, and before this check
  the run went ahead and training then read the empty variant as 0 rows,
  with no error anywhere (#334). Two messages, because they are two
  situations: nothing under the base at all (never built), or the base
  without this variant (a train-only setting changed).
  Predicate: ``train_version_landed_errors`` (returns errors; the dataset
  command raises before any node runs or any manifest is written). The facts
  are metastore partition listings the command collects through
  ``pipelines/dataset/run_contract.py``; this module still never reads the
  catalog (A28's reason). NOT aggregated: it reads a CLI flag and the
  metastore, neither of which the aggregator sees. After the run, the same
  evidence decides whether ``train_variants/latest`` moves
  (``run_contract.unlanded_train_tables``, which also asks for
  ``train_dev_model_input`` unless ``train_dev_ratio`` is 0) — a rule, not an
  invariant, so it carries no A-code.
* A56 — a catalog entry ``preprocessor_on_disk`` written by the deployment
  must name the ``preprocessor`` entry's file (ADR-0029 decision 13). It is
  the second name ``fit_preprocessor_metadata`` reads that file under before
  overwriting it (a node may not read and write one name), and evaluation's
  ``prepare_eval_data`` reads the evaluated model's list through it. The entry
  is optional because the file does not exist before a version's first run,
  so a path to anywhere else loads as ``None``, B19 takes it for a first run
  and checks nothing. The CLI derives the entry from ``preprocessor`` when the
  catalog leaves it out, which is the normal case; this catches the one left
  over. Predicate: ``preprocessor_on_disk_path_errors`` (returns errors). The
  CLI hands in the two filepaths when it builds the catalog for a pipeline
  whose nodes read the entry — before any node runs, and before
  ``--dry-run`` / ``--list-nodes`` return; this module never reads the
  catalog (A28's reason). NOT aggregated, for A28's reason too: it needs the
  resolved catalog.

The evaluation command's ``--rebuild-dates`` belongs to A21 (predicates
``resolved_baseline_rebuild_dates`` before Spark starts,
``baseline_rebuild_dates_absent_errors`` once sample_pool is listed): it only
exists for the positive-rate baseline's ``popularity_period_counts`` and is
refused when that path is not wired; each date must fall inside a lookback
window of ``evaluation.snap_date`` and be a time value sample_pool holds there.

Layer 1 invariants that hang off a single command instead of the aggregator,
because they need context the aggregator never sees: A12/A13 and A21 (CLI
flags), A22/A46/A51 (``--post-training``), A23/A24/A26/A27/A34/A36/A42/A43/A49/A50/A53 (config keys whose
harm belongs to one pipeline), A28/A39/A45/A47 (the resolved catalog), A30 (``--env``
+ the filesystem), A35 (the ``--var`` CLI flags), A55 (``--only-test-months`` + the
metastore). A56 needs the resolved catalog too, but hangs off no single command:
the CLI checks it where it builds the catalog, for every pipeline whose nodes
read ``preprocessor_on_disk``.

Layer 2 — data-stage validation (B1 + B5 + B6 + B7 + B8 + B9 + B10 + B11 + B12
+ B13 + B14 + B15 + B16 + B17 + B18 + B19 implemented and wired):

* B1 — sample_pool items ↔ declared items must be equal; label items ⊆
  declared items (unknown item values corrupt training or violate invariants).
  With the item list counted from the data (#379) there is no declared list:
  sample_pool is not compared (a val/test item the train months lack is a new
  item, warned about in ``build_val_model_input`` /
  ``build_test_model_input``, off the landed keys table), and label items ⊆
  sample_pool items instead.
  Predicate: ``item_coverage_errors`` (pure, no Spark); wired via
  ``validate_data_consistency`` (``pipelines/dataset/nodes.py``) as the
  first node of the dataset pipeline. B3 — a declared item has zero positives over
  the train window — intentionally NOT reported by ``item_coverage_errors``
  (deferred).
* B2 — label-window leakage columns reach features (specified but DEFERRED).
* B5 — a column declared in ``dataset.prepare_model_input.categorical_columns``
  has a type outside ``CATEGORICAL_DTYPES`` (string, the integer family,
  boolean) in feature_table. An allow-list since #407; before it B5 rejected
  only decimal/double/float, and date/timestamp/binary crashed the
  ``fit_preprocessor_metadata`` save after a full scan while complex types
  crashed the encoder. Each rejection names its type family's way out, shared
  with B6 and the config tool through ``categorical_dtype_problem``. Predicate:
  ``categorical_dtype_errors`` (pure, no Spark); wired via
  ``validate_data_consistency`` alongside B1 (reads ``feature_table.dtypes``,
  metastore metadata only — no scan). Runtime backstop:
  ``require_supported_categorical_dtypes`` (``pipelines/dataset/steps/categoricals.py``),
  run by ``fit_preprocessor_metadata`` before its vocabulary scan — a sliced run
  skips the gate, and the one-pass vocabulary collection there is not exact on a
  double/float column (``collect_set`` keeps each NaN and does not normalise
  -0.0).
* B6 — a feature column that is non-numeric (string / binary / date / timestamp /
  complex) and is NOT declared categorical (so never integer-encoded): it becomes
  an ``object``-dtype model feature → driver OOM at ``pdf_to_X`` ``to_numpy`` and
  a downstream LightGBM float-cast error. Predicate: ``nonnumeric_feature_errors``
  (with the ``spark_dtype_is_numeric`` classifier). Wired at TWO call sites — the
  dataset gate ``validate_data_consistency`` (prevents a rebuilt dataset baking it
  in) and a training-read backstop in ``io/extract.py`` (fails fast on an
  already-built parquet, before the expensive pandas read). The gate passes the
  column types in, so a column B5 would reject as a categorical is not told to
  become one (#407); the backstop passes none and keeps the generic advice.
  B4 is unused.

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
  ``exact_value_limit`` over ``SIGNIFICAND_BITS``. Asked first,
  ``numeric_precision_file_errors``: the gate found the files this run wrote,
  without which there is nothing to measure. Wired in
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

* B10 — a ``*_model_input`` table holds a different number of rows than the
  ``*_keys`` table it was built from. ``build_model_input`` LEFT joins the keys
  to ``label_table``, to ``preprocessed_feature_table`` and to the
  candidate-level feature table when one is declared (ADR-0026), at the keys'
  own grain, so the counts can only diverge when a right table holds a join key
  more than once — the "silently N-times-too-large dataset" that node's own
  comment names as the failure it fears. ``require_columns_present`` there
  covers only the other cause (a join key missing the item column); nothing
  covered this one, and the upstream ``max_duplicate_key_ratio`` contract does
  not close it (A32 passes when ``primary_key`` and ``quality_checks`` are both
  absent, and the framework lets a user supply source tables this repo's
  ``source_etl`` never wrote). Predicate: ``model_input_grain_errors`` (pure —
  it takes a split → ``SplitRowCounts`` mapping). Asked first, for each table
  compared whole, ``model_input_grain_scope_errors``: its version / variant
  scope holds any of its files, or both counts would be a vacuous 0. Wired in
  ``validate_model_input_grain`` (``pipelines/dataset/nodes.py``), which runs
  after the ``build_*_model_input`` nodes have landed their tables.

  **Facts come from parquet footer row counts, not from ``count()``** — the
  same cost invariant B8 works under (ADR-0006 and its amendments): a footer
  read costs one seek per file and is independent of how many rows the file
  holds. ``block.getRowCount()`` is the quantity B8's reader was already
  reading to interpret its min/max statistics; B10 only sums it.

  **All four splits are covered** (ADR-0029 decision 4). Every model_input
  lands straight out of ``build_model_input``, and every split's
  zero-positive group draw (ADR-0025 decision 3) runs on its keys, before
  the keys table lands — precisely so this pairing survives it: a drop after
  the build would make the counts differ on purpose, and a deliberate drop
  can hide a fan-out of any smaller size. Until decision 4 val / test drew
  on the built table instead, which is why this gate could not speak for
  them, and why a duplicate key confined to a month only val or test cover
  used to go unseen. train, train_dev and val are compared whole (every file
  under this version, and variant for the two train tables). test is
  compared over the months ``test_model_input_month_plan.to_process`` names,
  each month its own pair — both tables accumulate months under one version,
  ``build_test_model_input`` reads only those months of its keys, and a sum
  would let one month's fan-out cancel another's shortfall. A test month
  that fails has landed already, so its message says to rebuild it by name
  (``--rebuild-dates``); a plain re-run would skip it. An empty
  ``to_process`` means this run wrote no test month: the pair is reported as
  not written, not failed. A planned month neither table has a file for is
  0 = 0 (the draw emptied it), not a measurement failure.
* B11 — ``sample_pool`` or ``label_table`` is missing a column a declared
  optional role names (``schema.columns.occasion`` / ``schema.columns.event``).
  Declaring the role widens ``identity_columns``, which is
  what ``select_*_keys`` project by and what ``build_model_input`` LEFT joins
  the labels on, so a source table without the column fails — but as a raw
  Spark ``AnalysisException`` about an unresolved name, from whichever node
  happens to touch it first, naming neither the role nor the other table. This
  turns it into one message, before the label join, naming both. Checked
  against ``DataFrame.columns`` (metastore metadata, no rows), so it costs
  nothing and stays inside ADR-0006's cost invariant for this node.
  ``feature_table`` is deliberately NOT checked: it is entity-level and joins
  by ``base_key_columns``, which no optional role widens — requiring an
  impression column there would be the ``occasion``-mis-classification ADR-0025
  decision 2 warns about, written into a gate. Predicate:
  ``optional_role_source_column_errors``. Wired in ``validate_data_consistency``
  (``pipelines/dataset/nodes.py``) with the rest of Layer 2.
* B12 — a model feature named ``ZERO_POSITIVE_GROUP_WEIGHT_COL`` while
  ``dataset.val_zero_positive_group_ratio`` or ``test_zero_positive_group_ratio``
  is above 0: the val / test group drops put a column by that name on the
  keys, and the build carries it into the same frame as the features
  (ADR-0025 decision 3, ADR-0029 decision 4). Checked against the feature
  columns derived from the feature tables' metadata (both, when a
  candidate-level one is declared) — no rows. Only features count (val / test
  keys carry nothing else; a dropped column is no feature), and the train
  ratio adds no weight. Predicate:
  ``zero_positive_group_weight_collision_errors``. Wired in
  ``validate_data_consistency``. Runtime backstop, for a sliced run that
  skips the gate: ``build_model_input``, where the weight on the keys meets
  the feature — its output select cannot tell the two apart and Spark raises
  ``Reference ... is ambiguous``, the same backstop B7 has. Loud rather than
  named, and never an overwrite. (Until decision 4 the draw itself refused a
  frame that already held the column; it now draws over keys that hold
  identity and the label only, where no feature can be.)
* B13 — the candidate-level feature table (ADR-0026) is missing a column of
  ``identity_columns``. It joins on identity, so the join itself would fail —
  as an unresolved-name ``AnalysisException`` naming neither the table nor the
  reason. Checked against ``DataFrame.columns`` (metadata, no rows).
  Predicate: ``candidate_feature_table_key_errors``. Wired in
  ``validate_data_consistency``.
* B14 — a column (not identity, not the label, not dropped) is in both
  ``feature_table`` and the candidate-level feature table. Both are joined onto
  the same row, so it would arrive twice. Predicate:
  ``feature_table_overlap_errors``. Wired in ``validate_data_consistency``.
* B15 — two different combinations of a multi-column ``schema.columns.item``
  combine to the same value (``a-b`` + ``c`` and ``a`` + ``b-c`` are both
  ``a-b-c``), so two items would be counted, encoded and joined as one
  (ADR-0027 decision 2). A value that merely holds a ``-`` is fine. Checked on
  the distinct combinations ``sample_pool`` and ``label_table`` hold in the
  dataset windows — the same distinct B1 already reads, widened to the source
  columns, so no extra scan. Predicate: ``combined_item_collision_errors``.
  Wired in ``validate_data_consistency``.
* B16 — a table that carries items lacks one of a multi-column item's source
  columns, or already has a column named ``item`` that combining would
  overwrite. Column names only. Predicate: ``item_source_column_errors``.
  Wired in ``validate_data_consistency`` for ``sample_pool``, ``label_table``
  and the candidate-level feature table. Runtime backstop:
  ``utils.item_columns.combine_item_columns`` raises it at every entry that
  reads one of the user's tables, so evaluation refuses it too.
* B17 — one of a multi-column item's source columns has different types in
  ``sample_pool``, ``label_table`` and the candidate-level feature table.
  Each part is turned into text before combining, so ``7`` and ``7.0`` become
  different items and those rows stop joining, silently. Column types only
  (metastore metadata). Predicate: ``item_source_dtype_errors``. Wired in
  ``validate_data_consistency``.
* B18 — in column mode (``evaluation.item_categories.column``, A51), one item
  has two or more non-NULL values of that sample_pool column over the
  evaluated months, in one month or across them (#379). The
  category pass joins the table on the item alone, so the item's predictions
  would be copied into each category. Checked on the distinct (time, item,
  category) triples the table is built from, so no extra scan. Predicate:
  ``item_category_conflict_errors``. Wired in evaluation's
  ``prepare_eval_data`` (it reads sample_pool there, and only in column mode),
  not in ``validate_data_consistency``: the dataset gate does not know which
  months a later evaluation run will read.
* B19 — with the item list counted from the data (#379), the list counted
  now differs from the one in the preprocessor already on disk for the same
  ``base_dataset_version``. A listed list is in the version ID; a counted one
  is not, so the version does not move when the train-month data change, and
  overwriting would shift every item's code under the models trained on the
  old list. Predicate: ``item_list_drift_errors``. Wired in
  ``fit_preprocessor_metadata`` (which reads the file on disk through the
  optional catalog entry ``preprocessor_on_disk``, A6's other entry name),
  not in ``validate_data_consistency``: a slice starting at the fit skips the
  gate, and the fit is what would overwrite the file.

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
import re
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any, NamedTuple

import pandas as pd

from recsys_tfb.core.date_ranges import as_date_list, lookback_window_bounds
from recsys_tfb.core.group_utils import RANKING_OBJECTIVES
from recsys_tfb.core.schema import (
    COMBINED_ITEM_COLUMN,
    ITEM_LIST_FROM_TRAIN_DATA,
    ITEM_SEPARATOR,
    OPTIONAL_ROLE_KEYS,
    ENTITY_GROUPING_KEYS,
    get_schema,
)

#: ``parameters`` key the CLI hands the A21-validated ``--rebuild-dates`` value
#: to nodes under (values normalised to ``YYYY-MM-DD`` by
#: :func:`resolved_rebuild_dates`). It lives beside the predicate that produces
#: the value because two pipelines read it — training's cache/predict nodes
#: and inference's scoring node — and pipelines never import each other.
#: Dataset does not: its month plans take ``--rebuild-dates`` from the CLI.
REBUILD_SNAP_DATES_KEY = "_rebuild_snap_dates"

#: ``parameters`` key the training command hands ``compute_test_metrics`` the
#: test ratio the dataset version it reads was built with (read off that
#: version's manifest), for A54's second place (:func:`
#: binary_test_metrics_verdict`). Absent — a direct call — the version is the
#: one the config builds.
DATASET_TEST_RATIO_KEY = "_dataset_test_zero_positive_group_ratio"


class ConsistencyError(ValueError):
    """Base for all consistency failures (subclasses ValueError by design)."""


class ConfigConsistencyError(ConsistencyError):
    """Config self-contradiction detectable without data (Layer 1)."""


class DataConsistencyError(ConsistencyError):
    """Config disagrees with the actual data (Layer 2)."""


def collect_all_message(headline: str, errors: Sequence[str]) -> str:
    """Every finding of one gate under one headline, one finding per line.

    The shape the dataset pipeline's Layer-2 gates raise (or, under B8's
    ``truncate``, log) in. Collect-all rather than first-failure, so one fix
    pass clears every finding; one function so the three gates cannot drift
    apart in how they say it.
    """
    return f"{headline} ({len(errors)} issue(s)):\n- " + "\n- ".join(errors)


def _prepare_model_input(parameters: dict) -> dict:
    return (parameters.get("dataset", {}) or {}).get("prepare_model_input", {}) or {}


def require_item_list_declared(parameters: dict) -> None:
    """A3 — the item column, when it is a declared categorical (in
    ``prepare_model_input.categorical_columns``), has a
    ``schema.categorical_values`` cell: a list, or
    :data:`~recsys_tfb.core.schema.ITEM_LIST_FROM_TRAIN_DATA` (#379).

    Raises ``ConfigConsistencyError``. The one definition, run by
    ``schema.validate_schema_config`` at every CLI entry and by
    :func:`resolved_item_values`. A missing cell is refused rather than read
    as "count it from the data": forgetting it must not quietly switch modes.
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
            f"schema.categorical_values.{item} in parameters.yaml, or "
            f"{ITEM_LIST_FROM_TRAIN_DATA!r} to count it from the train months' "
            f"sample_pool."
        )


def item_list_counted_from_data(parameters: dict) -> bool:
    """Whether the item list is counted from the train months' sample_pool
    (``schema.categorical_values[<item>]:``
    :data:`~recsys_tfb.core.schema.ITEM_LIST_FROM_TRAIN_DATA`, #379) rather
    than listed. The one question every reader of the item list asks before
    reading it; in this mode the list exists only in the preprocessor
    (``category_mappings[<item>]``) once the dataset pipeline has fit it.
    """
    # Fast path, without resolving the schema: no cell holds the value at all.
    # Checks that read only one section (A27 reads `inference` alone) are
    # called with parameters that carry no schema.
    raw = ((parameters.get("schema") or {}).get("categorical_values") or {})
    if ITEM_LIST_FROM_TRAIN_DATA not in raw.values():
        return False
    schema = get_schema(parameters)
    cat_values = schema.get("categorical_values", {}) or {}
    return cat_values.get(schema["item"]) == ITEM_LIST_FROM_TRAIN_DATA


def resolved_item_values(parameters: dict) -> list[str]:
    """Canonical sorted list of the declared item values (the single source).

    Reads ``schema.categorical_values[schema.item]``. Raises
    ``ConfigConsistencyError`` for A3 (:func:`require_item_list_declared`),
    and when the item list is counted from the data
    (:func:`item_list_counted_from_data`): there is no declared list then, and
    sorting the cell's string would hand the caller its letters. A caller
    reads the list only after asking which mode it is in.

    Returns ``[]`` when the item column is not a declared categorical (or
    ``categorical_columns`` is absent). Callers relying on this as the single
    source of valid item values must ensure ``item_missing_from_categorical``
    (invariant A2) is validated upstream — ``validate_config_consistency``
    does this.
    """
    require_item_list_declared(parameters)
    if item_list_counted_from_data(parameters):
        item = get_schema(parameters)["item"]
        raise ConfigConsistencyError(
            f"schema.categorical_values[{item!r}] is {ITEM_LIST_FROM_TRAIN_DATA!r}: "
            f"the item list is counted from the train months' sample_pool and "
            f"lives in the preprocessor, not in the conf. This caller read it "
            f"as a declared list; it has to ask item_list_counted_from_data "
            f"first."
        )
    schema = get_schema(parameters)
    cat_values = schema.get("categorical_values", {}) or {}
    return sorted(cat_values.get(schema["item"], []))


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
    An item list counted from the data has no declared list to compare with;
    ``inference.products`` may not be written then at all (A52).
    """
    inf = parameters.get("inference") or {}
    if "products" not in inf or item_list_counted_from_data(parameters):
        return {"only_in_inference": [], "only_in_categorical": []}
    declared = set(resolved_item_values(parameters))
    products = set(inf.get("products") or [])
    return {
        "only_in_inference": sorted(products - declared),
        "only_in_categorical": sorted(declared - products),
    }


def inference_products_with_counted_items_errors(parameters: dict) -> list[str]:
    """A52 — ``inference.products`` written while the item list is counted
    from the data (#379).

    Offline inference scores every entity against the preprocessor's item
    list in that mode; a hand-written product list next to it would be a
    second source of the same list, and keeping it in step by hand is the
    very thing the mode removes. Refused rather than ignored, so a stale list
    cannot sit in the conf looking authoritative. Aggregated by
    ``validate_config_consistency``, beside A4 (the same key's check when the
    list is declared).
    """
    inf = parameters.get("inference") or {}
    if "products" not in inf or not item_list_counted_from_data(parameters):
        return []
    item = get_schema(parameters)["item"]
    return [
        f"A52: inference.products is written while "
        f"schema.categorical_values.{item} is {ITEM_LIST_FROM_TRAIN_DATA!r}. "
        f"Offline inference then scores every entity against the item list "
        f"the preprocessor counted from the train months; remove "
        f"inference.products."
    ]


def override_unknown_items(
    parameters: dict, items: Iterable[str] | None = None
) -> list[str]:
    """sample_ratio_overrides keys whose item component ∉ the item list (A5).

    Override keys are '|'-joined sample_group_keys values. If schema.item is not
    a sample_group_key there is no item component → nothing to check.

    ``items`` is the item list to check against; ``None`` means the declared
    one (:func:`resolved_item_values`). When the list is counted from the data
    (#379) there is none before the run, so the config-time call returns
    ``[]`` and the check runs again where the list is counted
    (``fit_preprocessor_metadata``), passing it in.
    """
    schema = get_schema(parameters)
    item = schema["item"]
    ds = parameters.get("dataset", {}) or {}
    group_keys = ds.get("sample_group_keys", [])
    if item not in group_keys:
        return []
    idx = group_keys.index(item)
    if items is None:
        if item_list_counted_from_data(parameters):
            return []
        items = resolved_item_values(parameters)
    declared = set(items)
    bad: set[str] = set()
    for key in (ds.get("sample_ratio_overrides") or {}):
        parts = str(key).split("|")
        if idx < len(parts) and parts[idx] not in declared:
            bad.add(parts[idx])
    return sorted(bad)


def _item_list_named(parameters: dict) -> tuple[str, str]:
    """``(where the item list lives, how to fix a key missing from it)`` for
    A5 / A9c's messages: the declaration when listed, the train months'
    count when counted (#379) — where the fix "declare the value" does not
    exist."""
    if item_list_counted_from_data(parameters):
        return ("the item list counted from the train months' sample_pool",
                "Fix the key(s).")
    return ("schema.categorical_values[item]",
            "Fix the key(s) or declare the value(s).")


def override_unknown_item_errors(
    parameters: dict, items: Iterable[str] | None = None
) -> list[str]:
    """A5 as the message both of its sites raise.

    ``items`` as in :func:`override_unknown_items`: ``None`` at the CLI entry
    (``validate_config_consistency``), the counted list in
    ``fit_preprocessor_metadata``. One text for both, so the code and the
    wording cannot drift between the listed and the counted mode.
    """
    unknown = override_unknown_items(parameters, items=items)
    if not unknown:
        return []
    where, fix = _item_list_named(parameters)
    return [
        f"A5: dataset.sample_ratio_overrides references item value(s) "
        f"{unknown} absent from {where} — the override silently never "
        f"matches. {fix}"
    ]


def optional_role_column_map(parameters: dict) -> dict[str, list[str]]:
    """``{role: [column, ...]}`` for every declared optional role; ``{}`` if none.

    The one place this module asks "which columns did the user add by
    declaring an optional role" — A38, A39, A40 and B11 all read it, so they
    cannot disagree about the answer, and a second optional role is added to
    :data:`~recsys_tfb.core.schema.OPTIONAL_ROLE_KEYS` alone.

    Keyed by role rather than flattened because three of the four callers say
    the role's name in their message, and a user who declared ``event`` must
    never read about ``occasion``. Undeclared roles are absent rather than
    mapped to ``[]``, so ``if not declared`` is the whole "nothing to check"
    test.

    Reads the resolved schema, not the raw config: that is what normalises a
    one-column ``event: impression_id`` into a list.
    """
    schema = get_schema(parameters)
    return {
        role: cols
        for role in OPTIONAL_ROLE_KEYS
        if (cols := schema.get(role, []))
    }


def optional_role_columns(parameters: dict) -> list[str]:
    """Every column an optional role declares, in identity order; ``[]`` if none.

    The flat view of :func:`optional_role_column_map`, for the one caller that
    writes the columns out rather than talking about them
    (``training.nodes.predict_and_write_test_predictions``).
    """
    return [c for cols in optional_role_column_map(parameters).values() for c in cols]


def optional_role_as_feature_errors(parameters: dict) -> list[str]:
    """(A38) an optional-role column must not be declared a categorical feature.

    Returns error strings (empty list when fine), collected by
    :func:`validate_config_consistency`.

    Identity columns have exactly one way of becoming model features — being
    listed in ``dataset.prepare_model_input.categorical_columns`` — and
    ``schema.item`` takes it, so much so that A2 *requires* it to. The optional
    roles do not get that exit. An impression id is a row label: a tree that
    splits on it memorises which impressions were clicked, and the split
    survives every check the framework runs because the column is genuinely in
    the training data. A second-resolution ``event`` timestamp is the worse
    case, because it is not noise — it moves with whatever the deployment's
    within-week dynamics are (in this repo's ad example, the generator's
    fatigue effect), so the model trains well, evaluates well offline, and has
    learnt something that does not exist at serving time.

    The legitimate want behind the mistake — "let the model see what time of
    day the impression was" — is a column computed in a feature table, not the
    identity column reused. The message says so, because a gate that only
    refuses sends the user looking for a way around it.

    Collect-all across roles and columns: a config that lists two of them
    should be fixed in one pass.
    """
    declared = _prepare_model_input(parameters).get("categorical_columns")
    if not declared:
        return []
    role_of = {
        col: role
        for role, cols in optional_role_column_map(parameters).items()
        for col in cols
    }
    offenders = [c for c in declared if c in role_of]
    if not offenders:
        return []
    return [
        f"(A38) {col!r} is declared by schema.columns.{role_of[col]} and also "
        f"listed in dataset.prepare_model_input.categorical_columns. A column "
        f"in that list becomes a model feature (that is how schema.item "
        f"becomes one), but a {role_of[col]!r} column identifies rows rather "
        f"than describing the candidate: a model that splits on it memorises "
        f"which rows were clicked, and one that is a timestamp additionally "
        f"correlates with within-period effects that do not exist at serving "
        f"time. Remove {col!r} from categorical_columns; to feed the model "
        f"something about when the row happened, compute that as its own "
        f"column in a feature table."
        for col in offenders
    ]


def optional_role_columns_declared_errors(
    parameters: dict,
    declared_columns: list[str] | None,
    target_name: str,
) -> list[str]:
    """(A39) the prediction write target must declare every optional-role column.

    Returns error strings (empty list when fine); the training command raises.
    A28's shape exactly — including ``None`` meaning ``columns: "auto"``, which
    declares nothing and so drops nothing — and A28's reason:
    ``HiveTableDataset.save`` ends with ``df.select(*declared)``, so a column
    the catalog entry never named is dropped there with no error and no log
    line.

    What makes it worth its own code rather than a widened A28: the failure
    downstream is different and reads as a data problem. Dropping an entity
    column publishes rows that identify the wrong thing; dropping an ``event``
    column publishes several rows per item that nothing can tell apart, so
    evaluation's identity duplicate check raises on a table that was correct
    when it was written, and the operator goes looking upstream at
    ``label_table``. Naming the role in the message is what shortens that.

    A deployment that declares no optional role gets an empty list without
    reading the catalog's declaration at all.
    """
    if declared_columns is None:
        return []

    errors: list[str] = []
    for role, role_cols in optional_role_column_map(parameters).items():
        missing = [c for c in role_cols if c not in declared_columns]
        if missing:
            errors.append(
                f"(A39) catalog entry {target_name!r} does not declare "
                f"{role} column(s) {missing}; schema.columns.{role} is "
                f"{role_cols}. A Hive save keeps only declared columns, so "
                f"those columns would be dropped from every written row "
                f"without an error — leaving several indistinguishable rows "
                f"per item in the published table. Add them to that entry's "
                f"`columns:`."
            )
    return errors


def zero_positive_group_weight_declared_errors(
    parameters: dict,
    declared_columns: list[str] | None,
    target_name: str,
) -> list[str]:
    """(A45) the prediction write target must declare the zero-positive group
    weight while test keeps any zero-positive group.

    Returns error strings (empty list when fine); the training command raises.
    A28/A39's shape and reason: ``HiveTableDataset.save`` keeps only declared
    columns, so an undeclared weight is dropped without an error, and
    evaluation then counts every row once — the kept zero-positive groups
    stand for ``1 / r`` groups each, so the binary metrics come out biased
    towards the positives with nothing in the run to say so. ``None`` means
    ``columns: "auto"``, which declares nothing and so drops nothing.

    Only the test ratio matters here: this table holds test predictions alone;
    the val weight is read by training in memory and never lands.
    """
    if declared_columns is None:
        return []
    if not test_carries_zero_positive_group_weight(parameters):
        return []
    if ZERO_POSITIVE_GROUP_WEIGHT_COL in declared_columns:
        return []
    ratio = resolved_zero_positive_group_ratio(parameters, "test")
    return [
        f"(A45) catalog entry {target_name!r} does not declare "
        f"{ZERO_POSITIVE_GROUP_WEIGHT_COL!r}, but "
        f"dataset.test_zero_positive_group_ratio={ratio!r} keeps some query "
        f"groups holding no positive and weights their rows by 1/r. A Hive "
        f"save keeps only declared columns, so the weight would be dropped "
        f"without an error and evaluation would count each kept group once "
        f"instead of 1/r times. Add "
        f"{{name: {ZERO_POSITIVE_GROUP_WEIGHT_COL}, type: DOUBLE}} to that "
        f"entry's `columns:`."
    ]


#: The source tables B11 requires an optional role's columns in, mapped to the
#: config key whose SQL produces each. ``feature_table`` is deliberately absent
#: — see B11 in the module docstring.
_OPTIONAL_ROLE_SOURCE_TABLES = ("sample_pool", "label_table")


def optional_role_source_column_errors(
    parameters: dict,
    columns_by_table: Mapping[str, Sequence[str]],
) -> list[str]:
    """(B11) every declared optional-role column exists in both source tables.

    Returns error strings (empty list when fine), collected by
    ``validate_data_consistency``.

    Pure: the caller reads ``DataFrame.columns`` (metastore metadata, no rows)
    and hands the lists in, the same arrangement A28/A39 use for the catalog.

    Declaring ``event`` widens ``identity_columns``, and that is what
    ``select_*_keys`` project ``sample_pool`` by and what ``build_model_input``
    LEFT joins ``label_table`` on. A table missing the column therefore already
    fails — as an ``AnalysisException`` about an unresolved name, raised by
    whichever node reached it first, naming neither the role that asked for the
    column nor the other table that may be missing it too. One message, both
    tables, before the label join.

    Collect-all across tables and columns: two tables built from the same
    upstream query are usually missing the same column, and reporting one per
    run would cost two passes to learn that.
    """
    role_columns = optional_role_column_map(parameters)
    if not role_columns:
        return []

    errors: list[str] = []
    for table in _OPTIONAL_ROLE_SOURCE_TABLES:
        present = set(columns_by_table.get(table, ()))
        for role, cols in role_columns.items():
            missing = [c for c in cols if c not in present]
            if not missing:
                continue
            errors.append(
                f"B11: {table} is missing column(s) {missing}, declared by "
                f"schema.columns.{role}. Declaring {role!r} makes those "
                f"columns part of a candidate row's identity, which is what "
                f"the keys are projected by and what the labels are joined "
                f"on — every row of both {' and '.join(_OPTIONAL_ROLE_SOURCE_TABLES)} "
                f"must carry them. Add them to that table's source SQL, or "
                f"remove the {role!r} declaration."
            )
    return errors


def candidate_feature_table_key_errors(
    identity_columns: Sequence[str],
    candidate_columns: Sequence[str],
) -> list[str]:
    """(B13) the candidate-level feature table carries every identity column.

    Returns error strings (empty list when fine), collected by
    ``validate_data_consistency``. Pure: the caller hands in
    ``DataFrame.columns`` (metastore metadata, no rows).

    One of its rows describes one candidate, so ``build_model_input`` joins it
    on identity (ADR-0026). Without this check a missing column still fails —
    as an unresolved-name ``AnalysisException`` from inside the join, naming
    neither the table nor why it needs the column. The shape it catches most
    is a table at (time, entity) grain declared as the candidate one: it holds
    the base key and nothing below it, and belongs in ``feature_table``.
    """
    present = set(candidate_columns)
    missing = [c for c in identity_columns if c not in present]
    if not missing:
        return []
    return [
        f"B13: candidate_feature_table is missing identity column(s) {missing}. "
        f"It is joined onto the candidate rows on identity "
        f"({list(identity_columns)}): one of its rows describes one candidate "
        f"(ADR-0026). Add the columns to that table's source SQL. If the table "
        f"describes an entity in a period rather than a candidate, it belongs "
        f"in feature_table, which joins on (time, entity)."
    ]


def item_source_column_errors(
    schema: dict,
    table: str,
    columns: Sequence[str],
) -> list[str]:
    """(B16) a table that carries items can have a multi-column item combined.

    Returns error strings (empty list when fine). Pure: the caller hands in
    ``DataFrame.columns`` (metadata, no rows). ``validate_data_consistency``
    collects it for the dataset's tables; ``utils.item_columns.
    combine_item_columns`` raises it at every entry, so a table read only by
    evaluation is covered too.

    Only a multi-column item is checked — a single column is neither combined
    nor dropped, so a column named ``item`` there is just the item column.
    Two ways combining goes wrong (ADR-0027 decision 4): a source column is
    missing, which would otherwise surface as an unresolved-name
    ``AnalysisException`` naming neither the table nor the declaration; or the
    table already has a column called ``item``, which the combined value would
    silently overwrite.
    """
    sources = schema["item_source_columns"]
    if len(sources) < 2:
        return []
    present = set(columns)
    errors: list[str] = []
    missing = [c for c in sources if c not in present]
    if missing:
        errors.append(
            f"B16: {table} is missing item column(s) {missing}. "
            f"schema.columns.item lists {list(sources)}, and every table that "
            f"carries items must have all of them — they are combined into "
            f"one column named {COMBINED_ITEM_COLUMN!r} when read (ADR-0027)."
        )
    if COMBINED_ITEM_COLUMN in present:
        errors.append(
            f"B16: {table} already has a column named "
            f"{COMBINED_ITEM_COLUMN!r}. schema.columns.item lists "
            f"{list(sources)}, which are combined into a column of that name "
            f"when read, so it would overwrite yours. Rename that column in "
            f"the table's source SQL."
        )
    return errors


def combined_item_collision_errors(
    source_columns: Sequence[str],
    combinations: Iterable[tuple[tuple, str | None]],
) -> list[str]:
    """(B15) no two different item combinations combine to the same value.

    ``combinations`` is ``(source values, combined value)`` pairs, as the
    dataset gate reads them in one distinct from ``sample_pool`` and
    ``label_table``. Repeats are fine (both tables hold the same item); a
    null combined value is skipped (a null part, which combines nothing).

    The separator is fixed at ``-`` and values holding ``-`` are allowed
    (ADR-0027 decision 2), so ``("a-b", "c")`` and ``("a", "b-c")`` both
    become ``a-b-c``. After that they are one item everywhere — one code for
    the model, one label join — and nothing downstream can tell them apart.
    Collect-all: every colliding value in one message.
    """
    by_value: dict[str, set[tuple]] = {}
    for parts, value in combinations:
        if value is None:
            continue
        by_value.setdefault(value, set()).add(tuple(parts))
    errors: list[str] = []
    for value in sorted(by_value):
        # Sorted by repr: the two tables may hold one column as different
        # types (an int 7 and a string "7" combine to the same text), and
        # comparing those raw would raise instead of reporting. B17 reports
        # the type mismatch itself.
        combos = sorted(by_value[value], key=repr)
        if len(combos) < 2:
            continue
        errors.append(
            f"B15: item combinations {', '.join(repr(c) for c in combos)} of "
            f"{list(source_columns)} all combine to {value!r}, so they would "
            f"be treated as one item. The item columns are joined with "
            f"{ITEM_SEPARATOR!r} (ADR-0027); change one of the values in your "
            f"source SQL so the combinations stay distinct."
        )
    return errors


def item_category_conflict_errors(
    category_column: str,
    rows: Iterable[tuple[Any, str | None, str | None]],
) -> list[str]:
    """(B18) each item has at most one non-NULL category over the evaluated
    months (#379).

    ``rows`` is ``(time value, item, category)``, the distinct triples
    ``prepare_eval_data`` reads off ``sample_pool``'s ``category_column`` for
    the months it evaluates. A NULL category is no second category (the
    NULL rule itself is ``prepare_eval_data``'s decision); a NULL item is
    skipped, since it joins nothing.

    Why a category may not change with time, not even from one month to the
    next: several evaluated dates are ranked together as one set of query
    groups, and the category pass joins the table on the item alone
    (``metrics_spark.collapse_to_categories``). An item with two rows in the
    table would have every one of its predictions copied into both
    categories and counted twice, with nothing raising. Keying the table by
    (time, item) instead would change the join keys of the aggregation — a
    different design, not a fix of this one. Collect-all: every such item,
    each category with the months it appears in.
    """
    months: dict[str, dict[str, set[str]]] = {}
    for time_value, item, category in rows:
        if item is None or category is None:
            continue
        months.setdefault(item, {}).setdefault(category, set()).add(
            str(time_value))
    errors: list[str] = []
    for item in sorted(months):
        by_category = months[item]
        if len(by_category) < 2:
            continue
        spelled = ", ".join(
            f"{category!r} ({', '.join(sorted(by_category[category]))})"
            for category in sorted(by_category)
        )
        errors.append(
            f"B18: item {item!r} has {len(by_category)} categories in "
            f"sample_pool column evaluation.item_categories.column="
            f"{category_column!r} over the evaluated months: {spelled}. A "
            f"category must not change with time — the category pass joins "
            f"on the item alone, so the item's rows would be counted in "
            f"each. Fix the column upstream, or pick a column that does not "
            f"change."
        )
    return errors


def item_list_drift_errors(
    item: str,
    counted: Sequence,
    existing: Sequence | None,
    base_dataset_version: str,
) -> list[str]:
    """(B19) a counted item list may not change under one
    ``base_dataset_version`` (#379).

    ``counted`` is the list this run counted from the train months'
    sample_pool, ``existing`` the one in the preprocessor already on disk for
    the version (``None`` when there is none). With a listed item list the
    list is in the version ID, so adding an item moves the version and a new
    directory is written; a counted list is not in the conf, so the version
    stays put when the train-month data change under it. Overwriting the
    preprocessor then would move every item's code while a model trained on
    the old one still decodes by the old positions — every score shifted, no
    error. Refused instead, naming what moved and how to go on.

    The way on the message recommends first moves the version, so the new
    list gets a new directory and every existing model keeps its own:
    inference and evaluation read the preprocessor from
    ``data/dataset/<base_dataset_version>/`` — the model directory holds no
    copy — so deleting that directory and rebuilding is exactly the silent
    re-coding this check exists to stop, for every model on the version that
    is not retrained, a promoted one included. It stays in the message as
    the last resort, with that consequence spelled out.
    """
    if existing is None or list(existing) == list(counted):
        return []
    added = sorted(set(counted) - set(existing), key=str)
    removed = sorted(set(existing) - set(counted), key=str)
    moved = "" if added or removed else " (same items, different order)"
    return [
        f"B19: base_dataset_version {base_dataset_version!r} already has a "
        f"preprocessor whose item list (counted from the train months' "
        f"sample_pool) differs from the one counted now{moved}: added "
        f"{added}, removed {removed}. The train-month data changed under the "
        f"same version; overwriting would shift every item's code under the "
        f"models trained on the old list. To build with the new data, make "
        f"the version move, so a new directory is built and existing models "
        f"keep theirs: change dataset.train_snap_dates, or write the item "
        f"list out under schema.categorical_values.{item} (the version then "
        f"holds the list) — every item sample_pool holds in the train, val "
        f"and test months (B1), with inference.products equal to it (A4). "
        f"Last resort: delete "
        f"data/dataset/{base_dataset_version}/ (and that version's dataset "
        f"partitions) and rebuild — every model on this version that is not "
        f"retrained, a promoted one included, then scores by shifted item "
        f"codes: silently wrong, no error."
    ]


def item_source_dtype_errors(
    schema: dict,
    dtypes_by_table: Mapping[str, Mapping[str, str]],
) -> list[str]:
    """(B17) each of a multi-column item's source columns has one type across
    the tables that carry it.

    Returns error strings (empty list when fine). Pure: the caller hands in
    ``dict(DataFrame.dtypes)`` per table (metastore metadata, no rows).

    Combining turns every part into text first (``utils.item_columns``), so a
    column that is an ``int`` in ``sample_pool`` and a ``double`` in the
    candidate-level feature table combines to ``7-banner`` in one and
    ``7.0-banner`` in the other. Before combining, Spark would have cast the
    two for the join and matched them; after it, the rows simply do not
    join — the candidate's features become NULL and nothing raises (the
    candidate table has no item check of its own). Only a multi-column item
    is checked: a single column is joined as itself.
    """
    sources = schema["item_source_columns"]
    if len(sources) < 2:
        return []
    errors: list[str] = []
    for col in sources:
        types = {t: d[col] for t, d in dtypes_by_table.items() if col in d}
        if len(set(types.values())) > 1:
            errors.append(
                f"B17: item column {col!r} has different types across tables "
                f"{types}. The item columns are turned into text before they "
                f"are combined (ADR-0027), so the same value written as two "
                f"types (7 and 7.0) becomes two different items and those "
                f"rows no longer join, silently. Cast it to one type in the "
                f"source SQL of every table."
            )
    return errors


def feature_table_overlap_errors(
    feature_table_columns: Sequence[str],
    candidate_columns: Sequence[str],
    drop_columns: Sequence[str],
    identity_columns: Sequence[str],
    label_column: str,
) -> list[str]:
    """(B14) no column is read from both feature tables.

    Returns error strings (empty list when fine), collected by
    ``validate_data_consistency``. Pure: column names only.

    ``build_model_input`` joins both tables onto the same row, and each
    contributes the features it holds; a name both hold would arrive twice and
    Spark fails with ``Reference 'x' is ambiguous``. Identity columns are not an
    overlap — they are the join keys — and neither is the label or a dropped
    column, because neither table selects those. What is left is a name one
    source SQL has to rename (or both drop).
    """
    shared = (
        set(feature_table_columns) & set(candidate_columns)
    ) - set(identity_columns) - set(drop_columns) - {label_column}
    if not shared:
        return []
    return [
        f"B14: column(s) {sorted(shared)} are in both feature_table and "
        f"candidate_feature_table. A feature is read from one table only: "
        f"build_model_input joins both onto the same candidate row, so the "
        f"column would arrive twice and the join fails as an ambiguous "
        f"reference. Rename it in one table's source SQL, or add it to "
        f"dataset.prepare_model_input.drop_columns if neither copy is a feature."
    ]


def optional_role_monitoring_errors(
    parameters: dict, post_training: bool
) -> list[str]:
    """(A40) monitoring-mode evaluation is refused when an optional role is
    declared.

    Returns error strings (empty list when fine); the evaluation command
    raises. Takes the flag rather than reading it, so the predicate stays pure
    the way A22's does.

    Monitoring evaluates what offline inference published, and inference
    ignores the optional roles by design: its candidates are the framework's
    own entity x item grid, where no impression exists to name. So its rows
    have no ``event`` column and ``label_table``'s rows do. What comes out of
    joining those two is not a smaller answer but a wrong one — the reason
    ADR-0021 decision 5 puts this at the CLI entry rather than letting the run
    finish and produce a report someone reads.

    The message names ``--post-training`` because that is the mode that works,
    not as advice to try a flag at random: it reads
    ``training_eval_predictions``, whose rows A39 makes carry the columns.
    """
    declared = optional_role_column_map(parameters)
    if post_training or not declared:
        return []
    named = "; ".join(
        f"schema.columns.{role}={cols}" for role, cols in declared.items()
    )
    return [
        f"(A40) evaluation's monitoring mode cannot be used while an optional "
        f"column role is declared ({named}). Monitoring evaluates "
        f"ranked_predictions, which offline inference writes from its own "
        f"entity x item candidate grid — those rows carry no such column, "
        f"while label_table's do, so the two sides identify rows differently "
        f"and the join produces a wrong answer rather than a missing one. "
        f"Run evaluation with --post-training, which reads "
        f"training_eval_predictions."
    ]


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
    categorical columns: the first three are stored raw, and a declared
    categorical is stored as a code the lookup decodes back at runtime
    (``io/extract.py::decode_weight_keys``). Anything else is physically absent
    from the parquet, so its weight silently no-ops at 1.0.
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


def weight_unknown_items(
    parameters: dict, items: Iterable[str] | None = None
) -> list[str]:
    """training.sample_weights keys whose product component ∉ the item list (A9c).

    Weight-table keys are '|'-joined sample_weight_keys values. If schema.item
    is not a weight key there is no product component → nothing to check
    (mirrors A5's item-only check in override_unknown_items). Only keys whose
    segment count matches the key arity are inspected; arity errors are
    reported separately by weight_key_arity_mismatch.

    ``items``: as in :func:`override_unknown_items`. With a counted list the
    check runs where training first holds the preprocessor
    (``select_features``, whose memory-only output every training slice
    re-runs it for).
    """
    training = parameters.get("training", {}) or {}
    keys = training.get("sample_weight_keys") or []
    item = get_schema(parameters)["item"]
    if item not in keys:
        return []
    idx = keys.index(item)
    weights = training.get("sample_weights") or {}
    if items is None:
        if item_list_counted_from_data(parameters):
            return []
        items = resolved_item_values(parameters)
    declared = set(items)
    bad: set[str] = set()
    for key in weights:
        parts = str(key).split("|")
        if len(parts) == len(keys) and parts[idx] not in declared:
            bad.add(parts[idx])
    return sorted(bad)


def weight_unknown_item_errors(
    parameters: dict, items: Iterable[str] | None = None
) -> list[str]:
    """A9c as the message both of its sites raise: the CLI entry
    (``items=None``) and training's ``select_features`` (the preprocessor's
    counted list). See :func:`override_unknown_item_errors`, its A5 twin.
    """
    unknown = weight_unknown_items(parameters, items=items)
    if not unknown:
        return []
    where, fix = _item_list_named(parameters)
    return [
        f"A9c: training.sample_weights references item value(s) {unknown} "
        f"absent from {where} — the weight silently never matches. {fix}"
    ]


_SEGMENT_OVERRIDE_FIELDS = ("table", "key_columns", "segment_column")


def segment_source_override_errors(parameters: dict) -> list[str]:
    """Malformed ``evaluation.segment_sources`` overrides (A10).

    Only a column in ``evaluation.segment_columns`` that has an override is
    checked. Without one, the column comes from the run mode's population
    table; this layer runs before Spark and does not know the mode, so it
    cannot tell whether that table has the column. A misspelt column name
    therefore passes here and shows up at run time as "population table has
    no column" in the log WARN and the report, both naming the column.

    An override is found by the column's name, so its key must be in
    ``segment_columns`` and its ``segment_column`` must be that name. A key
    outside the list is never joined, and the column it was meant for quietly
    comes from the population table instead (an old config keyed by an alias,
    valid when this check matched through ``segment_column``, lands here); a
    different ``segment_column`` would join a column nothing groups by.
    """
    ev = parameters.get("evaluation", {}) or {}
    overrides = ev.get("segment_sources", {}) or {}
    segment_columns = ev.get("segment_columns", []) or []
    errors = [
        f"evaluation.segment_sources.{key} is not in evaluation.segment_columns "
        f"{list(segment_columns)}. Overrides are looked up by the column they "
        f"deliver, so this one is never joined; rename the key to that column "
        f"(and list it in segment_columns) or remove the entry."
        for key in overrides if key not in segment_columns
    ]
    for col in segment_columns:
        if col not in overrides:
            continue
        cfg = overrides[col] or {}
        missing = [f for f in _SEGMENT_OVERRIDE_FIELDS if not cfg.get(f)]
        if missing:
            errors.append(
                f"evaluation.segment_sources.{col} is missing {missing}. An "
                f"override must give all of {list(_SEGMENT_OVERRIDE_FIELDS)}; "
                f"remove the entry to take {col!r} from the run mode's "
                f"population table instead."
            )
        elif cfg["segment_column"] != col:
            errors.append(
                f"evaluation.segment_sources.{col} has segment_column="
                f"{cfg['segment_column']!r}; it must be {col!r}, the column "
                f"this override delivers."
            )
    return errors


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


#: Values ``training.hpo_objective`` may take (A25) are the metric registry's
#: names (``evaluation/metric_registry.py``, ADR-0028 decision 2), imported
#: rather than copied so the entry gate and the scorer the value dispatches to
#: cannot disagree about what is admissible: adding an objective is one row.
#: Its binary-prediction family needs val to keep query groups holding no
#: positive (A48).

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
    # Imported here, not at the top: core/ has no import-time dependency on
    # the layers above it, and evaluation/metrics_spark imports this module
    # (the A15 precedent for diagnosis/).
    from recsys_tfb.evaluation.metric_registry import METRIC_NAMES

    errors: list[str] = []
    training = parameters.get("training", {}) or {}
    for key, admitted in (
        ("hpo_objective", METRIC_NAMES),
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
#: — the reason A25 keeps ``FINAL_MODEL_STRATEGIES`` in this module.
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


#: How much of the query groups holding no positive each split keeps, when its
#: key is absent (ADR-0025 decision 3). The defaults are what the pipeline did
#: before the keys existed: train kept every group, val and test kept none.
#: ``train`` also governs train_dev — the two are one split cut by entity, and
#: the ADR gives them one key.
ZERO_POSITIVE_GROUP_RATIO_DEFAULTS: dict[str, float] = {
    "train": 1.0,
    "val": 0.0,
    "test": 0.0,
}

#: The column a val / test table carries when its ratio is above 0: 1 on every
#: row of a group holding a positive (always kept), ``1 / r`` on every row of a
#: kept zero-positive group — the inverse of the probability that the row's
#: group survived the draw. A design weight, not an unbiased estimator (ADR-0025
#: decision 3). Deliberately a name of its own, not shared with a per-row
#: training weight a user supplies (#425): the two answer different questions
#: and must never overwrite each other.
ZERO_POSITIVE_GROUP_WEIGHT_COL = "zero_positive_group_weight"


def _zero_positive_group_ratio_key(split: str) -> str:
    if split not in ZERO_POSITIVE_GROUP_RATIO_DEFAULTS:
        raise ValueError(
            f"No zero-positive group ratio for split {split!r}; the keys cover "
            f"{sorted(ZERO_POSITIVE_GROUP_RATIO_DEFAULTS)}. train_dev reads "
            f"the train key (ADR-0025 decision 3)."
        )
    return f"{split}_zero_positive_group_ratio"


def resolved_zero_positive_group_ratio(parameters: dict, split: str) -> float:
    """``dataset.<split>_zero_positive_group_ratio`` with its default applied.

    One resolver so "what an absent key means" has a single definition, read by
    the dataset nodes that draw, the training write that carries the weight,
    and the evaluation gate that needs to know whether test kept any
    zero-positive group. Callers may assume the value is legal: A44 rejects
    anything else at CLI entry.
    """
    key = _zero_positive_group_ratio_key(split)
    ds = parameters.get("dataset") or {}
    if key not in ds:
        return ZERO_POSITIVE_GROUP_RATIO_DEFAULTS[split]
    return float(ds[key])


def test_carries_zero_positive_group_weight(parameters: dict) -> bool:
    """Whether the test table carries :data:`ZERO_POSITIVE_GROUP_WEIGHT_COL` —
    ``dataset.test_zero_positive_group_ratio`` above 0.

    The one derivation for every reader of that fact: the training write that
    carries the column into the prediction table, A45 that makes the catalog
    declare it, the evaluation node that weights by it, the report note that
    prints it, and A46. Each deciding it for itself is how a write and the
    gate that checks it end up disagreeing.
    """
    return resolved_zero_positive_group_ratio(parameters, "test") > 0.0


def _is_legal_zero_positive_group_ratio(value) -> bool:
    """A44's domain, shared with A48 so the two cannot disagree on what A44
    reports and A48 therefore skips."""
    is_number = isinstance(value, (int, float)) and not isinstance(value, bool)
    return is_number and 0.0 <= value <= 1.0


def zero_positive_group_ratio_errors(parameters: dict) -> list[str]:
    """A44 — each ``dataset.*_zero_positive_group_ratio`` is a number in [0, 1].

    Absent is clean: every config written before the keys existed is in that
    state, and the resolver supplies today's behaviour. An explicit YAML
    ``null`` is rejected rather than read as absent, for A31's reason — it is
    present in the version payload and would move a version ID while changing
    nothing. ``True`` is rejected too although Python calls it an int: a
    ratio written as a boolean is a config mistake, not a request for 1.0.

    Aggregated by ``validate_config_consistency``, for A29/A31's reason: the
    train key feeds ``train_variant_id`` and the val / test keys feed
    ``base_dataset_version``, so a bad value moves the artifact paths every
    later command resolves.
    """
    errors: list[str] = []
    ds = parameters.get("dataset") or {}
    for split in ZERO_POSITIVE_GROUP_RATIO_DEFAULTS:
        key = _zero_positive_group_ratio_key(split)
        if key not in ds:
            continue
        value = ds[key]
        if _is_legal_zero_positive_group_ratio(value):
            continue
        errors.append(
            f"A44: dataset.{key}={value!r} is not a ratio. It is the share of "
            f"the {split} query groups holding no positive that are kept, so it "
            f"must be a number in [0, 1]; delete the line to take the default "
            f"{ZERO_POSITIVE_GROUP_RATIO_DEFAULTS[split]!r}."
        )
    return errors


def hpo_objective_population_errors(parameters: dict) -> list[str]:
    """A48 — a binary-prediction HPO objective needs val to keep query groups
    holding no positive (#430).

    ``training.hpo_objective`` in the registry's binary-prediction family
    (``metric_registry.BINARY_PREDICTION_METRICS``) requires
    ``dataset.val_zero_positive_group_ratio`` above 0. At 0 — the default —
    the dataset pipeline drops every val query group without a positive, and
    average precision is then computed on a population filtered by the label
    it scores: a number, just not the one the objective names.
    The ranking objectives are unaffected (such a group adds nothing to a
    ranking score), so the default every existing deployment runs stays clean.

    A ratio A44 rejects is left to A44: one message per mistake, and resolving
    a YAML ``null`` here would raise ``TypeError`` instead of joining the
    collected errors.

    Aggregated by ``validate_config_consistency`` rather than hung off the
    training command: the ratio takes effect in the dataset pipeline, so
    stopping only at training would come after val was already built without
    the groups — and fixing the ratio moves ``base_dataset_version``, so that
    build is rerun either way.
    """
    # Imported here, not at the top: core/ has no import-time dependency on
    # the layers above it, and evaluation/metrics_spark imports this module
    # (the A15 precedent for diagnosis/).
    from recsys_tfb.evaluation.metric_registry import BINARY_PREDICTION_METRICS

    training = parameters.get("training") or {}
    objective = training.get("hpo_objective")
    if objective not in BINARY_PREDICTION_METRICS:
        return []
    ds = parameters.get("dataset") or {}
    key = _zero_positive_group_ratio_key("val")
    if key in ds and not _is_legal_zero_positive_group_ratio(ds[key]):
        return []
    if resolved_zero_positive_group_ratio(parameters, "val") > 0.0:
        return []
    return [
        f"A48: training.hpo_objective={objective!r} scores every val row as a "
        f"binary prediction, but dataset.{key} is "
        f"{ds.get(key, ZERO_POSITIVE_GROUP_RATIO_DEFAULTS['val'])!r}: the "
        f"dataset pipeline drops every val query group holding no positive, so "
        f"average precision would be computed on that filtered population. "
        f"Set dataset.{key} above 0 (it busts base_dataset_version and "
        f"rebuilds val), or choose mean_ap / macro_per_item_map."
    ]


class BinaryTestMetricsVerdict(NamedTuple):
    """What :func:`binary_test_metrics_verdict` decides: ``errors`` stop the
    command (A54); ``withheld`` maps a metric scored with no value on test to
    the reason."""

    errors: list
    withheld: dict


def binary_test_metrics_verdict(
    parameters: Mapping,
    dataset_version: str | None = None,
    dataset_test_ratio: float | None = None,
) -> BinaryTestMetricsVerdict:
    """A54 — a binary-prediction metric scored on test needs test to keep the
    query groups holding no positive, weighted as val's are (ADR-0028
    decision 4).

    Those metrics score every test row as one yes/no prediction, and HPO chose
    by the same number on val, where A48 makes the population keep those
    groups. Test keeps them only with ``dataset.test_zero_positive_group_ratio``
    above 0 twice over: in the config, which decides whether the predictions
    are written with ``ZERO_POSITIVE_GROUP_WEIGHT_COL``; and in the dataset
    version the run reads, which decides whether the rows are there. Without
    them the number is a different population's, not comparable with val's.

    What is asked for decides what happens, the same at both places this runs:

    * the selection metric (``test_metrics.selection_metric``, or the
      effective HPO objective it follows when unset) or a name in
      ``test_metrics.metrics`` → an error naming both ways out. Promote ranks
      by the first and the second was asked for by name; a value that never
      comes would let promote fail quietly later.
    * only the HPO objective (the selection metric written as a ranking
      metric) → withheld with the reason: it joins the scored set on its own,
      and is not worth a test rebuild the user never asked for.

    ``dataset_version`` / ``dataset_test_ratio`` are the dataset version a
    training run reads and the test ratio its manifest records; left out, the
    version is the one the config builds. Where this runs: aggregated by
    ``validate_config_consistency`` (A48's reason — the ratio takes effect in
    the dataset pipeline, so the dataset command stops too); at the training
    entry once the dataset version is resolved, before the pipeline runs
    (the config raised, the data built before it); and in
    ``compute_test_metrics`` for the withheld reasons and as the runtime
    backstop. A ratio A44 rejects is left to A44; a malformed
    ``test_metrics`` block or unknown names, to A53 / A25.
    """
    # Imported here, not at the top: core/ has no import-time dependency on
    # the layers above it, and evaluation/metrics_spark imports this module
    # (the A15 precedent for diagnosis/).
    from recsys_tfb.evaluation.metric_registry import (
        BINARY_PREDICTION_METRICS,
        effective_hpo_objective,
        selection_metric,
    )

    clean = BinaryTestMetricsVerdict([], {})
    block = parameters.get("test_metrics")
    if block is not None and not isinstance(block, Mapping):
        return clean
    ds = parameters.get("dataset") or {}
    key = _zero_positive_group_ratio_key("test")
    if key in ds and not _is_legal_zero_positive_group_ratio(ds[key]):
        return clean
    configured = resolved_zero_positive_group_ratio(parameters, "test")
    if dataset_test_ratio is None:
        dataset_test_ratio = configured
    if configured > 0.0 and dataset_test_ratio > 0.0:
        return clean

    selected = selection_metric(parameters)
    written = (block or {}).get("selection_metric") is not None
    listed = (block or {}).get("metrics")
    if not isinstance(listed, list) or not all(isinstance(m, str) for m in listed):
        listed = []
    blocked_by = []
    if selected in BINARY_PREDICTION_METRICS:
        blocked_by.append(
            f"test_metrics.selection_metric={selected!r}" if written else
            f"the selection metric {selected!r} (test_metrics.selection_metric "
            f"is unset, so it follows training.hpo_objective)"
        )
    asked = [m for m in listed if m in BINARY_PREDICTION_METRICS]
    if asked:
        blocked_by.append(f"test_metrics.metrics lists {asked}")

    # Why test cannot score them, and how to make it: three situations, each
    # told as it is — the config's ratio decides whether the predictions carry
    # the weight column, the dataset version's whether the rows are there.
    written_ratio = ds.get(key, ZERO_POSITIVE_GROUP_RATIO_DEFAULTS["test"])
    if configured > 0.0:
        cause = (
            f"dataset version {dataset_version!r}, which this training run "
            f"reads, was built with dataset.{key} 0 (as its manifest records "
            f"it; a manifest or a key that is missing reads as 0) while the "
            f"config says {configured!r}: its test kept no query group holding "
            f"no positive"
        )
        rebuild = (
            f"rerun the dataset command so a version built with dataset.{key} "
            f"{configured!r} exists (training reads data/dataset/latest), or "
            f"pass one as --base-dataset-version"
        )
    elif dataset_test_ratio > 0.0:
        cause = (
            f"dataset.{key} is {written_ratio!r} in the config, so the test "
            f"predictions are written without {ZERO_POSITIVE_GROUP_WEIGHT_COL}, "
            f"whatever dataset version {dataset_version!r} kept"
        )
        rebuild = (
            f"set dataset.{key} back to {dataset_test_ratio!r}, the ratio that "
            f"version was built with"
        )
    else:
        cause = (
            f"dataset.{key} is {written_ratio!r}: the dataset pipeline drops "
            f"every test query group holding no positive"
        )
        rebuild = (
            f"set dataset.{key} above 0 (it busts base_dataset_version: rerun "
            f"dataset, then training)"
        )

    errors = []
    if blocked_by:
        # Either the data changes, or everything that asked for a binary
        # metric stops asking — all of it, so the second way is one remedy.
        stop_asking = []
        if selected in BINARY_PREDICTION_METRICS:
            stop_asking.append(
                "set test_metrics.selection_metric to a ranking metric "
                "(mean_ap / macro_per_item_map)")
        if asked:
            stop_asking.append(
                f"take {'it' if len(asked) == 1 else 'them'} out of "
                f"test_metrics.metrics")
        errors.append(
            f"A54: {' and '.join(blocked_by)} asks test for a binary-prediction "
            f"metric, which scores every test row weighted as val's are, but "
            f"{cause} — so the number would be another population's, not "
            f"comparable with the one HPO chose by on val. Either {rebuild}; "
            f"or {' and '.join(stop_asking)}."
        )

    withheld = {}
    objective = effective_hpo_objective(parameters)
    if objective in BINARY_PREDICTION_METRICS and objective != selected \
            and objective not in asked:
        withheld[objective] = (
            f"not scored on test: {cause}, so the value would not be "
            f"comparable with val's. It is here only as the HPO objective; the "
            f"selection metric is {selected!r}. To score it, {rebuild}."
        )
    return BinaryTestMetricsVerdict(errors, withheld)


#: ``evaluation.query_filter``'s keys and their off-state (#376). The only key
#: today is the switch A49 checks; #396's random-traffic switch is expected to
#: join this subtree later, at which point it gets its own entry here.
QUERY_FILTER_DEFAULTS: dict[str, bool] = {"drop_all_positive_groups": False}


def query_filter_param_errors(parameters: dict) -> list[str]:
    """A49 — ``evaluation.query_filter`` parameter domain (#376).

    * ``drop_all_positive_groups`` is a ``bool``. It is the only reader's
      (``evaluation/metrics.py::drop_all_positive_groups``) sole input; a
      non-bool there (``"yes"``, ``1``) would be read with plain truthiness
      (``bool(qf.get(...) or False)``) instead of raising, so a typo'd value
      would silently turn the switch on or stay off with nobody told.
    * No other key, A42's shape: a subtree that silently ignores anything it
      does not name is worse than one that has no keys yet, because a typo
      inside it (e.g. ``drop_all_positiv_groups``) looks like it worked.
    * Absent block, or an explicit ``null`` (the YAML a fully-commented-out
      ``query_filter:`` parent line reads as), both mean off — the same
      reading ``drop_all_positive_groups()`` gives them.

    Not aggregated by ``validate_config_consistency``: only evaluation reads
    this key (A34's reason, issue #158). The evaluation command raises it,
    collected with A22/A34/A40/A42/A43/A46.
    """
    eval_params = parameters.get("evaluation", {}) or {}
    if not isinstance(eval_params, Mapping):
        return []
    block = eval_params.get("query_filter")
    if block is None:
        return []
    if not isinstance(block, Mapping):
        return [
            f"A49: evaluation.query_filter={block!r} must be a mapping with "
            f"the keys {sorted(QUERY_FILTER_DEFAULTS)} (or left absent/null)."
        ]
    errors = []
    unknown = sorted(set(block) - set(QUERY_FILTER_DEFAULTS), key=str)
    if unknown:
        errors.append(
            f"A49: evaluation.query_filter declares {unknown}, which nothing "
            f"reads; its keys are {sorted(QUERY_FILTER_DEFAULTS)}."
        )
    if "drop_all_positive_groups" in block:
        value = block["drop_all_positive_groups"]
        if not isinstance(value, bool):
            errors.append(
                f"A49: evaluation.query_filter.drop_all_positive_groups="
                f"{value!r} must be a bool."
            )
    return errors


#: ``evaluation.baseline.score``'s domain (A50). ``count`` ranks by positives
#: in the lookback window, ``rate`` by positives ÷ times a candidate (#397).
BASELINE_SCORES: tuple[str, ...] = ("count", "rate")

#: The keys ``evaluation.baseline`` may declare (A50): what
#: ``evaluation/baselines.py`` reads (``resolve_lookback_months``,
#: ``baseline_score``).
BASELINE_KEYS: tuple[str, ...] = ("lookback_months", "score")


def baseline_score_errors(parameters: dict) -> list[str]:
    """A50 — ``evaluation.baseline.score`` is one of :data:`BASELINE_SCORES`,
    and ``evaluation.baseline`` declares no key outside :data:`BASELINE_KEYS`.

    Absent or ``null`` means ``count``, the reading ``baseline_score()``
    gives. The unknown-key half is A49's shape: a typo'd key looks like it
    worked. Not aggregated by ``validate_config_consistency``: only evaluation
    reads this key (A34's reason, issue #158). The evaluation command raises
    it, collected with A22/A34/A40/A42/A43/A46/A49.
    """
    eval_params = parameters.get("evaluation", {}) or {}
    if not isinstance(eval_params, Mapping):
        return []
    block = eval_params.get("baseline") or {}
    if not isinstance(block, Mapping):
        return []
    errors = []
    unknown = sorted(set(block) - set(BASELINE_KEYS), key=str)
    if unknown:
        errors.append(
            f"A50: evaluation.baseline declares {unknown}, which nothing "
            f"reads; its keys are {list(BASELINE_KEYS)}."
        )
    value = block.get("score")
    if value is not None and value not in BASELINE_SCORES:
        errors.append(
            f"A50: evaluation.baseline.score={value!r} must be one of "
            f"{list(BASELINE_SCORES)} (or left absent, which means 'count')."
        )
    return errors


def _item_categories_block(parameters: dict) -> Mapping:
    """``evaluation.item_categories`` as a mapping; ``{}`` when absent, null
    or not a mapping (its readers treat all three as "no categories")."""
    eval_params = parameters.get("evaluation", {}) or {}
    if not isinstance(eval_params, Mapping):
        return {}
    block = eval_params.get("item_categories") or {}
    return block if isinstance(block, Mapping) else {}


def item_category_column(parameters: dict) -> str | None:
    """The sample_pool column each item's category is read from (#379), or
    ``None`` when categories come from ``evaluation.item_categories.mapping``
    or are switched off.

    Column mode needs ``enabled`` too: ``enabled: false`` switches every
    category pass off, and a column left under it is inert. The one reading of
    "is this column mode" shared by A51 (below), ``prepare_eval_data`` (which
    builds the table), its readers, the ``--compare-only`` input check and
    ``metrics_spark.hand_category_mapping``. A value A51 refuses (not a
    non-empty string) comes back as is; A51 stops the run before anything
    reads it.
    """
    block = _item_categories_block(parameters)
    if not block.get("enabled") or "column" not in block:
        return None
    return block["column"]


def item_categories_enabled(parameters: dict) -> bool:
    """Whether ``evaluation.item_categories.enabled`` switches the category
    pass on."""
    return bool(_item_categories_block(parameters).get("enabled"))


def category_table_needed(parameters: dict) -> bool:
    """Whether the category pass reads ``evaluation_item_categories`` rather
    than the conf alone: in column mode it holds the table itself, and with
    the item list counted from the data (#379) the evaluated model's item list
    a hand mapping is checked against (``known_items``). What ``--compare-only``
    asks the file for; any other deployment does not need it, and an
    evaluation directory written before #379 does not have it.
    """
    return item_category_column(parameters) is not None or (
        item_categories_enabled(parameters)
        and item_list_counted_from_data(parameters)
    )


def item_category_column_errors(parameters: dict, post_training: bool) -> list[str]:
    """A51 — ``evaluation.item_categories.column`` (#379).

    * ``column`` and ``mapping`` are two sources for one table; with both
      declared nothing says which wins, so neither is picked. A ``mapping:
      null`` is no mapping — the reader takes it as empty, and it is how an
      env overlay switches off a mapping the base conf declares (a deep merge
      cannot delete a key).
    * ``column`` is a non-empty string (a column name). An empty ``column:``
      would otherwise read as "no column" and fall back to every item its own
      category, a report that looks fine.
    * In column mode (:func:`item_category_column`) ``unmapped``, when
      written, is ``singleton``: the rule for an item whose column is NULL in
      every row. The hand-mapping reader refuses anything else at run time,
      but column mode never reaches that check, so ``unmapped: drop`` would be
      silently read as singleton.
    * Column mode needs ``--post-training``. The column is read off
      ``sample_pool``, the table the post-training test set is drawn from;
      monitoring evaluates ``inference_population``, which has no candidate
      column to read. Only ``--compare-only`` without ``--post-training`` is
      monitoring too, for A22/A40's reason: no carve-out.

    Not aggregated by ``validate_config_consistency``: it needs
    ``--post-training``, and only evaluation reads the key (A34's reason,
    issue #158). The evaluation command raises it, collected with
    A22/A34/A40/A42/A43/A46/A49/A50.
    """
    block = _item_categories_block(parameters)
    if "column" not in block:
        return []
    column = block["column"]
    errors = []
    if block.get("mapping") is not None:
        errors.append(
            "A51: evaluation.item_categories declares both column and "
            "mapping. Keep one: column reads each item's category off that "
            "sample_pool column, mapping lists them by hand."
        )
    if not (isinstance(column, str) and column):
        errors.append(
            f"A51: evaluation.item_categories.column={column!r} must be a "
            f"sample_pool column name (a non-empty string)."
        )
    elif item_category_column(parameters) is not None:
        if "unmapped" in block and block["unmapped"] != "singleton":
            errors.append(
                f"A51: evaluation.item_categories.unmapped="
                f"{block['unmapped']!r} with column: only 'singleton' is "
                f"implemented (an item whose column is NULL in every row is "
                f"its own category)."
            )
        if not post_training:
            errors.append(
                f"A51: evaluation.item_categories.column={column!r} needs "
                f"--post-training: categories are read off sample_pool, and "
                f"monitoring evaluates inference_population, which has no "
                f"candidate column to read them from. --compare-only without "
                f"--post-training is monitoring too. Use item_categories.mapping "
                f"for monitoring, or add --post-training."
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
    ``select_sample_keys``'s comment ("PK enforced upstream by source_etl's
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


#: A33 (migration-period). ``old key -> new key`` under ``evaluation``.
#: Delete this constant with the predicate below; see A33 in the module
#: docstring for the retirement condition.
LEGACY_EVALUATION_KEYS = {"product_categories": "item_categories"}


def legacy_evaluation_key_errors(parameters: dict) -> list[str]:
    """A33 — a conf still spelling a pre-#327 ``evaluation`` key.

    ⚠ **This is a migration tool with a delete-by condition, not an
    invariant.** It exists so the one config rename in #327 costs an operator a
    startup rather than a silently missing report section, and it comes out
    again once the company environment's ``conf/`` is confirmed updated. The
    condition is written out in A33 of the module docstring; do not promote this
    to a permanent compatibility layer, and do not accept both spellings.

    The failure it replaces is silent all the way down.
    ``metrics_spark.hand_category_mapping`` reads
    ``evaluation.item_categories``; against an old conf that lookup returns
    ``{}``, ``enabled`` is falsy, the function returns ``None``, and
    ``compute_all_metrics`` simply never adds the ``category`` bundle. The run
    succeeds, the report renders, and the category tables are gone — the shape
    an operator cannot tell from "this month had no categories to show".

    Only the key's *presence* is checked, whatever it holds: a conf that spells
    the old name has not been migrated regardless of what is under it. The
    values themselves (``mapping``'s category names and item lists) are the
    user's own business vocabulary and are deliberately untouched by #327.
    """
    eval_params = parameters.get("evaluation", {}) or {}
    if not isinstance(eval_params, Mapping):
        return []
    return [
        f"A33: evaluation.{old!r} was renamed to evaluation.{new!r} (#327) — "
        f"the framework no longer spells its own config keys in the example "
        f"deployment's business vocabulary. There is no dual-key fallback: "
        f"left as-is, the category evaluation is skipped and every category "
        f"table disappears from the report without an error. Rename the key in "
        f"your conf; the values under it are yours and do not change."
        for old, new in LEGACY_EVALUATION_KEYS.items()
        if old in eval_params
    ]


#: Config keys that named the calibration mechanism removed in #411, written as
#: dotted paths from the top of ``parameters``.
#:
#: **This is not A33's migration tool with a delete-by date.** The keys do not
#: point at a renamed setting, they point at a mechanism that no longer exists
#: anywhere in the framework, so the check stays for as long as anyone might
#: still be carrying a pre-#411 conf.
#:
#: Complete as of #414: T1 (#413) retired the two keys the calibrator itself
#: read, and T2 (#414) added the four that configured the calibration data
#: split. Listed in the order a conf tree spells them — dataset, training,
#: inference — so the message reads in the order the operator will edit.
RETIRED_CALIBRATION_KEYS: tuple[str, ...] = (
    "dataset.enable_calibration",
    "dataset.calibration_snap_dates",
    "dataset.calibration_sample_ratio",
    "dataset.calibration_sample_ratio_overrides",
    "training.calibration",
    "inference.use_calibration",
)


def _dotted_key_present(parameters: Mapping, dotted: str) -> bool:
    """Is ``dotted`` (e.g. ``"training.calibration"``) spelled in ``parameters``?

    Presence of the *key*, never the truth of its value: ``None`` and ``False``
    are spelled, and that is what A37 is asking. Any non-mapping on the way down
    means the path is not spelled at all — a conf that writes ``training: null``
    has no ``training.calibration`` to delete.
    """
    node = parameters
    *parents, leaf = dotted.split(".")
    for part in parents:
        if not isinstance(node, Mapping):
            return False
        node = node.get(part)
    return isinstance(node, Mapping) and leaf in node


def retired_calibration_key_errors(parameters: dict) -> list[str]:
    """A37 — a conf still spelling a calibration key after #411 removed it.

    The failure it replaces is silent. Nothing reads these keys any more, so an
    operator who left ``training.calibration.enabled: true`` in place gets an
    uncalibrated model, a successful run, and no signal at all that the setting
    they wrote was ignored.

    **Presence is the failure, whatever the value** — including ``false``, an
    empty block and an empty list. The whole ``training:`` subtree is hashed
    into ``model_version`` and the whole ``dataset:`` subtree into
    ``base_dataset_version``, so a conf that keeps ``enable_calibration: false``
    or ``calibration_snap_dates: []`` computes different version IDs than one
    that deleted them. Requiring deletion is what makes two upgraded conf trees
    agree.

    One message listing every key, not one per key: the fix is a single edit,
    and a per-key message would make an operator re-run to discover the next
    one.
    """
    if not isinstance(parameters, Mapping):
        return []
    present = [
        dotted for dotted in RETIRED_CALIBRATION_KEYS
        if _dotted_key_present(parameters, dotted)
    ]
    if not present:
        return []
    return [
        "A37: calibration was removed from the framework (#411), and the "
        "config key(s) " + ", ".join(repr(k) for k in present) + " are read by "
        "nothing. Delete them; there is no replacement setting. The value does "
        "not matter — `false` and an empty block are rejected too, because "
        "these keys sit inside the subtrees hashed into base_dataset_version / "
        "model_version, so leaving one behind computes a different version ID "
        "than deleting it."
    ]


#: A43: the ``evaluation.report.diagnostics`` keys that configured the
#: ``[0, 1]`` calibration bins #381 removed.
RETIRED_CALIBRATION_BIN_KEYS: tuple[str, ...] = (
    "evaluation.report.diagnostics.include_calibration",
    "evaluation.report.diagnostics.n_calibration_bins",
)


def retired_calibration_bin_key_errors(parameters: dict) -> list[str]:
    """A43 — refuse the calibration-bin keys #381 retired, whatever their value.

    Same reasoning as A37 (``retired_calibration_key_errors``), one pipeline
    down: nothing reads them, so left in place they switch nothing and say
    nothing; and they sit in a fingerprinted subtree, so keeping them is not
    the same conf as deleting them. Both are named in one message.
    """
    present = [
        dotted for dotted in RETIRED_CALIBRATION_BIN_KEYS
        if _dotted_key_present(parameters, dotted)
    ]
    if not present:
        return []
    return [
        "A43: the score-bin table these key(s) configured was removed in #381 "
        "and nothing reads " + ", ".join(repr(k) for k in present) + ". "
        "Delete them. The one score-bin table left is the prediction-quality "
        "family's (evaluation.prediction_quality, switched on by "
        "evaluation.report.sections.prediction_quality); its bins span this "
        "run's min..max score rather than [0, 1]."
    ]


#: The ``evaluation.report.sections`` switches the report reads: one name per
#: ``_section_on(parameters, name)`` call in ``evaluation/report_builder.py``,
#: which refuses any name not listed here (A34). ``baseline``,
#: ``diagnostics`` and ``prediction_quality`` are also read directly by the
#: nodes that skip computing (``compute_baseline_metrics`` /
#: ``compute_report_aggregates`` / ``compute_prediction_quality``); those reads
#: bypass the ``_section_on`` pre-check, so a new direct read of a name that is
#: not listed here is caught by nothing. ``report_builder`` imports this
#: constant, not the other way round: ``core/`` has no import-time dependency on
#: the layers above it, and ``report_builder`` drags pandas and plotly in.
EVALUATION_REPORT_SECTIONS: frozenset[str] = frozenset({
    "dataset_overview",
    "primary_map",
    "diagnostics",
    "baseline",
    "diagnosis_links",
    "prediction_quality",
})

#: ``evaluation.prediction_quality``'s keys and the value each takes when the
#: conf leaves it out: fine bins over this data's score range (the threshold
#: resolution), display bins they merge into (the report's bin table), and how
#: many items get their own bins (the per-item budget, ADR-0024 decision 6).
#: Here, not in ``evaluation/prediction_quality.py``, so A42 can check a
#: partly declared block against the same defaults the node applies without
#: ``core/`` importing the evaluation layer (that module imports pyspark).
PREDICTION_QUALITY_DEFAULTS: dict[str, int] = {
    "n_bins": 1000,
    "n_display_bins": 10,
    "top_n": 30,
}


def prediction_quality_param_errors(parameters: dict) -> list[str]:
    """A42 — ``evaluation.prediction_quality`` parameter domains.

    * ``n_bins`` and ``n_display_bins`` are ints >= 1, and ``n_display_bins``
      divides ``n_bins``: each display bin merges the same number of fine
      bins. Otherwise the last display bin is narrower than the rest, and the
      bin table puts two widths side by side with nothing saying so.
    * ``top_n`` is an int >= 0 (0 lists no item, the overall numbers alone).
    * No other key. The family's on/off switch is
      ``evaluation.report.sections.prediction_quality`` (the ``baseline``
      family's shape, ADR-0024 decision 1); an ``enabled: true`` written here
      would be read by nothing and switch nothing on.

    ``bool`` is not an int here: YAML ``true`` would pass ``isinstance(v,
    int)`` and bin by 1.

    Not aggregated by ``validate_config_consistency``: only evaluation reads
    these keys (A34's reason, issue #158). The evaluation command raises it,
    collected with A22/A34.
    """
    eval_params = parameters.get("evaluation", {}) or {}
    if not isinstance(eval_params, Mapping):
        return []
    block = eval_params.get("prediction_quality") or {}
    if not isinstance(block, Mapping):
        return [
            f"A42: evaluation.prediction_quality={block!r} must be a mapping "
            f"with the keys {sorted(PREDICTION_QUALITY_DEFAULTS)}."
        ]
    errors = []
    unknown = sorted(set(block) - set(PREDICTION_QUALITY_DEFAULTS), key=str)
    if unknown:
        errors.append(
            f"A42: evaluation.prediction_quality declares {unknown}, which "
            f"nothing reads; its keys are "
            f"{sorted(PREDICTION_QUALITY_DEFAULTS)}. The switch that turns "
            f"the family on is evaluation.report.sections.prediction_quality."
        )
    values = {**PREDICTION_QUALITY_DEFAULTS, **block}
    floors = {"n_bins": 1, "n_display_bins": 1, "top_n": 0}
    well_formed = True
    for key, floor in floors.items():
        value = values[key]
        if not (isinstance(value, int) and not isinstance(value, bool)
                and value >= floor):
            well_formed = False
            errors.append(
                f"A42: evaluation.prediction_quality.{key}={value!r} must be "
                f"an int >= {floor}."
            )
    if well_formed and values["n_bins"] % values["n_display_bins"]:
        errors.append(
            f"A42: evaluation.prediction_quality.n_display_bins="
            f"{values['n_display_bins']} must divide n_bins={values['n_bins']}:"
            f" each display bin merges the same number of fine bins."
        )
    return errors


def prediction_quality_on(parameters: dict) -> bool:
    """Whether ``evaluation.report.sections.prediction_quality`` switches the
    family on — **off** when the switch is absent, unlike the report's other
    sections (ADR-0024 decision 1).

    One reading for the node that computes the family and the A46 gate that
    refuses it: two ``dict.get`` calls with two defaults is how a gate ends up
    guarding a family the node never runs, or the reverse.
    """
    eval_params = parameters.get("evaluation") or {}
    report = eval_params.get("report") or {} if isinstance(eval_params, Mapping) else {}
    sections = report.get("sections") or {} if isinstance(report, Mapping) else {}
    return bool(sections.get("prediction_quality", False)) if isinstance(
        sections, Mapping) else False


def prediction_quality_population_errors(
    parameters: dict, post_training: bool
) -> list[str]:
    """A46 — the prediction-quality family under ``--post-training`` needs a
    test table that kept some query groups holding no positive.

    Returns error strings (empty list when fine); the evaluation command raises
    it, collected with A22/A34/A42/A43. Takes the flag rather than reading it,
    like A22/A40.

    ``--post-training`` evaluates ``training_eval_predictions``, which is the
    test table scored — and with ``dataset.test_zero_positive_group_ratio`` at
    its default 0 the dataset pipeline dropped every zero-positive group from
    that table before anything was scored. The family treats each row as a
    binary prediction, so on that table every precision, recall and area comes
    out biased high, and the report's "population: every row" note is false.
    ADR-0024 decision 3's "computed before the filter" only reaches the filter
    inside evaluation; this is the one upstream of it (ADR-0025).

    Monitoring mode is untouched: it LEFT-joins the labels onto offline
    inference's output, a population no dataset filter ever saw.
    """
    if not post_training or not prediction_quality_on(parameters):
        return []
    if test_carries_zero_positive_group_weight(parameters):
        return []
    return [
        "A46: evaluation.report.sections.prediction_quality is on under "
        "--post-training, but dataset.test_zero_positive_group_ratio is 0 (its "
        "default): the dataset pipeline dropped every test query group holding "
        "no positive before the model scored it. The prediction-quality family "
        "treats each row as a binary prediction, so on that table every metric "
        "comes out biased high and its 'every row' population note is false. "
        "Set dataset.test_zero_positive_group_ratio above 0 and rebuild the "
        "dataset (it moves base_dataset_version), or switch the section off."
    ]


def report_section_key_errors(parameters: dict) -> list[str]:
    """A34 — ``evaluation.report.sections`` declares exactly
    :data:`EVALUATION_REPORT_SECTIONS`.

    Equality, checked in both directions, because each direction alone let a
    real drift through (ADR-0019 decision 6):

    * declared but not read: ``guardrail_recall``, ``per_item_attr``,
      ``category`` and ``per_segment`` were declared and set to ``true`` for
      months while nothing asked about them — switching one off changed
      nothing, and the troubleshooting table told users to do exactly that;
    * read but not declared: ``diagnosis_links`` was asked about by the report
      but absent from the conf, so it was on by default with no line to turn
      it off.

    A check that fires only when the report asks for a name (the pre-check in
    ``_section_on``) never sees the first shape, which is why this runs on the
    conf itself.

    A ``sections`` block that declares no switch at all (absent, null, or an
    empty mapping) is not checked: declaring nothing is a visible opt-out to
    every default, not a key that drifted quietly. Once any switch is
    declared, the block is checked in full.
    """
    eval_params = parameters.get("evaluation", {}) or {}
    if not isinstance(eval_params, Mapping):
        return []
    report = eval_params.get("report", {}) or {}
    if not isinstance(report, Mapping):
        return []
    sections = report.get("sections")
    if sections is None:
        return []
    if not isinstance(sections, Mapping):
        return [
            f"A34: evaluation.report.sections={sections!r} must be a mapping "
            f"of switch name to bool, one key per switch the report reads: "
            f"{sorted(EVALUATION_REPORT_SECTIONS)}."
        ]
    if not sections:
        return []
    declared = set(sections)
    errors = []
    unread = declared - EVALUATION_REPORT_SECTIONS
    if unread:
        # key=str: YAML can hand over a non-str key (``on:`` loads as True),
        # and sorting it against str keys would raise instead of reporting.
        errors.append(
            f"A34: evaluation.report.sections declares "
            f"{sorted(unread, key=str)}, which "
            f"the report never reads — setting it changes nothing. Delete the "
            f"key(s). The switches the report reads are "
            f"{sorted(EVALUATION_REPORT_SECTIONS)}."
        )
    undeclared = EVALUATION_REPORT_SECTIONS - declared
    if undeclared:
        errors.append(
            f"A34: evaluation.report.sections does not declare "
            f"{sorted(undeclared)}, which the report reads — the section is on "
            f"by default and this conf has no line to turn it off. Declare it "
            f"(true keeps the current behaviour)."
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

    # A38 — the mirror image of A2, one line below it on purpose: the item
    # MUST take the categorical_columns exit and an optional role MUST NOT,
    # and the two rules read as one decision only when they sit together.
    errors.extend(optional_role_as_feature_errors(parameters))

    mm = inference_products_mismatch(parameters)
    if mm["only_in_inference"] or mm["only_in_categorical"]:
        errors.append(
            f"inference.products disagrees with schema.categorical_values"
            f"[item]: only_in_inference={mm['only_in_inference']}, "
            f"only_in_categorical={mm['only_in_categorical']}. They must be "
            f"identical sets."
        )
    errors.extend(inference_products_with_counted_items_errors(parameters))

    errors.extend(override_unknown_item_errors(parameters))

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

    errors.extend(weight_unknown_item_errors(parameters))

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

    errors.extend(optional_role_compare_source_errors(parameters))

    errors.extend(segment_source_override_errors(parameters))

    errors.extend(diagnosis_metric_param_errors(parameters))

    errors.extend(suppression_param_errors(parameters))

    errors.extend(training_diagnostics_param_errors(parameters))

    errors.extend(training_hpo_finalize_param_errors(parameters))

    errors.extend(entity_grouping_key_errors(parameters))

    errors.extend(numeric_storage_param_errors(parameters))

    errors.extend(zero_positive_group_ratio_errors(parameters))

    errors.extend(hpo_objective_population_errors(parameters))

    errors.extend(binary_test_metrics_verdict(parameters).errors)

    errors.extend(dataset_source_quality_check_errors(parameters))

    # A33 is a migration-period check; it leaves with the migration (see
    # A33 in the module docstring).
    errors.extend(legacy_evaluation_key_errors(parameters))

    errors.extend(retired_calibration_key_errors(parameters))

    if errors:
        raise ConfigConsistencyError(
            "Config consistency check failed (" + str(len(errors))
            + " issue(s)):\n- " + "\n- ".join(errors)
        )


def item_coverage_errors(
    item: str,
    declared: list[str] | None,
    sample_pool_items: set[str],
    label_items: set[str],
) -> list[str]:
    """B1 invariant — the single definition.

    ``declared`` is ``None`` when the item list is counted from the data
    (#379). Then there is no list to hold sample_pool to: the train months'
    items *are* the list, and a val/test item they lack is a new item, warned
    about where it is encoded (``build_val_model_input`` /
    ``build_test_model_input``, off the landed keys table), not refused.
    Only label_table is checked: an item it holds that sample_pool never
    offers as a candidate is a label for nothing, the same business-logic
    error as below.

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
    if declared is None:
        orphan = sorted(label_items - sample_pool_items)
        if not orphan:
            return []
        return [
            f"label_table has item value(s) {orphan} that sample_pool never "
            f"holds in the dataset windows — label business logic "
            f"(label_*.sql) produced an item that is no one's candidate. "
            f"Reconcile label_*.sql with sample_pool.sql."
        ]
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


# B5 — the Spark ``DataFrame.dtypes`` simpleStrings a declared categorical may
# have. An allow-list rather than a list of what is banned, so a type nobody
# thought of is rejected before the vocabulary scan instead of failing after it.
# Hive ``varchar(n)`` / ``char(n)`` read back as "string" (Spark 3.3.2).
CATEGORICAL_DTYPES = frozenset(
    {"string", "tinyint", "smallint", "int", "bigint", "boolean"}
)

# decimal carries a precision/scale suffix ("decimal(15,0)"), so it is matched
# by prefix below.
_CONTINUOUS_NUMERIC_DTYPES = {"double", "float"}
_DATETIME_DTYPES = {"date", "timestamp"}
_COMPLEX_DTYPE_PREFIXES = ("array<", "struct<", "map<")


class CategoricalDtypeProblem(NamedTuple):
    """Why a type cannot be a categorical's, in three parts a message reads
    in order: what the type is, what goes wrong, what to do instead."""

    kind: str
    why: str
    way_out: str


def categorical_dtype_problem(dt: str) -> CategoricalDtypeProblem | None:
    """Why ``dt`` cannot be a categorical's type.

    ``None`` when it can. The one place a type family's advice is written, so
    B5 (a column declared categorical), B6 (a column that would have to be) and
    ``scripts/suggest_categorical_cols.py`` (a column a human must decide on)
    never tell the user two different things about the same column.
    """
    if dt in CATEGORICAL_DTYPES:
        return None
    if dt.startswith("decimal") or dt in _CONTINUOUS_NUMERIC_DTYPES:
        return CategoricalDtypeProblem(
            "a continuous-numeric type",
            "a decimal categorical is not JSON-serializable (the preprocessor "
            "save crashes after the full vocabulary scan) and a double/float "
            "one is almost always a mis-tag",
            "keep it as a numeric feature, or, if it is a numeric code, cast it "
            "to string or integer in the source ETL",
        )
    if dt in _DATETIME_DTYPES:
        return CategoricalDtypeProblem(
            "a date/time type",
            "used as a category it only ever matches the dates the train months "
            "happened to contain, and the preprocessor cannot store it (the "
            "JSON save crashes after the full vocabulary scan)",
            "derive a numeric feature from it in the source ETL (e.g. days "
            "since the snapshot)",
        )
    if dt == "binary":
        return CategoricalDtypeProblem(
            "a binary type (bytes — a 0/1 flag is boolean or integer instead)",
            "the preprocessor cannot store bytes (the JSON save crashes after "
            "the full vocabulary scan)",
            "if it is a code, convert it to a string in the source ETL — "
            "hex(col) gives each value exactly one string, so no category is "
            "lost",
        )
    if dt.startswith(_COMPLEX_DTYPE_PREFIXES):
        return CategoricalDtypeProblem(
            "a complex type",
            "the encoder cannot encode a complex value",
            "flatten it into scalar string/integer/boolean columns in the "
            "source ETL",
        )
    return CategoricalDtypeProblem(
        "an unsupported type",
        "nothing in the preprocessor is known to handle it",
        "convert it to string, an integer type or boolean in the source ETL",
    )


def categorical_dtype_errors(
    categorical_cols: list[str],
    feature_table_dtypes: dict[str, str],
    table: str = "feature_table",
) -> list[str]:
    """B5 invariant — the single definition.

    A column declared in ``dataset.prepare_model_input.categorical_columns``
    must have one of the :data:`CATEGORICAL_DTYPES` in ``feature_table``:
    string, an integer type, or boolean. Every other type fails somewhere
    downstream, and before this gate it failed late — after a full scan of the
    train months:

    - ``decimal``, ``date``, ``timestamp``, ``binary`` collect to Python values
      ``json.dump`` cannot write, so the preprocessor save crashes.
    - ``double`` / ``float`` serialize, but a continuous value used as a
      category is almost always a mis-tag, float-equality lookup in the
      ``F.create_map`` encoding is fragile, and the vocabulary collection is not
      exact on one (see ``collect_vocabularies_from_data``).
    - complex types (array / struct / map) crash the encoder.

    An allow-list, not a list of those: a type not named here is rejected too,
    rather than being let through to find out. Rejecting them broke no
    configuration that used to produce a usable feature (#407): the only ones
    that finished were columns entirely NULL over the train months, whose empty
    vocabulary encodes every row to the same sentinel. Each rejection names the way out
    for its type family (:func:`categorical_dtype_problem`) — the reason to
    fail early rather than merely fail.

    ``feature_table_dtypes`` maps a feature_table column name to its Spark
    ``DataFrame.dtypes`` simpleString (e.g. ``"decimal(15,0)"``, ``"double"``,
    ``"string"``). Identity categoricals (``schema.item``) come from
    ``schema.categorical_values`` rather than feature_table, so they are absent
    from this mapping and correctly skipped. Pure (no Spark): the Layer-2 gate
    passes ``dict(feature_table.dtypes)`` in. Returns collect-all error strings
    sorted by column; empty list means OK.

    ``table`` is the feature table the dtypes were read from: the gate asks this
    once per feature table (ADR-0026), and the message has to send the operator
    to the one that holds the column.
    """
    errors: list[str] = []
    for col in sorted(categorical_cols):
        dt = feature_table_dtypes.get(col)
        if dt is None:
            continue  # identity categorical / not a feature_table column
        problem = categorical_dtype_problem(dt)
        if problem is None:
            continue
        errors.append(
            f"categorical column {col!r} is {problem.kind} (type={dt}) in "
            f"{table} — {problem.why}. A categorical must be string, an integer "
            f"type or boolean. Remove {col!r} from "
            f"dataset.prepare_model_input.categorical_columns and {problem.way_out}; "
            f"if it is not a model feature, add it to "
            f"dataset.prepare_model_input.drop_columns instead."
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


def numeric_precision_file_errors(
    files_found: int,
    *,
    months: int,
    base_dataset_version: str,
    columns: int,
) -> list[str]:
    """B8, asked before the rule: the gate found the files it reads.

    Pure — the caller counts the files (``landed_partition_files`` in the
    dataset pipeline's ``steps/footer_facts.py``). No file for the months this
    run wrote leaves every checked column's precision unknown. That is one
    error, not one "no statistics" per column from
    :func:`numeric_precision_errors`: the fix is different (the gate could not
    find the data at all, rather than found it and learned nothing), and
    reporting it once beats reporting it once per column. The way out it names
    is the policy key, for the reason that function gives.
    """
    if files_found:
        return []
    return [
        f"B8: found no parquet files for the {months} month(s) this "
        f"run wrote under base_dataset_version={base_dataset_version}, so the "
        f"precision of {columns} feature column(s) could not be established. "
        f"The gate reads footer statistics from the landed partitions; set "
        f"dataset.numeric_precision_policy: truncate to proceed without "
        f"that check."
    ]


def numeric_precision_errors(
    by_column: Mapping[str, ColumnPrecision],
    storage_type: str,
    table: str | None = None,
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

    ``table`` leads every message when given — the candidate-level feature
    table's findings name it; the entity-level table's are the gate's default
    and carry no name.
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
    if table is not None:
        errors = [f"{table}: {e}" for e in errors]
    return errors


def nonnumeric_feature_errors(
    feature_kinds: dict[str, str],
    will_be_encoded: set[str],
    dtypes: dict[str, str] | None = None,
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

    ``dtypes`` (Spark simpleStrings, optional) decides the way out. "Declare it
    categorical" is wrong advice for a type B5 rejects — it sends the user from
    this error straight into that one — so a column whose type cannot be a
    categorical gets B5's way out instead (#407). Without ``dtypes`` every
    column gets the categorical-or-drop advice: the training-read backstop
    passes none, and its real remedy is rebuilding the dataset anyway (see the
    B6 legend).
    """
    dtypes = dtypes or {}
    errors: list[str] = []
    for col in sorted(feature_kinds):
        if feature_kinds[col] == "numeric" or col in will_be_encoded:
            continue
        problem = None if col not in dtypes else categorical_dtype_problem(dtypes[col])
        prefix = (
            f"feature column {col!r} is non-numeric and is not declared "
            f"categorical, so it would become an un-encoded object-dtype "
            f"model feature (OOM at pdf_to_X.to_numpy, then a LightGBM "
            f"float-cast error). "
        )
        if problem is None:
            errors.append(
                prefix
                + f"If {col!r} is a categorical feature, add it "
                f"to dataset.prepare_model_input.categorical_columns (it is then "
                f"integer-encoded); if it is not a model feature, add it to "
                f"dataset.prepare_model_input.drop_columns."
            )
        else:
            errors.append(
                prefix
                + f"It cannot be declared categorical either: it is {problem.kind} "
                f"(type={dtypes[col]}) — {problem.why}. If {col!r} should be a "
                f"model feature, {problem.way_out}; if not, add it to "
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
# B10 — a model_input must hold exactly as many rows as its keys table
# ---------------------------------------------------------------------------


class SplitRowCounts(NamedTuple):
    """What B10 needs to know about one split: how many rows went in and out.

    ``keys_rows`` is the split's keys table; ``model_input_rows`` is the table
    ``build_model_input`` produced from it. One type rather than two parallel
    mappings, for the same reason as :class:`ColumnPrecision`: the pair always
    travels together and a caller that got them out of step would compare one
    split's input against another's output silently.
    """

    keys_rows: int
    model_input_rows: int


def model_input_grain_errors(
    by_split: Mapping[str, SplitRowCounts | Mapping[str, SplitRowCounts]],
    identity_columns: Sequence[str] | None = None,
) -> list[str]:
    """B10 invariant — the single definition.

    Pure — no Spark, no parquet, no filesystem. How the two numbers are
    obtained without scanning the data is the caller's problem (``footer_rows``
    in the dataset pipeline's ``steps/footer_facts.py``), and keeping it out of
    here is what lets the rule be tested by handing it a dict.

    **Equality, not a bound.** ``build_model_input`` joins keys to labels and to
    features with LEFT joins on the keys' own grain, so a key that matches
    nothing keeps its row and a key that matches once keeps its row: the output
    has exactly the input's rows unless a right table holds a join key twice.
    That makes ``!=`` the whole rule, and it makes both directions worth
    reporting — more rows is the N-times-too-large dataset the node comment
    names, fewer rows means something other than that fan-out happened and a
    gate that stayed silent about it would be claiming a guarantee it did not
    check.

    The multiplier is in the message because it names the cause: 2.0 says "one
    duplicated key in a right table", 1.0002 says "a handful", and the two send
    an operator to different queries. It is omitted when there are no keys to
    divide by — a ratio against zero has no meaning and the message has to
    stand without one.

    Collect-all and sorted by split: two runs of the same config read the same
    way, and one investigation covers every offending split.

    **The rule did not change when ``event`` arrived; the wording did.** A
    duplicated join key is still an upstream table being wrong, and the two
    impressions of one item are not a duplicated key — they differ in the
    ``event`` columns, which are part of identity (ADR-0025's correction to
    ADR-0021, which had called this a reversal of meaning). What a reader of
    this message cannot otherwise tell is *which* columns count as one key
    here, and that is the half that moves with the declared roles. So
    ``identity_columns`` is named in the message when the caller passes it.
    Optional rather than required: the predicate stays callable from a test
    that only cares about the counts, and an omitted list simply drops that
    clause.

    **A split written a month at a time comes as ``{month: SplitRowCounts}``**
    — test, whose table accumulates months (ADR-0029 decision 4). Each month
    is its own pair: a fan-out in one month and a shortfall in another would
    cancel in a sum. Its message adds what only such a split needs: the month
    has landed, so a plain re-run skips it and leaves the bad rows in place —
    it has to be rebuilt by name — and, for a month rebuilt that way, a
    partition the rebuild did not rewrite survives from the earlier write
    and is counted too.
    """
    errors: list[str] = []
    for split in sorted(by_split):
        counts = by_split[split]
        per_month = (
            sorted(counts.items()) if isinstance(counts, Mapping)
            else [(None, counts)]
        )
        errors += [
            _grain_error(split, month, keys_rows, model_input_rows, identity_columns)
            for month, (keys_rows, model_input_rows) in per_month
            if keys_rows != model_input_rows
        ]
    return errors


def _grain_error(
    split: str,
    month: str | None,
    keys_rows: int,
    model_input_rows: int,
    identity_columns: Sequence[str] | None,
) -> str:
    """One B10 message: one split, or one month of a split written by month."""
    # Fixed decimals rather than significant figures: ``:.4g`` renders
    # a 1.0002x fan-out as "1x", which reads as agreement.
    ratio = (
        f" ({model_input_rows / keys_rows:,.4f}x)" if keys_rows else ""
    )
    key_clause = (
        f" One key here is {list(identity_columns)}."
        if identity_columns else ""
    )
    in_month = f" for {month}" if month is not None else ""
    rebuild_clause = (
        f" {month} has landed, so a plain re-run skips it and keeps these "
        f"rows: once the source is fixed, rebuild it with --rebuild-dates "
        f"{month}. If it was just rebuilt that way, the extra rows can "
        f"also be an item partition (or the whole month) the rebuild did "
        f"not rewrite because it came out empty this time — dynamic "
        f"partition overwrite leaves such partitions as they were."
        if month is not None else ""
    )
    return (
        f"B10: {split}_model_input holds {model_input_rows:,} row(s)"
        f"{in_month} but {split}_keys holds {keys_rows:,}{ratio}. "
        f"build_model_input LEFT "
        f"joins the keys to label_table, to preprocessed_feature_table and, "
        f"when one is declared, to candidate_feature_table, each on the "
        f"keys' own grain, so the two counts can only differ if a right "
        f"table holds one of those join keys more than once — the "
        f"silently N-times-too-large dataset that node's comment names."
        f"{key_clause} "
        f"Check the duplicate-key contract on label_table, on "
        f"feature_table and on candidate_feature_table's source table "
        f"(source_etl quality_checks: "
        f"max_duplicate_key_ratio, plus primary_key — A32 passes when both "
        f"are absent), and do not de-duplicate downstream: which of the "
        f"duplicate rows is the right one is not knowable here."
        f"{rebuild_clause}"
    )


def model_input_grain_scope_errors(
    split: str,
    side: str,
    *,
    files_in_table: int,
    files_in_scope: int,
    scope: Mapping[str, str],
) -> list[str]:
    """B10, asked before the rule: a table compared whole has files in scope.

    Pure — the caller counts both numbers (``footer_rows`` in the dataset
    pipeline's ``steps/footer_facts.py``). ``side`` is ``"keys"`` or
    ``"model_input"``; ``scope`` is the partition values the table was written
    under (the dataset version, and for the train tables the train variant).

    A scope that matches none of a table's files makes both sides of
    :func:`model_input_grain_errors` read 0, every comparison trivially true,
    and the gate would report success having looked at nothing — so it is an
    error, not zero rows. A table with no files **at all** is a different fact
    and passes: that is a genuinely empty split, which
    ``dataset.train_dev_ratio: 0`` produces on purpose. From two file counts
    this cannot tell a scope mismatch from a split this version left empty
    while other versions' files sit in the same table, so the message names
    both readings.

    Only for tables compared whole. For a table written a month at a time a
    month with no file is what a month the group drop emptied looks like, and
    ``validate_model_input_grain`` counts it as 0 = 0.

    It can fire only where the table's file listing reaches past the scope.
    Under Spark's default settings a catalog-loaded frame lists the scope's
    files alone, so a scope with no files reads as a table with none and
    passes; which settings list more is in ``steps/footer_facts.py``.
    """
    if not files_in_table or files_in_scope:
        return []
    spec = ", ".join(f"{k}={v}" for k, v in scope.items())
    return [
        f"B10: {split}_{side} has {files_in_table} parquet file(s) but "
        f"none under {spec}, so this run's row count could not be "
        f"established and the comparison for {split} would have "
        f"passed on two zeroes. Either the version/variant in "
        f"parameters no longer matches what is on disk, the "
        f"table was written by a different catalog entry than the "
        f"one this node reads, or {split} came out empty under "
        f"this version while other versions' files remain (for "
        f"val: every query group dropped — r = 0 and the val "
        f"month's labels not in yet, say)."
    ]


# ---------------------------------------------------------------------------
# B7 — a carry column that also lives in feature_table must be dropped
# ---------------------------------------------------------------------------


def carry_column_collision_errors(
    carry_columns: list[str],
    feature_table_columns: set[str] | list[str],
    drop_columns: list[str],
    identity_columns: list[str],
    label_column: str,
    table: str = "feature_table",
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
    collect-all error strings sorted by column; empty list means OK. ``table``
    names the feature table those columns came from: the candidate-level one
    (ADR-0026) is joined onto the same rows, so a carried column in it collides
    the same way, and is asked separately so the message names the right table.
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
            f"{table}, so build_model_input would join two frames that "
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
# B12 — a feature column named like the zero-positive group weight
# ---------------------------------------------------------------------------


def zero_positive_group_weight_collision_errors(
    parameters: dict,
    feature_columns: Sequence[str],
) -> list[str]:
    """B12 — no model feature may be called :data:`ZERO_POSITIVE_GROUP_WEIGHT_COL`
    while val or test keeps zero-positive groups.

    Returns error strings (empty list when fine), collected by
    ``validate_data_consistency``. Pure: the caller hands in the feature
    columns it derived from the feature tables' metadata — no rows. Both
    tables' features count: a candidate-level one reaches the same frame
    (ADR-0026).

    Above 0 the ``filter_{val,test}_keys`` nodes put that column on the keys,
    and the build carries it into the frame the features join: a feature by
    the same name makes the build's output select ambiguous (the runtime
    backstop, loud but unnamed), and only once the joins are planned. Only
    features count: val / test keys carry nothing else, so a carry column
    cannot reach those tables, and a column listed in ``drop_columns`` never
    becomes a feature. The train ratio adds no weight, so it cannot collide.
    """
    keeps = [
        split for split in ("val", "test")
        if resolved_zero_positive_group_ratio(parameters, split) > 0.0
    ]
    if not keeps or ZERO_POSITIVE_GROUP_WEIGHT_COL not in feature_columns:
        return []
    keys = " and ".join(f"dataset.{s}_zero_positive_group_ratio" for s in keeps)
    return [
        f"B12: feature column {ZERO_POSITIVE_GROUP_WEIGHT_COL!r} is a model "
        f"feature, and {keys} > 0 makes the dataset pipeline add a column by "
        f"that name to the same table (the zero-positive group weight). Rename "
        f"the source column, or list it in "
        f"dataset.prepare_model_input.drop_columns if it is not a feature."
    ]


# ---------------------------------------------------------------------------
# A11/A12/A13 — compare-source predicates (multi-model comparison feature)
# ---------------------------------------------------------------------------

_COMPARE_KINDS = {"model_version", "external_hive"}
_VALID_UNMAPPED = {"fail", "drop"}
# Same-stack Hive tables a model_version compare source may read from.
# Mirror of pipelines.evaluation.steps.compare_sources.MODEL_VERSION_SOURCES;
# A11 is the config-static gate, the loader checks again at read time.
_VALID_MODEL_VERSION_SOURCES = {
    "enriched_eval_predictions",
    "ranked_predictions",
    "training_eval_predictions",
}


def _required_external_columns(parameters: dict, declared=()) -> set[str]:
    """Columns an ``external_hive`` compare source must declare, as schema roles.

    Every entry is a *role* resolved through :func:`get_schema` — the identity
    columns (time + entity + item) plus score — never a literal column name.
    ``entity`` is a list, so a multi-column entity requires all of its columns.

    A multi-column item can be declared two ways (ADR-0027 decision 3), and
    ``declared`` — the source's ``columns`` keys — says which one this source
    uses. Its source columns, when the external table holds them: the loader
    combines them as every other entry does, and ``prod_mapping``'s keys are
    the combined values. Or ``item`` alone, when the external table names an
    item with one id of its own (another system's ad code): there is nothing
    to combine, and ``prod_mapping`` translates those ids. With neither
    declared, the source columns are what is asked for.
    """
    schema = get_schema(parameters)
    item = schema["item"]
    sources = schema["item_source_columns"]
    required = {c for c in schema["identity_columns"] if c != item} | {schema["score"]}
    if len(sources) > 1 and item not in declared:
        return required | set(sources)
    return required | {item}


def _external_item_declared_twice(parameters: dict, declared) -> list[str]:
    """Item source columns declared alongside ``item`` itself, sorted.

    Only a multi-column item has the two spellings. Both at once leaves the
    loader two different item values for one row — the table's own id and
    the combined one — so neither is taken on trust.
    """
    schema = get_schema(parameters)
    sources = schema["item_source_columns"]
    if len(sources) < 2 or schema["item"] not in declared:
        return []
    return sorted(set(sources) & set(declared))


def _external_item_alternative_hint(parameters: dict, missing) -> str:
    """For a multi-column item whose source columns are missing: say that
    ``item`` alone is the other way to declare it. Empty otherwise, so a
    single-column deployment reads the message it always read."""
    schema = get_schema(parameters)
    sources = schema["item_source_columns"]
    if len(sources) < 2 or not set(sources) & set(missing):
        return ""
    return (
        f" — or, if the external table names items with one id of its own, "
        f"map {schema['item']!r} to that column instead of {sources} "
        f"(prod_mapping then translates those ids)"
    )


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
            missing = _required_external_columns(parameters, cols) - set(cols.keys())
            if missing:
                errs.append(
                    f"(A11) compare_sources[{key!r}].columns missing required keys: {sorted(missing)}"
                    + _external_item_alternative_hint(parameters, missing)
                )
            twice = _external_item_declared_twice(parameters, cols)
            if twice:
                errs.append(
                    f"(A11) compare_sources[{key!r}].columns declares both 'item' "
                    f"and item source column(s) {twice}. Declare the source "
                    f"columns when the external table holds them (they are "
                    f"combined, ADR-0027), or 'item' alone when it names items "
                    f"with one id of its own — not both."
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


def optional_role_compare_source_errors(parameters: dict) -> list[str]:
    """(A41) no ``ranked_predictions`` compare source while an optional role is
    declared.

    Returns error strings (empty list when fine), collected by
    :func:`validate_config_consistency`.

    ``ranked_predictions`` is offline inference's output: its rows are the
    framework's own entity x item grid and carry no optional-role column
    (ADR-0025 decision 1). This run's rows do, so the two sides identify rows
    differently — A40's reason, in compare mode. The two post-training tables
    of the same model version carry the columns (A39), so the message names
    them. Only ``kind: model_version`` has a ``source``; a malformed source is
    A11's to report and is skipped here.
    """
    declared = optional_role_column_map(parameters)
    if not declared:
        return []
    sources = (
        (parameters.get("evaluation", {}) or {}).get("compare_sources", {}) or {}
    )
    named = "; ".join(
        f"schema.columns.{role}={cols}" for role, cols in declared.items()
    )
    return [
        f"(A41) compare_sources[{key!r}].source='ranked_predictions' cannot be "
        f"compared while an optional column role is declared ({named}): "
        f"offline inference writes that table from its own entity x item grid, "
        f"so its rows carry no such column and cannot be matched to this run's "
        f"rows. Use source: training_eval_predictions or "
        f"enriched_eval_predictions (a post-training run of that model "
        f"version)."
        for key, src in sources.items()
        if isinstance(src, dict)
        and src.get("kind") == "model_version"
        and src.get("source") == "ranked_predictions"
    ]


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


def parse_etl_var_flags(
    raw_vars: list[str] | None,
) -> tuple[list[tuple[str, str]], list[str]]:
    """Split each ``--var KEY=VALUE`` string on the first ``=``.

    The one ``KEY=VALUE`` split implementation shared by :func:`etl_cli_var_errors`
    (A35) and :func:`merged_etl_variables`, and callable by the CLI itself for
    the "effective variables" log line — so a change to the split rule (e.g.
    allowing ``=`` inside a value) cannot leave the validator, the merge, and
    the log disagreeing about what one flag meant.

    Returns ``(parsed, parse_errors)``: ``parsed`` is the ordered list of
    ``(key, value)`` pairs for well-formed items (duplicates kept, in the
    order given — callers that care about "seen twice" want every occurrence);
    ``parse_errors`` is one ``(A35)`` message per item with no ``=``. A
    malformed item contributes nothing to ``parsed``: there is no sane
    ``(key, value)`` to hand back for it.
    """
    parsed: list[tuple[str, str]] = []
    errors: list[str] = []
    for raw in raw_vars or []:
        if "=" not in raw:
            errors.append(
                f"(A35) --var {raw!r} has no '='. Use --var key=value, e.g. "
                f"--var raw_db=my_raw_db (repeat --var for more than one)."
            )
            continue
        key, value = raw.split("=", 1)
        parsed.append((key, value))
    return parsed, errors


def etl_cli_var_errors(
    variables, raw_vars: list[str] | None
) -> list[str]:
    """(A35) ETL ``--var`` overrides must be well-formed, declared, and safe.

    Guards every way a repeatable ``--var key=value`` flag on the four source
    ETL commands (feature_etl/label_etl/sample_pool_etl/
    inference_population_etl, #370) could silently do the wrong thing,
    collected in one pass (collect-all) so a config with several problems is
    fixed in one run. ``variables`` is the stage's YAML ``variables`` block
    (e.g. ``etl_config.get("variables")``); ``raw_vars`` is the list of
    ``--var`` strings exactly as typed.

    Checks, in the order run:

    (j) ``variables`` itself must be a mapping (absent/``None`` is fine) —
        checked first because every later check calls ``.items()``/``in`` on
        it, which would raise on a list or scalar instead of reporting.
    (h) a declared ``variables.target_date`` — even a plain string value is
        silently overwritten per date by ``--target-dates``/``target_dates``
        (``sql_runner.py``'s ``_table_variables``), so keeping the key only
        misleads a reader of the YAML. Because (h) already owns this name
        unconditionally, ``target_date`` is skipped entirely by (g), (f) and
        (k) below — those would otherwise fire *in addition to* (h) and hand
        out mutually contradictory instructions for the exact same key (a
        review of this ticket caught this: ``target_date: ~`` used to also
        trigger (f)'s "pass it via --var", which (c) then rejects; ``target_date: 2025``
        used to also trigger (g)'s "quote it", which (h) still rejects once quoted).
    (i) ``variables.target_db: ~`` (null) — null means "must come from
        --var", but (d) below means target_db can never come from --var, so
        this is unsatisfiable by construction.
    (g) any other YAML value that is neither a string nor null (a number,
        bool, list, ...) — ``SQLRenderer.render`` does ``str.replace`` and
        would raise a raw ``TypeError`` mid-render.
    (a) a --var item with no '=' (delegated to :func:`parse_etl_var_flags`).
    (e) the same --var name passed more than once.
    (c) ``--var target_date=...`` — rejected; use --target-dates instead.
    (d) ``--var target_db=...`` — rejected; edit the YAML instead.
    (b) a --var name absent from the declared ``variables`` — a typo'd flag
        would otherwise be a silent no-op, never reaching the SQL. Checked
        once per distinct name (a name repeated by (e) is not also reported
        by (b)/(c)/(d) once per repetition — a review of this ticket found
        ``--var typo=1 --var typo=2`` printing the same (b) message twice).
    (f) a YAML ``variables`` value of ``null`` with no matching --var this
        run — checked unconditionally, independent of ``--restart-from``: a
        table skipped this run may still be reached by a later
        ``--restart-from`` run against the same config.
    (k) a variable's FINAL value — the YAML string, or the ``--var``
        override when one was given — referencing another user variable via
        ``${...}``. ``SQLRenderer.render`` substitutes one key at a time, in
        the dict's iteration order (a single-pass ``str.replace`` per key),
        so whether a ``${...}`` *inside* a value gets expanded depends on
        where that key happens to sit relative to the variable it names — the
        same YAML/``--var`` combo can render two different SQL strings
        depending on dict order, which no config author can see or control.
        And when it does NOT get expanded, it is not "unresolved" either: a
        real Spark session has its own ``${...}`` substitution
        (``spark.sql.variable.substitute``, on by default) that silently
        turns a residual ``${...}`` into an empty string (measured against
        local Spark 3.3.2 during this ticket's review: ``'${nope}'`` ->
        ``''``) rather than raising — the exact silent-wrong-answer failure
        mode A35 exists to prevent, just one substitution layer further down.
        ``${target_date}`` is the one exception and is never flagged: a
        review of this ticket found the order-dependence argument does not
        apply to it — ``_table_variables`` always substitutes ``target_date``
        last, and it can never be declared in the YAML in the first place
        (h), so a reference to it is unaffected by declaration order and
        always expands. A value referencing ``${target_date}`` plus at least
        one OTHER variable is still rejected — only ``${target_date}``
        references, alone, are exempt. The error message names only the
        rejected ``${...}`` fragment(s), never the value's other contents —
        the value may have been assembled from a YAML ``${env.X}``, so it may
        hold a secret.

    Returns error strings (empty when everything is fine); the ETL command
    (``__main__._run_etl``) logs and exits before Spark starts. NOT
    aggregated by ``validate_config_consistency``: that gate runs at the
    entry of every command and is never given the ``--var`` flags, mirroring
    A12/A21/A30.
    """
    errors: list[str] = []

    if variables is not None and not isinstance(variables, Mapping):
        errors.append(
            f"(A35) variables must be a mapping of name to value; got "
            f"{type(variables).__name__}: {variables!r}."
        )
        variables = {}
    variables = variables or {}

    if "target_date" in variables:
        errors.append(
            "(A35) variables.target_date is declared in the YAML. Every "
            "date from target_dates/--target-dates is bound to "
            "${target_date} for that iteration, silently overwriting "
            "whatever is declared here. Remove variables.target_date and "
            "use target_dates/--target-dates instead."
        )

    if "target_db" in variables and variables["target_db"] is None:
        errors.append(
            "(A35) variables.target_db is null (~), which asks for a value "
            "via --var — but --var target_db=... is not allowed (writing to "
            "a different Hive database must show up as a file diff in the "
            "YAML, and the dataset pipeline downstream reads a separate "
            "key, hive.db, that --var would not change), so this can never "
            "be satisfied. Set variables.target_db to an explicit string in "
            "the YAML."
        )

    for name, value in variables.items():
        if name == "target_date":
            continue  # (h) already owns this name unconditionally
        if name == "target_db" and value is None:
            continue  # covered by the target_db-specific message above
        if value is not None and not isinstance(value, str):
            errors.append(
                f"(A35) variables.{name}={value!r} is not a string (got "
                f"{type(value).__name__}). Quote it in the YAML, e.g. "
                f"{name}: {str(value)!r}, or use null (~) to require --var."
            )

    parsed, parse_errors = parse_etl_var_flags(raw_vars)
    errors.extend(parse_errors)

    counts: dict[str, int] = {}
    for key, _value in parsed:
        counts[key] = counts.get(key, 0) + 1
    for name in sorted((n for n, c in counts.items() if c > 1), key=str):
        errors.append(
            f"(A35) --var {name}=... was passed {counts[name]} times. Pass "
            f"each variable at most once."
        )

    # Distinct names only (not one iteration per repetition of `parsed`) —
    # (e) above already reports "passed N times"; (b)/(c)/(d) below must not
    # also print their own message N times for the same repeated name.
    distinct_keys = list(dict.fromkeys(key for key, _value in parsed))
    for key in distinct_keys:
        if key == "target_date":
            errors.append(
                "(A35) --var target_date=... is not allowed: each "
                "--target-dates value is bound to ${target_date} for that "
                "run and would silently overwrite it. Use --target-dates "
                "instead."
            )
        elif key == "target_db":
            errors.append(
                "(A35) --var target_db=... is not allowed: edit "
                "variables.target_db in the stage's parameters YAML "
                "instead, so which database gets written leaves a file "
                "diff. (The dataset pipeline downstream reads a separate "
                "key, hive.db, that --var would not change anyway.)"
            )
        elif key not in variables:
            errors.append(
                f"(A35) --var {key}=... is not declared in variables. "
                f"Declared names: {sorted(variables.keys(), key=str)}. "
                f"Check for a typo, or add `{key}: ~` to variables in the "
                f"stage's parameters YAML first."
            )

    cli_keys = {key for key, _value in parsed}
    for name, value in variables.items():
        if name == "target_date":
            continue  # (h) already owns this name unconditionally
        if name == "target_db":
            continue  # covered by the target_db-specific null message above
        if value is None and name not in cli_keys:
            errors.append(
                f"(A35) variables.{name} is null (~) in the YAML, which "
                f"requires a value via --var {name}=..., but none was "
                f"passed for this run."
            )

    # (k) — see the docstring above for the full reasoning (render's
    # declaration-order substitution + Spark's own silent-empty-string
    # fallback). ``target_date`` as a NAME is skipped: (h) already owns it
    # and its declared value never reaches render anyway
    # (``_table_variables`` always overwrites it with the real snap_date).
    # ``target_db`` is checked against its YAML value only — a ``--var``
    # override for it is already rejected by (d), so there is no legitimate
    # CLI-sourced final value to check. Within another variable's value,
    # ``${target_date}`` REFERENCES are exempt (see docstring) — every other
    # ``${...}`` token is rejected, and the message names only the rejected
    # token(s), never the value itself (which may hold a secret pulled in
    # via a YAML ``${env.X}``).
    cli_override = dict(parsed)  # last occurrence wins, matching merged_etl_variables
    for name, value in variables.items():
        if name == "target_date":
            continue
        final_value = value if name == "target_db" else cli_override.get(name, value)
        if not isinstance(final_value, str):
            continue
        tokens = re.findall(r"\$\{[^}]*\}", final_value)
        bad_tokens = [t for t in dict.fromkeys(tokens) if t != "${target_date}"]
        if bad_tokens:
            errors.append(
                f"(A35) variables.{name} references {bad_tokens} inside its "
                f"value (the value itself is not printed — it may have been "
                f"assembled from a YAML ${{env.X}} and hold a secret). "
                f"SQLRenderer.render substitutes user variables one at a "
                f"time in declaration order, so whether a reference to "
                f"another variable expands depends on where {name!r} sits "
                f"relative to it. ${{target_date}} is the one exception: "
                f"it is always substituted last, so it is unaffected by "
                f"order. Anything else that does not expand "
                f"is not caught as unresolved either: a real Spark session "
                f"silently turns it into an empty string instead of "
                f"raising. Write the final literal value; an expression "
                f"that must vary per date may reference ${{target_date}}, "
                f"or be written directly into the SQL file."
            )

    return errors


def merged_etl_variables(variables, raw_vars: list[str] | None) -> dict:
    """Merge ``--var`` overrides onto the YAML ``variables`` dict (CLI wins).

    Callers MUST run :func:`etl_cli_var_errors` first and stop on any error
    — this function assumes clean input (e.g. it silently applies a
    malformed --var item's key/value split the same way
    :func:`parse_etl_var_flags` did, which the gate above already rejected)
    and exists only to keep the ``KEY=VALUE`` split logic in one place.
    Returns a NEW dict; the caller's YAML ``variables`` mapping is not
    mutated, so the ``etl_config`` handed to ``SQLRunner`` is a copy, not the
    ``ConfigLoader``-owned dict.
    """
    merged = dict(variables or {})
    parsed, _parse_errors = parse_etl_var_flags(raw_vars)
    for key, value in parsed:
        merged[key] = value
    return merged


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


def resolved_baseline_rebuild_dates(
    rebuild_dates, *, rate_wired: bool, eval_dates, lookback_months: int,
) -> list[str]:
    """(A21) ``--rebuild-dates`` for the evaluation command (#397).

    The flag names time values of ``popularity_period_counts`` to recount even
    though they landed (after a sample_pool / label_table backfill). Returns
    the sorted ``YYYY-MM-DD`` list, ``[]`` when not passed.

    * Refused unless the positive-rate baseline is wired (``rate_wired``, the
      caller's ``baseline_scores_by_rate`` and not ``--compare-only``):
      otherwise nothing reads the table, so the flag would be a silent no-op,
      A21's failure.
    * Each date must fall inside ``[S - lookback_months, S)`` of some date S of
      ``evaluation.snap_date``: the run only counts the periods its windows
      need. The window is ``core.date_ranges.lookback_window_bounds``, the
      node's own rule. Whether sample_pool actually holds a named date is
      known only after its listing:
      :func:`baseline_rebuild_dates_absent_errors`.

    ``lookback_months`` is a callable returning the caller's
    ``resolve_lookback_months``, so this check and the node cannot default
    differently (ADR-0020 bug 1), and it is read only when the flag was passed
    to a run that wires the path: a run without it reads no baseline key main
    did not read.
    """
    if not rebuild_dates:
        return []
    if not rate_wired:
        raise ConfigConsistencyError(
            "(A21) evaluation --rebuild-dates recounts popularity_period_counts, "
            "which only the positive-rate baseline reads. It is wired only "
            "with evaluation.baseline.score: rate, "
            "evaluation.report.sections.baseline on, --post-training (monitoring "
            "scores the full grid and keeps the count), and not --compare-only. "
            "This run is missing one of them, so the flag would do nothing; "
            "drop --rebuild-dates."
        )
    lookback_months = lookback_months()
    malformed = [d for d in rebuild_dates if _iso_date(d) is None]
    if malformed:
        raise ConfigConsistencyError(
            f"(A21) --rebuild-dates got non-ISO value(s) {malformed!r}. "
            "Expected YYYY-MM-DD."
        )
    windows = []
    for s in eval_dates:
        lower, upper = lookback_window_bounds(s, lookback_months)
        windows.append((pd.Timestamp(lower), pd.Timestamp(upper)))
    requested = sorted({_iso_date(d) for d in rebuild_dates})
    outside = [
        d for d in requested
        if not any(lo <= pd.Timestamp(d) < up for lo, up in windows)
    ]
    if outside:
        spans = ", ".join(
            f"[{lo.date()}, {up.date()})" for lo, up in windows) or "none"
        raise ConfigConsistencyError(
            f"(A21) --rebuild-dates names {outside}, outside every lookback "
            f"window of evaluation.snap_date (lookback_months="
            f"{lookback_months}: {spans}). This run only counts the periods "
            "those windows need, so it would have been a silent no-op."
        )
    return requested


def baseline_rebuild_dates_absent_errors(rebuild, periods) -> list[str]:
    """(A21) ``--rebuild-dates`` values sample_pool holds no rows at.

    The half of the evaluation command's A21 that needs sample_pool's listing
    (``periods``: its time values inside the lookback windows, the plan's
    configured periods). A value inside a window that is not one of them
    would recount nothing — a silent no-op.
    """
    absent = [d for d in rebuild if d not in set(periods)]
    if not absent:
        return []
    return [
        f"(A21) --rebuild-dates names {absent}, which sample_pool holds no "
        f"rows at inside the lookback windows (its time values there: "
        f"{','.join(periods) or '-'}), so nothing would be recounted."
    ]


def compare_mutual_exclusive_errors(compare: str | None, compare_only: str | None) -> list[str]:
    """(A13) --compare and --compare-only must not be passed together."""
    if compare is not None and compare_only is not None:
        return [
            f"(A13) --compare={compare!r} and --compare-only={compare_only!r} "
            "are mutually exclusive — pass at most one"
        ]
    return []


def post_training_snap_date_errors(parameters: dict, post_training: bool) -> list[str]:
    """(A22) Under ``--post-training``, every evaluation.snap_date must be a test month.

    ``evaluation.snap_date`` is one date or a list of dates (#374); with
    several, each must be in ``dataset.test_snap_dates`` and the message names
    the ones that are not.

    Returns error strings (empty list when fine); the CLI raises. Wired like
    A13 — it lives here, but the evaluation command calls it explicitly and
    hands it the mode flag; the date handling mirrors A21. It is deliberately
    NOT aggregated by ``validate_config_consistency``: that runs at CLI entry
    and never sees ``--post-training``, and the default monitoring mode reads
    inference output whose month legitimately need not be a test month, so an
    unconditional predicate would block a valid run.

    Reads the nested ``parameters['evaluation']['snap_date']`` — the same shape
    ``pipelines.evaluation.nodes.prepare_eval_data`` reads, deliberately without
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
    # Several evaluated dates (#374): each must be a test month, and the
    # message names the ones that are not. One date — written plainly or as a
    # one-element list — keeps the single-date messages below word for word.
    dates = as_date_list(raw)
    if len(dates) > 1:
        unreadable_snaps = [v for v in dates if _iso_date(v) is None]
        if unreadable_snaps:
            return [
                f"(A22) evaluation.snap_date holds {unreadable_snaps!r}, not a "
                f"readable ISO date (YYYY-MM-DD). --post-training evaluates "
                f"configured test months only; each date must be one of "
                f"{configured}."
            ]
        not_test = [s for s in (_iso_date(v) for v in dates) if s not in configured]
        if not_test:
            return [
                f"(A22) evaluation.snap_date date(s) {not_test!r} are not test "
                f"months (dataset.test_snap_dates: {configured}). "
                "--post-training reads training_eval_predictions, which "
                "accumulates every month ever predicted for this model_version, "
                "so an unlisted month can still return rows and yield a "
                "normal-looking report for a month this config does not "
                "evaluate. Add them to dataset.test_snap_dates and rerun "
                "dataset + predict, or drop them from evaluation.snap_date. "
                "(Monitoring mode — no --post-training — is not subject to "
                "this rule.)"
            ]
        return []
    if isinstance(raw, list) and len(dates) == 1:
        raw = dates[0]
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


#: The three ``dataset.*_snap_dates`` splits A24 keeps disjoint, in the order
#: their pairs are reported.
_DATE_SPLIT_NAMES = ("train", "val", "test")


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
      (``select_sample_keys``, ``collect_dataset_snap_dates``,
      ``fit_preprocessor_metadata``), so today it is a raw ``KeyError`` raised
      inside a Spark node: 2-4 minutes of cold start before a message that
      names neither the config key nor the fix.
    * **not a list** — a bare string is iterable, so none of those three sites
      raise. They walk it character by character and try to read ``"2"`` as a
      date.
    * **empty** — the branch with no downstream guard at all. An empty month
      list matches no row (``months_filter_as_date``), so ``select_sample_keys``
      draws nothing and the fit reads no vocabulary, and nothing at those two
      steps raises. A24 cannot see it: an empty set overlaps nothing.
      ``split_train_keys``' degenerate guard (ADR-0005) only fires when
      ``train_dev_ratio != 0``, and ``0`` is a legal setting. Before ADR-0029
      decision 3 the same list left the pool **whole** instead — every month
      of ``sample_pool``, the test months included — which is what this branch
      was first written against.
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
            "(A23) dataset.train_snap_dates is empty. Every read of the train "
            "months would match no row, so there would be nothing to train "
            "on. Name the months to train on."
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


def train_version_landed_errors(
    *,
    base_landed: bool,
    variant_landed: bool,
    base_dataset_version: str,
    train_variant_id: str,
) -> list[str]:
    """(A55) ``--only-test-months`` needs this config's train version to have landed.

    Returns error strings (empty list when fine); the dataset command raises
    before it writes a manifest stub or runs a node.

    The mode leaves out every train build, so it can only reuse a train
    version built earlier. A train-only key (``core/versioning.py``'s
    ``TRAIN_SAMPLING_KEYS``: the train sampling settings and
    ``carry_columns``) moves ``train_variant_id`` and not
    ``base_dataset_version``; before this check the run went ahead, wrote
    nothing under the new variant, and training read it as 0 rows without an
    error at any layer (#334).

    The two facts are metastore partition listings of ``train_model_input``,
    taken by the command (``pipelines/dataset/run_contract.py``) and passed
    in — this module never reads the catalog (A28's reason). ``base_landed``
    is "some variant of this base has partitions", ``variant_landed`` "this
    variant has". A manifest's ``status: completed`` is not the evidence: a
    slice that never touched train used to write it too (ADR-0029
    decision 12).

    Two messages, because the operator has two different things to find out:

    * **nothing under the base** — this base version was never built. Why the
      base moved is the question, so the likely causes are listed; one of
      them may be an edit the operator did not mean to make.
    * **the base without this variant** — a train-only setting changed since
      that base was built.

    Both end in a full dataset run. The mode deliberately does not build the
    missing variant itself: it stays "add a test month", and
    ``scripts/rebuild_eval_month.sh`` does not grow back the train rebuild
    ADR-0012 took out of it.

    NOT aggregated by :func:`validate_config_consistency`: it needs the
    ``--only-test-months`` flag and the metastore, neither of which that gate
    sees. Wired like A23/A24: the command logs each error and exits.
    """
    if variant_landed:
        return []
    if not base_landed:
        return [
            f"(A55) --only-test-months: train_model_input has no partition "
            f"under base_dataset_version={base_dataset_version}, so this base "
            f"version has never been built and there is no train data for the "
            f"mode to reuse. Run the full dataset pipeline (without "
            f"--only-test-months). Likely causes: this is the first run; a "
            f"base-level setting or the feature table's schema changed; a "
            f"framework upgrade raised the dataset artifact format version "
            f"(ADR-0029 decision 15)."
        ]
    return [
        f"(A55) --only-test-months: train_model_input has partitions under "
        f"base_dataset_version={base_dataset_version} but none under "
        f"train_variant_id={train_variant_id}: a train-only setting changed "
        f"(the keys in core/versioning.py's TRAIN_SAMPLING_KEYS — the train "
        f"sampling settings such as dataset.sample_ratio, and "
        f"dataset.carry_columns) and this train version has not been built. "
        f"Run the full dataset pipeline (without --only-test-months), or "
        f"revert that change."
    ]


def preprocessor_on_disk_path_errors(
    preprocessor_filepath: str, on_disk_filepath: str,
) -> list[str]:
    """(A56) ``preprocessor_on_disk`` must name ``preprocessor``'s file.

    Returns error strings (empty list when fine); the CLI stops the run.

    Only reached when a deployment's catalog writes the entry itself: left
    out, the CLI derives it from ``preprocessor`` and there is nothing to
    compare. Written, it has to be the same file, because the entry is
    optional (the file does not exist before a version's first run): a path to
    anywhere else loads as ``None``, and ``fit_preprocessor_metadata`` takes
    that for a first run and skips the item-list comparison (B19) without a
    word.

    Paths are compared as paths, so ``./`` or a doubled slash is the same
    file. The strings are the resolved ones, ``${...}`` already substituted.
    """
    if Path(preprocessor_filepath) == Path(on_disk_filepath):
        return []
    return [
        f"(A56) catalog entry 'preprocessor_on_disk' reads "
        f"{on_disk_filepath!r}, but 'preprocessor' writes "
        f"{preprocessor_filepath!r}. The two names are one file: the fit reads "
        f"it under the second name before overwriting it, to compare the item "
        f"list (B19). The entry is optional, so from another path it loads as "
        f"nothing and that comparison is skipped in silence. Remove the "
        f"'preprocessor_on_disk' entry — the CLI derives it from "
        f"'preprocessor' — or give it the same filepath."
    ]


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
    the value domains A25 checks, which are read, not copied: the strategies
    live here, the objectives in the metric registry, so the gate and the
    dispatcher read one table.) The agreement is
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


def missing_test_month_errors(parameters: dict) -> list[str]:
    """(A36) training needs at least one ``dataset.test_snap_dates`` month.

    Returns error strings (empty list when fine); the training command raises.

    With no test month, nothing in the training pipeline stops the run early:
    ``cache_test_model_input`` loops zero times and returns ``{}`` without a
    word, and the first thing to notice is ``predict_and_write_test_predictions``
    — after the whole HPO search and the final fit — failing inside
    ``open_parquet_dataset`` with a message about parquet roots that never
    names this key (#133).

    Absent, ``null`` and ``[]`` fail one check, because the downstream code
    reads all three as "no months" and the fix is the same. The message still
    says which it saw: "empty" would send someone whose yaml has no such line
    looking for one.

    Training-only on purpose. The dataset pipeline reads an absent or empty
    list as "no test months" and runs (``month_plans`` defaults it to ``[]``),
    and ``test_snap_dates`` is excluded from every version ID
    (``versioning.COVERAGE_ONLY_KEYS``), so a dataset built before any test
    month exists is legitimate.

    It blocks *every* training invocation, not only the ones that would reach
    a node reading test months: slices that never touch them (``--only-node
    compute_feature_importance``, say) are refused as well, and so are
    ``--list-nodes`` and ``--dry-run``, as A26 already does. That is the
    intent. The first half of an empty-list run does work — the Runner saves
    each output as its node finishes, so the model is on disk before the
    predict node fails — but ``docs/pipelines/training.md`` section 4.6 rules
    out using ``--only-node`` to build a model version that never completed,
    and a training run without test predictions never completes.
    """
    ds = parameters.get("dataset") or {}
    if ds.get("test_snap_dates"):
        return []
    state = "absent" if "test_snap_dates" not in ds else "empty"
    return [
        f"(A36) dataset.test_snap_dates is {state}, and training needs at "
        f"least one test month: it predicts on those months, scores the model "
        f"on them and computes its SHAP diagnostics from them. Without one "
        f"the run would only fail after the whole HPO search, with an error "
        f"that does not name this key. Add a month the dataset pipeline has "
        f"already built. (The dataset command runs without one; only training "
        f"needs it.)"
    ]



def scoring_snap_dates(parameters: Mapping) -> list[str]:
    """The scored months: the time values training scores the model on test
    over, as date texts in configured order (ADR-0028 decision 1).

    ``test_metrics.snap_date`` — one date, a list, or a ``{start, end, step}``
    range, read the way ``evaluation.snap_date`` is (``as_date_list``; the
    loader expands a range, ``core/date_ranges.DATE_LIST_KEYS``) — or, when
    that key is absent or null, the whole ``dataset.test_snap_dates``. The
    texts are compared with the prediction table's STRING time partition as
    they are, which is why A53 wants each one spelled as ``test_snap_dates``
    spells it. Nothing configured gives ``[]``; A36 and A53 stop the training
    command before that.
    """
    configured = (parameters.get("test_metrics") or {}).get("snap_date")
    if configured is None:
        configured = (parameters.get("dataset") or {}).get("test_snap_dates")
    return as_date_list(configured if configured is not None else [])


def scoring_param_errors(parameters: dict) -> list[str]:
    """(A53) the ``test_metrics`` block: scored months and metric names.

    Returns error strings (empty list when fine); the training command raises,
    collected with A26/A36, and ``scripts/promote_model.py`` runs the same
    three before ranking versions. Checked:

    * ``snap_date`` (the scored months): not an empty list — it would score
      nothing; each month in ``dataset.test_snap_dates``, spelled as it is
      there, since the prediction table only holds test months and is
      filtered by that text; and no month spelled two ways, A26's rule and
      key (``_test_month_key``), because at most one spelling can match the
      partition. The subset check is left to A36 when ``test_snap_dates`` is
      empty.
    * ``metrics`` a list of names and ``selection_metric`` a name, each in the
      metric registry (``evaluation/metric_registry.py``).
    * No key but the three above: a misspelled key would be ignored in
      silence, which A49 / A50 refuse for their blocks too. The list is spelled
      here, in the function S6 registers for it (R6), not as a module constant.

    A null value is "not set", for every key. NOT aggregated by
    ``validate_config_consistency``, for A24's reason (issue #158): no
    pipeline but training reads the block, and ``test_metrics`` is in no
    version ID.
    Nothing about whether a metric can be computed on test is decided here.
    """
    # Imported here, not at the top: core/ has no import-time dependency on
    # the layers above it, and evaluation/metrics_spark imports this module
    # (the A15 precedent for diagnosis/).
    from recsys_tfb.evaluation.metric_registry import METRIC_NAMES

    known_keys = ("snap_date", "metrics", "selection_metric")
    block = parameters.get("test_metrics")
    if block is None:
        return []
    if not isinstance(block, Mapping):
        return [
            f"(A53) test_metrics={block!r} must be a mapping with the keys "
            f"{list(known_keys)}."
        ]
    errors: list[str] = []
    unknown_keys = sorted(str(k) for k in block if k not in known_keys)
    if unknown_keys:
        errors.append(
            f"(A53) test_metrics declares {unknown_keys}, which nothing reads; "
            f"the keys are {list(known_keys)}. A misspelled key would "
            f"otherwise be ignored without a word."
        )

    if block.get("snap_date") is not None:
        errors.extend(_scored_month_errors(parameters))

    metrics = block.get("metrics")
    if metrics is not None:
        if not isinstance(metrics, list) or not all(
                isinstance(m, str) for m in metrics):
            errors.append(
                f"(A53) test_metrics.metrics={metrics!r} must be a list of "
                f"metric names; allowed: {', '.join(METRIC_NAMES)}."
            )
        else:
            unknown = [m for m in metrics if m not in METRIC_NAMES]
            if unknown:
                errors.append(
                    f"(A53) test_metrics.metrics names {unknown}, not in the "
                    f"metric registry; allowed: {', '.join(METRIC_NAMES)}."
                )

    selection = block.get("selection_metric")
    if selection is not None and selection not in METRIC_NAMES:
        errors.append(
            f"(A53) test_metrics.selection_metric={selection!r} is not in the "
            f"metric registry; allowed: {', '.join(METRIC_NAMES)}. Unset, it "
            f"follows training.hpo_objective."
        )
    return errors


def _scored_month_errors(parameters: Mapping) -> list[str]:
    """A53's half on ``test_metrics.snap_date``, when it is set."""
    months = scoring_snap_dates(parameters)
    if not months:
        return [
            "(A53) test_metrics.snap_date is empty, so training would score "
            "the model on no month. Name the months to score, or delete the "
            "key to score every dataset.test_snap_dates month."
        ]
    errors: list[str] = []
    spellings_by_month: dict[str, list[str]] = {}
    for month in months:
        spellings_by_month.setdefault(_test_month_key(month), []).append(month)
    for key in sorted(spellings_by_month):
        spellings = spellings_by_month[key]
        if len(spellings) > 1:
            errors.append(
                f"(A53) test_metrics.snap_date spells one month two ways: "
                f"{spellings} all resolve to {key!r}. The prediction table is "
                f"filtered by the text, so at most one of them can match. "
                f"Keep the one dataset.test_snap_dates uses."
            )
    test_months = as_date_list(
        (parameters.get("dataset") or {}).get("test_snap_dates") or [])
    if test_months:
        outside = [m for m in months if m not in test_months]
        if outside:
            errors.append(
                f"(A53) test_metrics.snap_date month(s) {outside} are not in "
                f"dataset.test_snap_dates {test_months}, spelled as there. "
                f"Only test months are predicted, and the prediction table is "
                f"filtered by that text: add them to dataset.test_snap_dates "
                f"(and run dataset for them), or score a subset of it."
            )
    return errors

def candidate_feature_table_inference_errors(declared: bool) -> list[str]:
    """(A47) offline inference is refused when a candidate-level feature table
    is declared.

    Returns error strings (empty list when fine); the inference command raises.
    Takes the fact rather than reading the catalog, so the predicate stays pure
    the way A40's does.

    The inference pipeline reads ``feature_table`` alone — it has no input for
    the candidate-level table — while a model trained under this configuration
    has that table's columns among its features. Left to run, it would not
    score wrongly: it would start Spark, read the population, and stop on
    ``Missing feature columns`` in ``build_inference_population_features``,
    naming neither the table nor the way out. This stops it first, and says
    why.

    Making inference read the table is not attempted (#380's out of scope). On
    impression data it could not help — the inference grid (each entity times
    every item) holds no candidate that was ever shown — but on a full-grid
    deployment whose candidate-level table is keyed ``(time, entity, item)`` it
    could, and refusing first keeps that option open (ADR-0022 decision 4).
    """
    if not declared:
        return []
    return [
        "(A47) offline inference cannot run while candidate_feature_table is "
        "declared in the catalog. The inference pipeline reads feature_table "
        "only, and a model trained under this configuration needs the "
        "candidate-level columns too: the run would start Spark and then fail "
        "on 'Missing feature columns'. Score this deployment with the system "
        "that computes those features online; training and evaluation "
        "--post-training are unaffected (ADR-0022 decision 4, ADR-0026)."
    ]


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

    # A counted item list is the preprocessor's (#379; A52 refuses products
    # next to it), so its emptiness is a runtime question
    # (predict_and_write_scores, through plan_scoring_chunks), not a config
    # one.
    if not item_list_counted_from_data(parameters) and not (inf.get("products") or []):
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
    groups on the full entity tuple (``evaluation/metrics_spark`` groups by
    ``query_group_columns``, which contains all of it), so the whole run's
    metrics silently answer a different question.

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
