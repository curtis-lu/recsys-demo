"""Dataset building pipeline definition."""

from recsys_tfb.core.node import Node
from recsys_tfb.core.pipeline import Pipeline

#: The nodes ``--only-test-months`` keeps: the Layer-2 data gate plus the four
#: nodes on the test chain. Adding a ``test_snap_dates`` month cannot change
#: what any other node writes, so the other ten recompute bit-identical content
#: over the same partitions (ADR-0012's opening paragraph).
#:
#: Listed rather than derived from the DAG — ADR-0013 overturned the derived
#: design. The cost of listing is one drift test
#: (``test_the_list_matches_the_dag_derived_test_chain``); the cost of deriving
#: was a paragraph of producer-map reasoning to explain why the constant held
#: two names.
#:
#: ``fit_preprocessor_metadata`` is deliberately absent: ``preprocessor`` is a
#: landed JSON, so the two nodes that read it load it from the catalog. If it is
#: not there, they raise — which is the right answer, because at that point this
#: run is not "just adding an eval month" (ADR-0013 consequences).
ONLY_TEST_MONTHS_NODES = (
    "validate_data_consistency",
    "select_test_keys",
    "apply_preprocessor_to_features",
    "validate_numeric_precision",
    "build_test_model_input",
    "filter_test_model_input",
)


def _keep_named(nodes: list[Node], names: tuple[str, ...]) -> list[Node]:
    """``nodes`` filtered down to ``names``.

    Order is not this function's business and no caller should read one into
    the result: ``Pipeline.__init__`` topologically re-sorts whatever it is
    handed, so the order here never survives.

    Raises when a name is absent instead of returning whatever matched: a
    dataset run that writes nothing still exits 0, so a node renamed out from
    under this list would turn the mode into a silent no-op that reports
    success — the exact failure shape ADR-0012 opens with.

    Filtering rather than re-declaring the nodes is also what keeps the AST
    audit honest: ``test_static_coverage_floor`` and
    ``test_zero_output_nodes_match_registry`` count every ``Node(`` call in
    ``pipelines/**/*.py``, ``if`` branches included, so a second declaration of
    the data gate would make A7's registry hold two.
    """
    by_name = {node.name: node for node in nodes}
    missing = [name for name in names if name not in by_name]
    if missing:
        raise ValueError(
            f"--only-test-months names node(s) this pipeline does not have: "
            f"{', '.join(missing)}. Available: {', '.join(sorted(by_name))}. "
            f"A node was renamed — update ONLY_TEST_MONTHS_NODES in "
            f"{__name__}."
        )
    wanted = set(names)
    return [node for node in nodes if node.name in wanted]


def create_pipeline(
    enable_calibration: bool = False, only_test_months: bool = False
) -> Pipeline:
    """Build the dataset pipeline.

    Modes:
      * default — the full DAG (13 nodes, or 15 with calibration).
      * ``--only-test-months`` — the data gate plus the test chain, for a run
        that only adds ``test_snap_dates`` months. See
        :data:`ONLY_TEST_MONTHS_NODES`.

    A mode decides *which line of work* this run does; ``--from-node`` /
    ``--only-node`` decide *where it resumes*. They are orthogonal and compose:
    the CLI slices whatever the mode built (ADR-0013).
    """
    from recsys_tfb.pipelines.dataset.nodes import (
        apply_preprocessor_to_features,
        build_model_input,
        build_test_model_input,
        filter_groups_with_positives,
        fit_preprocessor_metadata,
        select_calibration_keys,
        select_test_keys,
        select_train_keys,
        select_val_keys,
        split_train_keys,
        validate_data_consistency,
        validate_numeric_precision,
    )

    nodes = [
        # --- Layer-2 data gate (B1 item coverage + B5 categorical dtype
        #     + B6 non-numeric feature column + B7 carry/feature collision):
        # runs first (insertion-order Kahn seed), side-effect only
        # (outputs=None), fail-fast before any sampling / preprocessing ---
        Node(
            validate_data_consistency,
            inputs=["sample_pool", "label_table", "feature_table", "parameters"],
            outputs=None,
            name="validate_data_consistency",
        ),
        # --- Key selection ---
        Node(
            select_train_keys,
            inputs=["sample_pool", "parameters"],
            outputs="sample_keys",
            name="select_sample_keys",
        ),
        Node(
            split_train_keys,
            inputs=["sample_keys", "parameters"],
            outputs=["train_keys", "train_dev_keys"],
        ),
        Node(
            select_val_keys,
            inputs=["sample_pool", "parameters"],
            outputs="val_keys",
        ),
        # A `*_month_plan` input marks an incremental node: it processes only
        # the months in that plan (ADR-0002/0007). The plans are built once by
        # the caller and injected into the catalog, so nodes that share an
        # artifact cannot disagree about which months this run covers — and the
        # nodes that have no plan are exactly the ones that rebuild in full.
        Node(
            select_test_keys,
            inputs=["sample_pool", "test_keys_month_plan", "parameters"],
            outputs="test_keys",
        ),
        # --- Fit preprocessor on train date-range feature_table, decoupled from sampling ---
        Node(
            fit_preprocessor_metadata,
            inputs=["feature_table", "parameters"],
            outputs=["preprocessor", "category_mappings"],
            name="fit_preprocessor_metadata",
        ),
        # --- Encode non-identity categoricals once; all splits reuse this ---
        Node(
            apply_preprocessor_to_features,
            inputs=[
                "feature_table", "preprocessor",
                "preprocessed_feature_table_month_plan", "parameters",
            ],
            outputs="preprocessed_feature_table",
            name="apply_preprocessor_to_features",
        ),
        # --- B8 precision gate: the months just encoded must survive the
        #     declared numeric storage type. Side-effect only (outputs=None),
        #     and declared HERE rather than anywhere later in this list because
        #     list position is what orders it: it and the build_model_input
        #     nodes below become runnable at the same moment (they share
        #     `preprocessed_feature_table` as their last unmet input), and Kahn
        #     queues them in declaration order (`core/pipeline.py`). Moving this
        #     entry below them would let a narrowed value land before the gate
        #     that exists to stop it ---
        Node(
            validate_numeric_precision,
            inputs=[
                "preprocessed_feature_table", "preprocessor",
                "preprocessed_feature_table_month_plan", "parameters",
            ],
            outputs=None,
            name="validate_numeric_precision",
        ),
        # --- Build model_input per split (join keys + labels + encoded features) ---
        Node(
            build_model_input,
            inputs=[
                "train_keys", "preprocessed_feature_table", "label_table",
                "preprocessor", "parameters",
            ],
            outputs="train_model_input",
            name="build_train_model_input",
        ),
        Node(
            build_model_input,
            inputs=[
                "train_dev_keys", "preprocessed_feature_table", "label_table",
                "preprocessor", "parameters",
            ],
            outputs="train_dev_model_input",
            name="build_train_dev_model_input",
        ),
        Node(
            build_model_input,
            inputs=[
                "val_keys", "preprocessed_feature_table", "label_table",
                "preprocessor", "parameters",
            ],
            outputs="val_model_input_unfiltered",
            name="build_val_model_input",
        ),
        # test uses its own wrapper: `test_keys` is a persistent Hive table
        # holding every month, so reading it back has to be re-scoped to this
        # run's months. train/val/calibration read keys written by this run and
        # need no such wrapper.
        Node(
            build_test_model_input,
            inputs=[
                "test_keys", "preprocessed_feature_table", "label_table",
                "preprocessor", "test_model_input_month_plan", "parameters",
            ],
            outputs="test_model_input_unfiltered",
            name="build_test_model_input",
        ),
        # --- Drop (time, *entity) groups with no positives. Applied to val/test
        # only — these are evaluated by ranking metrics (mAP/NDCG) that exclude
        # zero-positive groups anyway, so retaining them just wastes Hive
        # storage and downstream predict / extract memory. train / train_dev /
        # calibration are NOT filtered: their losses use every row. ---
        Node(
            filter_groups_with_positives,
            inputs=["val_model_input_unfiltered", "parameters"],
            outputs="val_model_input",
            name="filter_val_model_input",
        ),
        # No month plan here: its input is produced by build_test_model_input,
        # which is already scoped. Re-filtering would only re-state the line
        # above. See ADR-0007 for the slicing scenario that was considered.
        Node(
            filter_groups_with_positives,
            inputs=["test_model_input_unfiltered", "parameters"],
            outputs="test_model_input",
            name="filter_test_model_input",
        ),
    ]

    if enable_calibration:
        nodes.extend([
            Node(
                select_calibration_keys,
                inputs=["sample_pool", "parameters"],
                outputs="calibration_keys",
            ),
            Node(
                build_model_input,
                inputs=[
                    "calibration_keys", "preprocessed_feature_table", "label_table",
                    "preprocessor", "parameters",
                ],
                outputs="calibration_model_input",
                name="build_calibration_model_input",
            ),
        ])

    # After the calibration branch, so the mode is decided against the full
    # node list however it was built. Neither calibration node is on the test
    # chain, so the two flags do not interact — pinned by a test rather than
    # left to be re-derived.
    if only_test_months:
        nodes = _keep_named(nodes, ONLY_TEST_MONTHS_NODES)

    return Pipeline(nodes)
