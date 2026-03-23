import logging
import time

from recsys_tfb.core.catalog import MemoryDataset

logger = logging.getLogger(__name__)


class Runner:
    """Execute pipeline nodes sequentially using a DataCatalog."""

    @staticmethod
    def _build_last_consumer_map(nodes: list) -> dict[str, object]:
        """Map each dataset name to the last node that uses it as input.

        Since *nodes* are in topological order, iterating forward means
        the last assignment wins — which is exactly the last consumer.
        """
        last_consumer: dict[str, object] = {}
        for node in nodes:
            for inp in node.inputs:
                last_consumer[inp] = node
        return last_consumer

    def run(self, pipeline, catalog) -> None:
        """Execute all nodes in the pipeline in topological order."""
        # Validate: check all pipeline inputs are available
        available = set()
        for name in list(catalog.list()):
            available.add(name)
        for node in pipeline.nodes:
            available.update(node.outputs)

        for node in pipeline.nodes:
            for inp in node.inputs:
                if inp not in available and not catalog.exists(inp):
                    raise ValueError(
                        f"Node '{node.name}' requires input '{inp}' "
                        f"which is not in the catalog and not produced by any prior node"
                    )

        node_count = len(pipeline.nodes)
        pipeline_start = time.time()
        last_consumer = self._build_last_consumer_map(pipeline.nodes)

        # Identify true intermediates: produced AND consumed within this pipeline
        # Terminal outputs (produced but not consumed) are kept for cross-pipeline use
        produced = set()
        consumed = set()
        for node in pipeline.nodes:
            produced.update(node.outputs)
            consumed.update(node.inputs)
        intermediates = produced & consumed

        logger.info(
            "Pipeline started (%d nodes)", node_count,
            extra={"event": "pipeline_started", "node_count": node_count},
        )

        for node in pipeline.nodes:
            logger.info(
                "Running node: %s", node.name,
                extra={
                    "event": "node_started",
                    "node": node.name,
                    "input_names": list(node.inputs),
                    "output_names": list(node.outputs),
                },
            )
            node_start = time.time()

            try:
                # Load inputs
                inputs = [catalog.load(name) for name in node.inputs]

                # Execute
                result = node.func(*inputs)

                # Save outputs
                if len(node.outputs) == 1:
                    catalog.save(node.outputs[0], result)
                elif len(node.outputs) > 1:
                    for name, value in zip(node.outputs, result):
                        catalog.save(name, value)

            except Exception as exc:
                duration = time.time() - node_start
                logger.error(
                    "Node '%s' failed after %.2fs: %s",
                    node.name, duration, exc,
                    extra={
                        "event": "node_failed",
                        "node": node.name,
                        "duration_seconds": round(duration, 3),
                        "status": "failed",
                        "error_message": str(exc),
                        "exception_type": type(exc).__name__,
                        "input_names": list(node.inputs),
                        "output_names": list(node.outputs),
                    },
                )

                # Emit pipeline failed
                total = time.time() - pipeline_start
                logger.error(
                    "Pipeline failed after %.2fs", total,
                    extra={
                        "event": "pipeline_failed",
                        "duration_seconds": round(total, 3),
                        "status": "failed",
                        "node_count": node_count,
                        "error_message": str(exc),
                        "exception_type": type(exc).__name__,
                    },
                )
                raise

            duration = time.time() - node_start
            logger.info(
                "Node %s completed in %.2fs", node.name, duration,
                extra={
                    "event": "node_completed",
                    "node": node.name,
                    "duration_seconds": round(duration, 3),
                    "status": "success",
                    "input_names": list(node.inputs),
                    "output_names": list(node.outputs),
                },
            )

            # Release MemoryDatasets no longer needed by downstream nodes
            # Only release true intermediates (produced AND consumed within this
            # pipeline) that were auto-created. External inputs and terminal outputs
            # are preserved for cross-pipeline use.
            for inp in node.inputs:
                if (last_consumer.get(inp) is node
                        and inp in intermediates
                        and inp in catalog._auto_created):
                    ds = catalog.get_dataset(inp)
                    if ds is not None and isinstance(ds, MemoryDataset):
                        ds.release()
                        logger.info(
                            "Released dataset: %s", inp,
                            extra={
                                "event": "dataset_released",
                                "dataset_name": inp,
                                "node": node.name,
                            },
                        )

        total = time.time() - pipeline_start
        logger.info(
            "Pipeline completed in %.2fs", total,
            extra={
                "event": "pipeline_completed",
                "duration_seconds": round(total, 3),
                "status": "success",
                "node_count": node_count,
            },
        )
