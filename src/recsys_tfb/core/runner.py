import logging
import time

from recsys_tfb.core.catalog import MemoryDataset
from recsys_tfb.core.logging import get_current_context

logger = logging.getLogger(__name__)


def _phase_durations(node_start, load_end, func_end, now) -> dict:
    """Seconds spent per phase, for the phases that were actually reached.

    A phase the node never got to is **absent**, not zero: on a node that died
    inside its function, a ``save_seconds: 0.0`` would read as "the save was
    instant" when the save never ran at all — and telling those two apart is
    the whole point of the split.
    """
    if load_end is None:
        return {"load_seconds": round(now - node_start, 3)}
    if func_end is None:
        return {
            "load_seconds": round(load_end - node_start, 3),
            "func_seconds": round(now - load_end, 3),
        }
    return {
        "load_seconds": round(load_end - node_start, 3),
        "func_seconds": round(func_end - load_end, 3),
        "save_seconds": round(now - func_end, 3),
    }


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
                name = inp[1:] if inp.startswith("@") else inp
                last_consumer[name] = node
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
                name = inp[1:] if inp.startswith("@") else inp
                if name not in available and not catalog.exists(name):
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
            consumed.update(
                inp[1:] if inp.startswith("@") else inp for inp in node.inputs
            )
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
            # Phase boundaries. With a lazy engine the node function returns a
            # plan in milliseconds and the whole computation runs inside
            # catalog.save(), so a single node duration cannot say which of the
            # two is slow. These two marks are what separate them; None means
            # "not reached", which is what the failure path reports.
            load_end = func_end = None
            _ctx = get_current_context()

            try:
                # Load inputs. An input name starting with '@' is resolved to
                # the catalog dataset HANDLE (not loaded data), used by nodes
                # that need to call .save() on the dataset themselves.
                inputs = []
                for name in node.inputs:
                    if name.startswith("@"):
                        inputs.append(catalog.get_dataset(name[1:]))
                    else:
                        inputs.append(catalog.load(name))
                load_end = time.time()

                # Execute
                if _ctx is not None:
                    _ctx.current_node = node.name
                result = node.func(*inputs)
                func_end = time.time()

                # Save outputs
                if len(node.outputs) == 1:
                    catalog.save(node.outputs[0], result)
                elif len(node.outputs) > 1:
                    for name, value in zip(node.outputs, result):
                        catalog.save(name, value)

            except Exception as exc:
                if _ctx is not None:
                    _ctx.current_node = ""
                failed_at = time.time()
                duration = failed_at - node_start
                logger.error(
                    "Node '%s' failed after %.2fs: %s",
                    node.name, duration, exc,
                    exc_info=True,
                    extra={
                        "event": "node_failed",
                        "node": node.name,
                        "duration_seconds": round(duration, 3),
                        # Which phase it died in, and how long it survived
                        # there. A node killed by an OOM during the write is
                        # the case this feature exists to locate, so the
                        # failure path carries the split too.
                        **_phase_durations(
                            node_start, load_end, func_end, failed_at,
                        ),
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

            if _ctx is not None:
                _ctx.current_node = ""
            node_end = time.time()
            duration = node_end - node_start
            phases = _phase_durations(node_start, load_end, func_end, node_end)
            logger.info(
                "Node %s completed in %.2fs (load %.2fs, func %.2fs, save %.2fs)",
                node.name, duration, phases["load_seconds"],
                phases["func_seconds"], phases["save_seconds"],
                extra={
                    "event": "node_completed",
                    "node": node.name,
                    "duration_seconds": round(duration, 3),
                    **phases,
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
                name = inp[1:] if inp.startswith("@") else inp
                if (last_consumer.get(name) is node
                        and name in intermediates
                        and name in catalog._auto_created):
                    ds = catalog.get_dataset(name)
                    if ds is not None and isinstance(ds, MemoryDataset):
                        ds.release()
                        logger.info(
                            "Released dataset: %s", name,
                            extra={
                                "event": "dataset_released",
                                "dataset_name": name,
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
