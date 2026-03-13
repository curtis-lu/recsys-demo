import logging
import time

logger = logging.getLogger(__name__)


class Runner:
    """Execute pipeline nodes sequentially using a DataCatalog."""

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

        pipeline_start = time.time()

        for node in pipeline.nodes:
            logger.info("Running node: %s", node.name)
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

            except Exception:
                logger.error("Node '%s' failed", node.name)
                raise

            duration = time.time() - node_start
            logger.info("Node %s completed in %.2fs", node.name, duration)

        total = time.time() - pipeline_start
        logger.info("Pipeline completed in %.2fs", total)
