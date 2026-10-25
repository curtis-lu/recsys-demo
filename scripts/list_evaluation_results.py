"""List the evaluation pipeline's landed results: who produces each, who reads
it, and the blocks it holds (ADR-0031 principle 2, supplement 2).

Plain text, no Spark. Nothing here is hand-written: the producers and node
readers come from ``create_pipeline``'s wiring, the section readers from each
report section's ``READS``, the blocks from the ``Produces`` declared above
the nodes in ``pipelines/evaluation/nodes.py``
(``pipelines.evaluation.pipeline.result_readers_table``).

Run: ``PYTHONPATH=<root>/src python scripts/list_evaluation_results.py``
"""

from recsys_tfb.pipelines.evaluation.pipeline import result_readers_table


def main() -> None:
    for result, entry in result_readers_table().items():
        print(result)
        print(f"  produced by: {', '.join(entry['producers']) or '-'}")
        print(f"  read by nodes: {', '.join(entry['reader_nodes']) or '-'}")
        sections = [
            f"{report}:{section}" + (f" (switch {switch})" if switch else "")
            for report, section, switch in entry["reader_sections"]]
        print(f"  read by report sections: {', '.join(sections) or '-'}")
        print("  blocks:")
        for block in entry["blocks"]:
            when = f" [only when: {block.when}]" if block.when else ""
            print(f"    {block.name}  ({block.grain}){when}")
            print(f"      {block.definition}")
        print()


if __name__ == "__main__":
    main()
