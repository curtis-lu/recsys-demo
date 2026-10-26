"""The log lines of the all-positive query group accounts. They are counted
into the row facts (``metrics.all_positive_accounts``); this module only says
them in the log.
"""

import logging

from recsys_tfb.evaluation.result_readers import (
    ALL_POSITIVE_KEY,
    ALL_POSITIVE_SHARE_KEY,
    ALL_POSITIVE_WARNS_KEY,
    N_WITH_POSITIVE_KEY,
    format_share,
)


def log_all_positive_accounts(
    caller_logger: logging.Logger, facts: dict, drop_all_positive: bool,
) -> None:
    """One info line per grain of ``facts`` (fine, and category when the
    facts hold it), and a warning per grain whose landed accounts say the
    all-positive share is high with the switch off.

    Read off the landed accounts, so the log and the report cannot disagree.
    Each grain decides on its own frame, so the two grains can disagree.
    ``caller_logger`` is the node's: the lines keep the logger name they are
    filtered by.
    """
    for grain, bundle in (("fine", facts),
                          ("category", facts.get("category"))):
        if bundle is None:
            continue
        n_with_positive = bundle[N_WITH_POSITIVE_KEY]
        n_all_positive = bundle[ALL_POSITIVE_KEY]
        share = bundle[ALL_POSITIVE_SHARE_KEY]
        share_text = "n/a" if share is None else format_share(share)
        caller_logger.info(
            "Row facts counted (%s grain): n_queries=%d, n_excluded=%d "
            "(no positive); %d hold a positive, of which %d all-positive "
            "(%s) — %s",
            grain, bundle["n_queries"], bundle["n_excluded_queries"],
            n_with_positive, n_all_positive, share_text,
            (f"dropped (evaluation.query_filter.drop_all_positive_groups "
             f"on), {n_with_positive - n_all_positive} scored")
            if drop_all_positive else
            ("scored like the others (evaluation.query_filter."
             "drop_all_positive_groups off)"),
        )
        if bundle[ALL_POSITIVE_WARNS_KEY]:
            caller_logger.warning(
                "All-positive query groups at the %s grain: %d of the %d "
                "holding a positive (%s). Every row carries the same positive "
                "label, so every per-query metric of such a group is the same "
                "whatever the order, and they count in the metrics as they "
                "are. Set evaluation.query_filter.drop_all_positive_groups: "
                "true to drop them from the metrics and the baseline.",
                grain, n_all_positive, n_with_positive, share_text,
            )
