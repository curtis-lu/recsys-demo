"""The structural gain ledger: the model's trees, accounted per item across
trees (the full-set traversal variant).

Every tree is walked from its root and its split gain goes into two accounts:
the gain of item-id splits (what isolating by item costs), and the gain of the
context splits taken once an item-id split has "conditioned" the path (how
much gain the model spends refining its discrimination after the item is
isolated). "Almost no context gain after the item is isolated" only says the
model bought no personalisation gain on that item; it does not say why. The
item may have been starved of capacity, or the data may hold no feature that
separates its positives from its negatives. The two look the same in the
ledger, and the ledger cannot tell them apart.

Three more global outputs, for the ``model_capacity`` presentation layer
(they do not touch the two accounts above): ``total_split_count`` (non-leaf
nodes in total, so the three-way split share can compute the unallocated
residual = total − item − context); ``pre_item`` (the unconditioned splits
taken **before** any item split, broken down by feature; their gain sums to
exactly the unallocated residual ``total_gain − item_id_gain −
context_gain``); ``first_item_split_depth`` (a quantile summary of each
tree's shallowest item split's ``node_depth``, root = 1: how deep the item
conditioning sits). The coarse ledger has ``total_split_count`` only;
``pre_item`` and ``first_item_split_depth`` are ``None`` there, because both
need the reachable-set traversal.

Two layers, for testability: ``ledger_from_trees`` is the pure pandas/dict
core — it takes only ``ModelAdapter.tree_structure()``'s table and touches
neither the model nor the preprocessor, so unit tests feed it hand-built
frames — and ``compute_gain_ledger`` (``nodes.py``) is the node, which reads
the config and schema, asks the adapter for the trees, and decides between
this core and ``coarse_ledger``.

The tree table is algorithm-neutral (``models/base.py``'s
``TREE_STRUCTURE_COLUMNS``): a categorical split arrives as the tuple of
category codes sent left, already decoded from LightGBM's ``"2||3||4"`` by the
adapter (ADR-0030 decision 1). A model with no trees raises
``UnsupportedCapability``; the ledger is then skipped and lands as the
"model cannot" shape (``steps/diagnosis_artifacts.unsupported_artifact``),
which evaluation's ``model_capacity`` reports as its own reason (decision 4).

The ``notes`` strings are part of ``gain_ledger.json`` and are kept word for
word; so is the log line.
"""

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def _total_gain(trees: pd.DataFrame) -> float:
    """Sum of every split's ``split_gain`` (a leaf's NaN counts as 0, a
    negative gain is clipped to 0)."""
    return float(
        pd.to_numeric(trees["split_gain"], errors="coerce").fillna(0).clip(lower=0).sum()
    )


def _tree_index_summary(tree_indices: list) -> dict:
    """Quantile summary of the item splits' ``tree_index`` (min/max as int,
    p25/p50/p75 as float); all ``None`` for an empty list."""
    if not tree_indices:
        return {"min": None, "p25": None, "p50": None, "p75": None, "max": None}
    arr = np.asarray(sorted(tree_indices), dtype=float)
    return {
        "min": int(arr.min()),
        "p25": float(np.percentile(arr, 25)),
        "p50": float(np.percentile(arr, 50)),
        "p75": float(np.percentile(arr, 75)),
        "max": int(arr.max()),
    }


def _depth_summary(depths: list) -> dict:
    """Quantile summary of each tree's shallowest item-split depth
    (``node_depth``, root = 1); all ``None`` for an empty list.

    ``n_trees_with_item_split`` is how many trees these depths come from (the
    trees with at least one item split), so a reader knows how many trees the
    quantiles are over — possibly fewer than the model has, since a tree can
    hold no item split at all.
    """
    if not depths:
        return {"min": None, "p25": None, "p50": None, "p75": None,
                "max": None, "n_trees_with_item_split": 0}
    arr = np.asarray(sorted(depths), dtype=float)
    return {
        "min": int(arr.min()),
        "p25": float(np.percentile(arr, 25)),
        "p50": float(np.percentile(arr, 50)),
        "p75": float(np.percentile(arr, 75)),
        "max": int(arr.max()),
        "n_trees_with_item_split": int(len(depths)),
    }


def _item_id_block(trees: pd.DataFrame, item_feature: str, total_gain: float) -> dict:
    """The item-id account: a plain filter-and-sum over the splits with
    ``split_feature == item_feature``.

    Deliberately independent of the traversal and of the reachable sets: an
    item split's own gain counts in this account whichever branch it sits
    under, even one an upstream condition has made impossible to reach. That
    is what lets the coarse ledger reuse this code with no categories at all.
    """
    item_rows = trees[trees["split_feature"] == item_feature]
    gain_sum = float(
        pd.to_numeric(item_rows["split_gain"], errors="coerce").fillna(0).clip(lower=0).sum()
    )
    gain_share = (gain_sum / total_gain) if total_gain > 0 else None
    tree_indices = [int(t) for t in item_rows["tree_index"].tolist()]
    return {
        "split_count": int(len(item_rows)),
        "gain_sum": gain_sum,
        "gain_share": gain_share,
        "tree_index_summary": _tree_index_summary(tree_indices),
    }


def _decode_codes(codes, categories: list) -> tuple:
    """Category codes sent left -> (their item values, the codes outside
    ``categories`` as strings).

    A code is an index into ``categories``. The out-of-range ones are kept as
    strings because that is how the note has always printed them.
    """
    values: set = set()
    unknown: set = set()
    for code in codes:
        if 0 <= code < len(categories):
            values.add(categories[code])
        else:
            unknown.add(str(code))
    return values, unknown


def _is_categorical_split(codes) -> bool:
    """``categories_left`` holds the codes for a categorical split and
    ``None`` otherwise; a frame round-trip may turn that ``None`` into NaN."""
    return isinstance(codes, (tuple, list))


def ledger_from_trees(trees: pd.DataFrame, item_feature: str, categories: list) -> dict:
    """The pure pandas/dict core: the ledger, from
    ``ModelAdapter.tree_structure()``'s table.

    Each tree is walked from its root (an iterative stack), carrying
    ``reachable`` (the item values that can reach the node; the whole item set
    at the root) and ``conditioned`` (whether the path has passed at least one
    item split):

    - **An item split** (``split_feature == item_feature``): the codes in
      ``categories_left`` map through ``categories[code]`` to a set S of item
      values; the left child carries ``reachable ∩ S`` and the right child
      ``reachable - S``, both with ``conditioned=True``. Its gain goes into the
      item-id account (see ``_item_id_block``, which ignores the traversal);
      every item in the reachable set it was entered with gets its
      ``isolating_split_count`` / ``trees_touched`` / ``first_tree_index``
      recorded. A code in ``categories_left`` outside ``categories`` is ignored
      and summarised in one note, without raising.
    - **A context split** (any other feature): when ``conditioned``, every item
      in ``reachable`` gets ``context_split_count`` / ``context_gain`` (and
      ``context_gain_isolated`` when ``len(reachable) == 1``), and the global
      context account's ``split_count`` / ``gain_sum`` grows once — once, not
      once per reachable item, so a split is never counted by the number of
      items. A global split that is not yet conditioned (common near the root)
      goes into none of those accounts; it is the ``pre_item`` breakdown.
    - A node whose own ``reachable`` is empty still settles its own accounts
      (the item-id account ignores the traversal anyway; the context account's
      per-item loop is a no-op over an empty set, so nothing extra is counted)
      but its children are not walked: no split below it can reach any item.
    """
    n_trees = int(trees["tree_index"].nunique())
    n_items = len(categories)
    all_items = list(dict.fromkeys(categories))

    total_gain = _total_gain(trees)
    item_block = _item_id_block(trees, item_feature, total_gain)

    isolating_split_count = {it: 0 for it in all_items}
    context_split_count = {it: 0 for it in all_items}
    context_gain = {it: 0.0 for it in all_items}
    context_gain_isolated = {it: 0.0 for it in all_items}
    context_split_isolated = {it: 0 for it in all_items}
    trees_touched: dict = {it: set() for it in all_items}
    first_tree_index: dict = {it: None for it in all_items}

    context_global_split_count = 0
    context_global_gain_sum = 0.0
    unknown_codes: set = set()
    numeric_item_splits = 0

    # Unallocated (pre-item) = the non-item splits taken before any item split
    # (``conditioned=False``): global context the model uses before it has
    # conditioned on which item. Accounted by feature, so ``model_capacity``
    # can break down which features hold up the unallocated share. These
    # splits go into none of the other accounts; their gain sums to exactly
    # ``total_gain − item_id_gain − context_gain`` (the unallocated residual).
    pre_item_gain_by_feat: dict = {}
    pre_item_split_by_feat: dict = {}
    pre_item_gain_sum = 0.0
    pre_item_split_count = 0
    # Each tree's shallowest item split's depth (node_depth, root = 1): how
    # deep the item conditioning sits, i.e. how much global context sits
    # above it. Only trees with an item split contribute.
    first_item_split_depths: list = []

    for _, tdf in trees.groupby("tree_index"):
        tdf = tdf.set_index("node_index")
        roots = tdf.index[tdf["parent_index"].isna()]
        if len(roots) == 0:
            continue  # Defensive: a malformed tree (no root) is skipped, not raised on.
        tree_item_depths: list = []
        stack = [(roots[0], set(all_items), False)]
        while stack:
            node, reachable, conditioned = stack.pop()
            row = tdf.loc[node]
            feat = row["split_feature"]
            if not isinstance(feat, str):
                continue  # A leaf: split_feature is not a string (NaN).
            gain = row["split_gain"]
            gain = 0.0 if pd.isna(gain) else float(gain)
            t_idx = int(row["tree_index"])

            if feat == item_feature:
                if not _is_categorical_split(row["categories_left"]):
                    # A non-categorical split on the item column (the column
                    # may not be declared categorical): no codes to decode,
                    # reachable left as it is, no per-item accounting — only
                    # counted as an anomaly (a guard, review fix 2026-07-08).
                    # A numeric split has no categories_left, so there is no
                    # set of items to send left.
                    numeric_item_splits += 1
                    stack.append((row["left_child"], reachable, conditioned))
                    stack.append((row["right_child"], reachable, conditioned))
                    continue
                tree_item_depths.append(int(row["node_depth"]))
                for it in reachable:
                    isolating_split_count[it] += 1
                    trees_touched[it].add(t_idx)
                    if first_tree_index[it] is None or t_idx < first_tree_index[it]:
                        first_tree_index[it] = t_idx
                if not reachable:
                    continue
                values, unknown = _decode_codes(row["categories_left"], categories)
                unknown_codes |= unknown
                stack.append((row["left_child"], reachable & values, True))
                stack.append((row["right_child"], reachable - values, True))
            else:
                if conditioned:
                    context_global_split_count += 1
                    context_global_gain_sum += gain
                    for it in reachable:
                        context_split_count[it] += 1
                        context_gain[it] += gain
                        trees_touched[it].add(t_idx)
                        if len(reachable) == 1:
                            context_gain_isolated[it] += gain
                            context_split_isolated[it] += 1
                else:
                    # Pre-item: a global context split taken before item
                    # conditioning, accounted by feature.
                    pre_item_gain_by_feat[feat] = (
                        pre_item_gain_by_feat.get(feat, 0.0) + gain
                    )
                    pre_item_split_by_feat[feat] = (
                        pre_item_split_by_feat.get(feat, 0) + 1
                    )
                    pre_item_gain_sum += gain
                    pre_item_split_count += 1
                if not reachable:
                    continue
                stack.append((row["left_child"], reachable, conditioned))
                stack.append((row["right_child"], reachable, conditioned))
        if tree_item_depths:
            first_item_split_depths.append(min(tree_item_depths))

    notes = []
    if unknown_codes:
        notes.append(
            "item 切點的類別碼超出 categories 範圍(已忽略): "
            f"{sorted(unknown_codes)}"
        )
    if numeric_item_splits:
        notes.append(
            f"item 欄出現 {numeric_item_splits} 筆非類別切點（沒有 categories_left）"
            "——該欄可能未宣告 categorical；這些切點不參與 per-item 帳"
            "（item_id 帳按特徵名仍納入）"
        )

    sum_context_gain = sum(context_gain.values())
    per_item = {}
    for it in sorted(all_items):
        cg = context_gain[it]
        share = (cg / sum_context_gain) if sum_context_gain > 0 else None
        per_item[it] = {
            "isolating_split_count": isolating_split_count[it],
            "context_split_count": context_split_count[it],
            "context_gain": cg,
            "context_gain_isolated": context_gain_isolated[it],
            "context_split_isolated": context_split_isolated[it],
            "context_gain_share": share,
            "first_tree_index": first_tree_index[it],
            "trees_touched": sorted(trees_touched[it]),
        }

    context_gain_share = (
        (context_global_gain_sum / total_gain) if total_gain > 0 else None
    )

    # Pre-item (unallocated) by feature, gain descending: the global feature
    # that eats the most unallocated gain is what a reader wants first.
    pre_item_by_feature = {
        f: {
            "gain": pre_item_gain_by_feat[f],
            "split_count": pre_item_split_by_feat[f],
        }
        for f in sorted(pre_item_gain_by_feat,
                        key=lambda k: pre_item_gain_by_feat[k], reverse=True)
    }
    total_split_count = int(trees["split_feature"].notna().sum())

    logger.info(
        "gain_ledger: n_trees=%d n_items=%d item_id.split_count=%d context.split_count=%d",
        n_trees, n_items, item_block["split_count"], context_global_split_count,
    )

    return {
        "enabled": True,
        "item_feature": item_feature,
        "n_trees": n_trees,
        "n_items": n_items,
        "total_gain": total_gain,
        "total_split_count": total_split_count,
        "item_id": item_block,
        "context": {
            "split_count": context_global_split_count,
            "gain_sum": context_global_gain_sum,
            "gain_share": context_gain_share,
        },
        "pre_item": {
            "gain_sum": pre_item_gain_sum,
            "split_count": pre_item_split_count,
            "by_feature": pre_item_by_feature,
        },
        "first_item_split_depth": _depth_summary(first_item_split_depths),
        "per_item": per_item,
        "fallback": False,
        "notes": notes,
    }


def coarse_ledger(trees: pd.DataFrame, item_feature: str, n_trees: int) -> dict:
    """The coarse ledger: only the item-id account (a filter by feature, no
    traversal, no categories needed). ``context`` and ``per_item`` are absent
    (``None``) and ``fallback`` is ``True``.
    """
    total_gain = _total_gain(trees)
    item_block = _item_id_block(trees, item_feature, total_gain)
    return {
        "enabled": True,
        "item_feature": item_feature,
        "n_trees": n_trees,
        "n_items": None,
        "total_gain": total_gain,
        # The total split count only needs the non-leaf nodes counted, not the
        # reachable-set traversal, so the coarse ledger can give it too.
        "total_split_count": int(trees["split_feature"].notna().sum()),
        "item_id": item_block,
        "context": None,
        # The pre-item breakdown and the item-split depth both need the
        # reachable-set traversal, which the coarse ledger cannot do -> None.
        "pre_item": None,
        "first_item_split_depth": None,
        "per_item": None,
        "fallback": True,
        "notes": [
            "preprocessor 缺 category_mappings[item_col]，降級為粗帳本"
            "（無法拆解 per-item context/isolating 帳，也沒有 pre-item 拆解"
            "與 item 切點深度）"
        ],
    }
