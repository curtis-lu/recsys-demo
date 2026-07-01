"""分層抽樣 helper（SHAP 診斷使用）。"""

import numpy as np
import pandas as pd


def _stratified_item_sample(item_values, total, min_per_item, seed):
    """族群代表抽樣：依 item 分層，item 內純隨機；每 item 至少 min_per_item，
    不足全取（take-all）。回傳選中的 positional indices（升序，對齊 dataset 順序）。

    ``item_values`` 是每列的 item 值（1-D array-like，dataset 順序）。行為與過去
    吃整個 pdf 的版本一致：``pd.unique`` 決定 item 順序、``np.where`` 給每 item 的
    升序位置、``rng.choice`` 以固定 seed 抽樣。
    """
    item_values = np.asarray(item_values)
    rng = np.random.RandomState(seed)
    groups = {item: np.where(item_values == item)[0]
              for item in pd.unique(item_values)}
    n_items = max(1, len(groups))
    per_item = max(int(min_per_item), total // n_items)
    selected = []
    for pos in groups.values():
        take = min(len(pos), per_item)
        selected.append(rng.choice(pos, size=take, replace=False))
    return np.sort(np.concatenate(selected)) if selected else np.array([], dtype=int)
