"""分層抽樣 helper（SHAP 診斷使用）。"""

import numpy as np
import pandas as pd


def _stratified_item_sample(pdf, item_col, total, min_per_item, seed):
    """族群代表抽樣：依 item 分層，item 內純隨機；每 item 至少 min_per_item，
    不足全取（take-all）。回傳選中的 positional indices（對 pdf.iloc）。"""
    rng = np.random.RandomState(seed)
    groups = {item: np.where(pdf[item_col].values == item)[0]
              for item in pd.unique(pdf[item_col])}
    n_items = max(1, len(groups))
    per_item = max(int(min_per_item), total // n_items)
    selected = []
    for pos in groups.values():
        take = min(len(pos), per_item)
        selected.append(rng.choice(pos, size=take, replace=False))
    return np.sort(np.concatenate(selected)) if selected else np.array([], dtype=int)
