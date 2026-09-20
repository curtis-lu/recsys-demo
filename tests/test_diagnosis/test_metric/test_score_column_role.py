"""三個診斷模組讀的是 schema 的 score 角色欄，而且數值與改讀之前逐值相同（#415）。

本檔守的是**兩件不同的事**，缺一條都會讓另一條變成假綠：

1. **值沒變。** 下面的 golden 數字是在 #415 改動**之前**的程式上量到的——當時
   模組寫死讀 ``score_uncalibrated``，餵它同一批數字，得到的就是這些值。改讀
   ``schema["score"]`` 之後一位不差（`score_col_used` 這個欄名標籤以外，三份
   診斷的每一個數值 byte-for-byte 相同）。校準器已隨 #411 移除，``score`` 就是
   模型原始輸出，所以「換一欄讀」本來就不該改變任何數字；這些 golden 值是那句
   話的證據，不是重新錄的期望值。
2. **欄名真的來自設定。** :func:`test_a_renamed_score_column_gives_the_same_numbers`
   把 ``schema.columns.score`` 換成 ``model_score`` 再算一次，要求結果一模一樣。
   任何形式的寫死欄名（不管寫死成 ``score`` 還是 ``score_uncalibrated``）都過
   不了這一條。

fixture 刻意讓 ``score_uncalibrated`` 帶一個**跟 ``score`` 不一致**的常數：
讀錯欄不會安靜地得到相同答案，會得到一整欄同分的退化結果。單純把那一欄拿掉
是不夠的——那樣「讀錯欄」只會變成 KeyError，抓不到「讀了它但值剛好一樣」。
"""
import numpy as np
import pandas as pd
import pytest

from recsys_tfb.diagnosis.metric.config_shift._compute import compute as compute_config_shift
from recsys_tfb.diagnosis.metric.item_ability._compute import compute as compute_item_ability
from recsys_tfb.diagnosis.metric.suppression._compute import compute as compute_suppression

#: 這一欄已 deprecated（恆等於 ``score``，#412 移除）。fixture 給它一個與
#: ``score`` 不一致的值，正是為了讓「還有人在讀它」這件事一定會被抓到。
DEPRECATED_COL = "score_uncalibrated"


def _params(score_col: str = "score") -> dict:
    return {
        "schema": {"columns": {"time": "snap_date", "entity": ["cust_id"],
                               "item": "prod_name", "label": "label",
                               "score": score_col, "rank": "rank"}},
        "dataset": {
            "sample_group_keys": ["cust_segment_typ", "prod_name", "label"],
            "sample_ratio": 1.0,
            "sample_ratio_overrides": {"mass|ccard_ins|0": 0.5},
        },
        "training": {"sample_weight_keys": [], "sample_weights": {}},
        "evaluation": {
            "metric": {"k": 3},
            "diagnosis": {
                "ci": {"enabled": True, "n_boot": 20},
                "sample": {"seed": 42},
                "config_shift": {"enabled": True},
                "item_ability": {"enabled": True, "top_n": 30},
                "suppression": {"enabled": True, "top_examples": 50},
            },
        },
    }


def _sample(score_col: str = "score") -> pd.DataFrame:
    """固定輸入。分數放進 ``score_col``；``score_uncalibrated`` 放不一致的常數。"""
    rng = np.random.default_rng(20260920)
    rows = []
    for c in range(60):
        for item in ("ccard_ins", "fund_bond", "fund_stock"):
            rows.append({
                "snap_date": "2026-01-31",
                "cust_id": f"c{c:03d}",
                "prod_name": item,
                "cust_segment_typ": "mass" if c % 2 == 0 else "affluent",
                "label": int((item == "ccard_ins" and c % 2 == 0)
                             or (item == "fund_bond" and c % 5 == 0)),
                "stratum": "hash_ratio" if c % 3 else "take_all",
                "inclusion_weight": 1 / 0.37 if c % 3 else 1.0,
                score_col: float(rng.uniform(0.05, 0.95)),
                DEPRECATED_COL: 0.5,
            })
    return pd.DataFrame(rows)


_META = {"n_queries": 60, "sample_ratio": 0.37,
         "strata": {"take_all": {"n_queries": 20, "weight": 1.0},
                    "hash_ratio": {"n_queries": 40, "weight": 1 / 0.37}}}

#: #415 改動**前**量到的值（模組當時寫死讀 ``score_uncalibrated``）。
GOLDEN_CONFIG_SHIFT = {
    "baseline_map": 0.6430203938115332,
    "corrected_map": 0.5973804500703235,
    "delta": -0.04563994374120972,
}
GOLDEN_ITEM_ABILITY_FIRST_ITEM = {
    "item": "ccard_ins",
    "raw_within_item_auc": 0.3258580355712225,
    "query_centered_auc": 0.3826607203261585,
    "auc_gap_raw_minus_centered": -0.056802684754935995,
}
GOLDEN_SUPPRESSION = {
    "macro_per_item_map": 0.661111111111111,
    "n_positive_rows": 42,
    "n_suppressed_positive_rows": 28,
    "mean_negatives_above_positive": 0.8571428571428571,
}


def _run_all(score_col: str) -> dict:
    sample = (_sample(score_col), _META)
    params = _params(score_col)
    return {
        "config_shift": compute_config_shift(sample, params),
        "item_ability": compute_item_ability(sample, params),
        "suppression": compute_suppression(sample, params),
    }


def test_config_shift_values_match_the_pre_change_run():
    out = compute_config_shift((_sample(), _META), _params())
    for key, expected in GOLDEN_CONFIG_SHIFT.items():
        assert out[key] == pytest.approx(expected, rel=1e-12), key
    assert out["score_col_used"] == "score"


def test_item_ability_values_match_the_pre_change_run():
    out = compute_item_ability((_sample(), _META), _params())
    first = out["per_item"][0]
    for key, expected in GOLDEN_ITEM_ABILITY_FIRST_ITEM.items():
        if isinstance(expected, str):
            assert first[key] == expected, key
        else:
            assert first[key] == pytest.approx(expected, rel=1e-12), key
    assert out["score_col_used"] == "score"


def test_suppression_values_match_the_pre_change_run():
    out = compute_suppression((_sample(), _META), _params())
    for key, expected in GOLDEN_SUPPRESSION.items():
        assert out[key] == pytest.approx(expected, rel=1e-12), key
    assert out["score_col_used"] == "score"


def test_a_renamed_score_column_gives_the_same_numbers():
    """欄名改掉、數字不准變——這條擋的是任何形式的寫死欄名。

    ``_sample`` 在兩次呼叫裡用同一個 seed 產生同一串數字，只是放進不同欄名。
    模組若寫死任何一個欄名，改名這一次就讀不到（raise）或讀到那個不一致的
    ``score_uncalibrated`` 常數（數字全變），兩種都會讓這條轉紅。
    """
    default = _run_all("score")
    renamed = _run_all("model_score")

    assert renamed["config_shift"]["score_col_used"] == "model_score"
    assert renamed["item_ability"]["score_col_used"] == "model_score"
    assert renamed["suppression"]["score_col_used"] == "model_score"

    for name in ("config_shift", "item_ability", "suppression"):
        a, b = default[name], renamed[name]
        # score_col_used 就是欄名本身，理當不同；其餘一律逐值相同。
        keys = sorted(set(a) - {"score_col_used", "field_notes"})
        for key in keys:
            assert a[key] == pytest.approx(b[key], rel=1e-12, abs=0) \
                if isinstance(a[key], float) else a[key] == b[key], (name, key)


def test_the_deprecated_column_is_not_read():
    """把 ``score_uncalibrated`` 整欄拿掉，三份診斷照樣跑得出同樣的數字。

    #415 的驗收條件之一：在一張沒有那一欄的預測表上也跑得動。
    """
    sample = _sample().drop(columns=[DEPRECATED_COL])
    params = _params()
    assert DEPRECATED_COL not in sample.columns

    cs = compute_config_shift((sample, _META), params)
    ia = compute_item_ability((sample, _META), params)
    sp = compute_suppression((sample, _META), params)

    assert cs["delta"] == pytest.approx(GOLDEN_CONFIG_SHIFT["delta"], rel=1e-12)
    assert ia["per_item"][0]["raw_within_item_auc"] == pytest.approx(
        GOLDEN_ITEM_ABILITY_FIRST_ITEM["raw_within_item_auc"], rel=1e-12)
    assert sp["n_suppressed_positive_rows"] == \
        GOLDEN_SUPPRESSION["n_suppressed_positive_rows"]
