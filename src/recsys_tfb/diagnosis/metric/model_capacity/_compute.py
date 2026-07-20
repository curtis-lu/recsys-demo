"""model_capacity 計算層：模型的 split gain 花在 item 身分上，還是 context 特徵上。

回答的問題（只有這一個）：**訓練後的模型，split gain 分成三份——item-id 切點
本身（Item Prior）、item 切點之後的 context 切點（Post-Item Context，逐 item
分配）、以及兩者之外的殘餘（未分配，item 切點之前的切點）——各占多少**。
用來分辨「學到互動訊號」與「只記住 item prior」。

**這一項不碰評測資料。** 它只讀訓練側產出的 ``gain_ledger``（LightGBM booster
逐樹逐切點記帳，見 ``diagnosis.model.gain_ledger``），外加同一次執行裡
``item_ability`` 的結果畫一張對照散點。與其餘四項診斷不同，它不吃共用的
``diagnosis_sample``——``contract.INPUTS`` 因此宣告成
``("gain_ledger", "evaluation_item_ability", "parameters")``，``compute`` 的
簽章對應改成 ``compute(gain_ledger, item_ability, parameters)``。

三種「拿不到資料」的路徑
------------------------
``gain_ledger`` 是跨 pipeline 的 optional 產物（訓練側寫
``data/models/${model_version}/diagnostics/gain_ledger.json``，catalog
``optional: true``）。呼叫端可能拿到三種東西：

1. **檔案不存在** → ``None``（catalog 的 ``load()`` 行為，不 raise）。evaluation
   單獨跑、或訓練側是舊版沒跑過這個 node 時會遇到。
2. **訓練側關掉了** → ``{"enabled": False}``（``diagnostics.gain_ledger.enabled:
   false``，見 ``gain_ledger.py`` 的 ``compute_gain_ledger``）。
3. **正常** → 完整 dict。

前兩種都不是錯誤，都回 ``{"enabled": ..., "available": False, "reason": ...}``，
``reason`` 分辨得出是哪一種——不得 raise，也不得讓兩種路徑共用同一句 reason
（讀者要能分辨「evaluation 單獨跑」與「訓練側刻意關掉」，處置方式不同）。

同理，``item_ability`` 也有三態（``None``／``{"enabled": False}`` stub／完整
dict）。**前兩種必須等價處理成「這次沒有 ability 資料可 join」**：只判斷
``is None`` 會讓 stub 走進 ``.get("per_item", [])`` 拿到空 list，靜默算出
「有資料但剛好是空的」，跟「壓根沒有這份資料」在讀者眼裡長得一樣但成因不同
——這裡分開處理是為了讓 ``notes`` 能講出是哪一種。

``gain_ledger`` 只認正式的巢狀 schema（見 :func:`_gain_sum`）
----------------------------------------------------------------
``diagnosis.model.gain_ledger.compute_gain_ledger`` 與其降級路徑
``_coarse_ledger``（``diagnosis/model/gain_ledger.py:217-256``）**兩條路都**
把 item-id／context 帳分別巢狀在 ``gain_ledger["item_id"]["gain_sum"]``／
``gain_ledger["context"]["gain_sum"]``——這是唯一的正式契約。

（2026-07-20 修正：本模組先前對一組扁平鍵 ``item_id_gain``／
``post_item_context_gain`` 提供了「相容讀取」備援，源頭是規格草稿裡一份憑空
捏造、``gain_ledger.py`` 從未產出過的測試 fixture。那個備援讓 schema 真的不
符時（例如巢狀鍵被改名）整項診斷會靜默算出全 ``None``、報表空白，而
**29 條測試沒有一條會轉紅**——相容讀取把「真正的 schema bug」偽裝成「正常
的降級路徑」。已整段移除，只認巢狀 schema；找不到就在 ``notes`` 出聲，見
:func:`_schema_notes`，不是靜默吞掉。）

``gain_ledger`` 還可能是**粗帳本降級**（``fallback: True``，訓練側 preprocessor
缺 item 欄的 category mapping 時，即 ``_coarse_ledger``）：此時只有 item-id
帳，``context``／``per_item`` 都明確是 ``None``——這是**已知合法**的退化
形狀（``gain_ledger.py`` 自己的契約），不是 schema 不符，:func:`_schema_notes`
刻意不對這個已知案例重複發另一句警告（``fallback`` 分支已經有自己的 note）。

**schema 真的不符時**（``item_id`` 整個找不到，或 ``context`` 找不到但
``fallback`` 沒有宣告——不符合上述任何一種已知形狀）：:func:`_schema_notes`
在 ``notes`` 點名缺了哪個區塊，**不 raise**（``gain_ledger`` 是 best-effort
產物，一份 schema 不符的 ledger 不該讓整條 evaluation pipeline 死掉）；對應
的 gain／share 欄位留 ``None``，不假裝算出一個數字。

per-item context_gain_share 的分母
------------------------------------
``per_item[item].context_gain_share`` 的分母是**這批 item 的 context_gain
加總**（allocated 總和），不是全模型的 ``context_gain``（global 總和）——與
``gain_ledger.py`` 自己的 ``per_item[item]["context_gain_share"]`` 同一個分母
慣例（``diagnosis/model/gain_ledger.py`` 的 ``_ledger_from_trees``）。之所以
在這裡重新算一次而不是直接讀 upstream 算好的值：測試 fixture 的 ``per_item``
只有 ``context_gain`` 一個欄，沒有預先算好的 share。

不下結論
--------
本模組只輸出數字與對照點。**沒有** severity／verdict／建議動作，也沒有把連續
量切成離散類別的門檻——判斷留給讀者。
"""
from __future__ import annotations

from typing import Any, Optional

from recsys_tfb.diagnosis.metric._common import diag_cfg

#: 每個非顯然欄位一句話定義，跟著 JSON 走。純定義，不含判讀（見模組 docstring
#: 「不下結論」）。
FIELD_NOTES: dict[str, str] = {
    "summary.total_gain": "全模型的 split gain 總和（訓練期，來自 gain_ledger.total_gain）。",
    "summary.item_id_gain": (
        "全模型 item-id 切點的 split gain 加總（訓練期，來自 "
        "gain_ledger.item_id.gain_sum，即 Item Prior）。"
    ),
    "summary.context_gain": (
        "全模型「item 切點之後」的 context 切點 gain 加總（訓練期，來自 "
        "gain_ledger.context.gain_sum，即 Post-Item Context）。"
    ),
    "summary.unaccounted_gain": (
        "total_gain − item_id_gain − context_gain。item 切點**之前**的切點，"
        "無法歸給任何單一 item——不是誤差，是這套記帳規則的殘餘。"
    ),
    "summary.item_id_gain_share": "item_id_gain / total_gain。",
    "summary.context_gain_share": "context_gain / total_gain。",
    "summary.unaccounted_gain_share": "unaccounted_gain / total_gain（殘差，不是假設為 0）。",
    "summary.n_items": "gain_ledger.per_item 裡出現的 item 數。",
    "per_item.context_gain": "該 item 分到的 context gain（來自 gain_ledger.per_item[item].context_gain）。",
    "per_item.context_gain_share": (
        "該 item 的 context_gain 占「這批 item 的 context_gain 加總」的比例"
        "——分母是 allocated 加總，不是全模型的 context_gain。"
    ),
    "per_item.query_centered_auc": (
        "同一次執行的 item_ability 診斷算出的 query-centered AUC；item_ability "
        "缺席或被關閉時為 None。"
    ),
}

#: 未計算時的 ``summary`` 形狀——三條以上 return 路徑的 key set 必須完全相同，
#: 這是它們共用的空殼（見模組頂層 :func:`compute` docstring）。
_EMPTY_SUMMARY: dict[str, Any] = {
    "total_gain": None,
    "item_id_gain": None,
    "context_gain": None,
    "unaccounted_gain": None,
    "item_id_gain_share": None,
    "context_gain_share": None,
    "unaccounted_gain_share": None,
    "n_items": None,
}


def _gain_sum(ledger: dict, nested_key: str) -> Optional[float]:
    """讀 ``ledger[nested_key]["gain_sum"]``——只認 ``gain_ledger.py`` 的正式
    巢狀 schema（``compute_gain_ledger`` 與 ``_coarse_ledger`` 兩條路都是這個
    形狀，見 ``diagnosis/model/gain_ledger.py:217-256``）。找不到就回
    ``None``，不 raise——呼叫端 :func:`_schema_notes` 另外判斷這個「找不到」
    要不要對讀者出聲。
    """
    nested = ledger.get(nested_key)
    if isinstance(nested, dict) and nested.get("gain_sum") is not None:
        return float(nested["gain_sum"])
    return None


def _schema_notes(ledger: dict) -> list[str]:
    """``gain_ledger`` 存在（非 None、非 stub）但找不到預期巢狀區塊時的說明。

    **不 raise**——``gain_ledger`` 是 best-effort 產物，schema 不符不該讓
    整條 evaluation pipeline 死掉；但也不能靜默：找不到就在 ``notes`` 點名
    缺哪個鍵，讀者才能分辨「這批 item 的 context gain 真的是 0」與「這份
    ledger 我根本讀不懂」。

    ``context`` 缺席在 ``fallback: True`` 時是**已知合法**的形狀
    （``_coarse_ledger`` 的既定契約，見模組 docstring）——那個案例已經有自己
    的 fallback note，這裡不重複發第二句。``item_id`` 則不論 fallback 與否
    都應該存在（兩條產出路徑都有 item-id 帳），缺席一律視為 schema 不符。
    """
    notes: list[str] = []
    if not isinstance(ledger.get("item_id"), dict) or ledger["item_id"].get("gain_sum") is None:
        notes.append(
            "gain_ledger schema 不符：找不到 item_id.gain_sum（預期巢狀區塊 "
            "'item_id'，見 diagnosis/model/gain_ledger.py 的正式輸出）——"
            "item_id_gain 留空，不假裝算出一個數字。"
        )
    if not ledger.get("fallback") and (
        not isinstance(ledger.get("context"), dict)
        or ledger["context"].get("gain_sum") is None
    ):
        notes.append(
            "gain_ledger schema 不符：找不到 context.gain_sum（預期巢狀區塊 "
            "'context'，見 diagnosis/model/gain_ledger.py 的正式輸出；"
            "fallback=True 時 context 為 None 是已知合法形狀，不算這裡）——"
            "context_gain 留空，不假裝算出一個數字。"
        )
    return notes


def _ability_lookup(item_ability: Optional[dict]) -> tuple[dict[str, dict], list[str]]:
    """``item_ability`` 的三態 → ``({item: 該 item 的 ability row}, notes)``。

    ``None``（未跑過）與 ``{"enabled": False}``（stub，上游被關閉）都回空
    dict——兩者都代表「這次沒有 ability 資料可 join」，但成因不同，各自留一句
    可分辨的 note（見模組 docstring）。
    """
    if item_ability is None:
        return {}, [
            "item_ability 未提供（可能還沒跑過該診斷，或這次 evaluation 未"
            "啟用它）——per_item 的 query_centered_auc 留空。"
        ]
    if not item_ability.get("enabled", True):
        return {}, [
            "item_ability 在上游被關閉（evaluation.diagnosis.item_ability."
            "enabled=false，落地的是 stub）——per_item 的 query_centered_auc "
            "留空。"
        ]
    rows = item_ability.get("per_item") or []
    return (
        {str(r.get("item")): r for r in rows if r.get("item") is not None},
        [],
    )


def compute(
    gain_ledger: Optional[dict],
    item_ability: Optional[dict],
    parameters: dict,
) -> dict:
    """回傳 JSON-safe dict（會直接被 JSONDataset 寫檔）。

    **三條以上 return 路徑（停用／gain_ledger 不可用／完整）的 key set 完全
    相同**，未計算的值留 ``None``／空容器——呼叫端（``render``）因此不必為每個
    鍵寫存在性判斷（照抄 ``config_shift``／``item_ability`` 的契約）。
    """
    diag = diag_cfg(parameters)
    cfg = diag.get("model_capacity", {}) or {}

    out: dict[str, Any] = {
        "enabled": bool(cfg.get("enabled", True)),
        "available": False,
        "reason": None,
        "summary": dict(_EMPTY_SUMMARY),
        "per_item": [],
        "field_notes": FIELD_NOTES,
        "notes": [],
    }
    if not out["enabled"]:
        out["notes"].append(
            "evaluation.diagnosis.model_capacity.enabled = false——未計算。"
        )
        return out

    if gain_ledger is None:
        out["reason"] = (
            "訓練側未產出 gain_ledger.json（catalog optional——訓練側可能是"
            "舊版還沒跑過這個 node，或這次 evaluation 是單獨執行，沒有配對的"
            "訓練產物）。"
        )
        out["notes"].append(out["reason"])
        return out

    if not gain_ledger.get("enabled", True):
        out["reason"] = (
            "訓練側關閉了 diagnostics.gain_ledger.enabled——gain_ledger 落地"
            "的是 stub（{'enabled': False}），不是完整帳本。"
        )
        out["notes"].append(out["reason"])
        return out

    out["available"] = True

    if gain_ledger.get("fallback"):
        out["notes"].append(
            "gain_ledger 是粗帳本降級版本（fallback，訓練側 preprocessor 缺 "
            "item 欄的 category mapping）——只有 item-id 帳，沒有 context 帳"
            "與 per-item 明細；context_gain／unaccounted_gain／per_item 均為 "
            "None／空。"
        )

    total_gain = gain_ledger.get("total_gain")
    total_gain = None if total_gain is None else float(total_gain)
    item_id_gain = _gain_sum(gain_ledger, "item_id")
    context_gain = _gain_sum(gain_ledger, "context")
    out["notes"].extend(_schema_notes(gain_ledger))

    ability_by_item, ability_notes = _ability_lookup(item_ability)
    out["notes"].extend(ability_notes)

    per_item_raw = gain_ledger.get("per_item") or {}
    allocated_sum = sum(
        float(v.get("context_gain"))
        for v in per_item_raw.values()
        if isinstance(v, dict) and v.get("context_gain") is not None
    )

    per_item: list[dict[str, Any]] = []
    for item, entry in per_item_raw.items():
        entry = entry or {}
        cg = entry.get("context_gain")
        cg = None if cg is None else float(cg)
        share = (
            None if cg is None or allocated_sum <= 0.0
            else cg / allocated_sum
        )
        ability_row = ability_by_item.get(str(item), {})
        per_item.append({
            "item": str(item),
            "context_gain": cg,
            "context_gain_share": share,
            "query_centered_auc": ability_row.get("query_centered_auc"),
        })
    # 排序後（依配置容量份額遞減）——per-item 分配條圖直接吃這個順序，不在
    # render 裡重排（排序權在 compute，見 config_shift／item_ability 同一慣例）。
    per_item.sort(key=lambda r: (
        r["context_gain_share"] is None,
        -(r["context_gain_share"] or 0.0),
        r["item"],
    ))

    unaccounted_gain = (
        None if total_gain is None or item_id_gain is None or context_gain is None
        else total_gain - item_id_gain - context_gain
    )

    def _share(x: Optional[float]) -> Optional[float]:
        return None if x is None or total_gain in (None, 0.0) else x / total_gain

    out["summary"] = {
        "total_gain": total_gain,
        "item_id_gain": item_id_gain,
        "context_gain": context_gain,
        "unaccounted_gain": unaccounted_gain,
        "item_id_gain_share": _share(item_id_gain),
        "context_gain_share": _share(context_gain),
        "unaccounted_gain_share": _share(unaccounted_gain),
        "n_items": len(per_item),
    }
    out["per_item"] = per_item
    return out
