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

from statistics import median_high
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
    "summary.n_trees": "booster 的樹數（來自 gain_ledger.n_trees）。",
    "summary.item_id_split_count": (
        "item-id 切點的總筆數（來自 gain_ledger.item_id.split_count；含所有 "
        "item 欄切點，與路徑可達性無關）。"
    ),
    "summary.context_split_count": (
        "post-item context 切點的全域筆數（來自 gain_ledger.context.split_count；"
        "每個切點只算一次，不依可達 item 數重複計）。"
    ),
    "summary.sum_allocated_context_gain": (
        "各 item 的 context_gain 加總——per-item context_gain_share 的分母。"
        "因共用切點的 gain 被記給每個可達 item，此加總大於全域 context_gain。"
    ),
    "summary.sum_allocated_context_split": (
        "各 item 的 context_split_count 加總——per-item context_split_share 的"
        "分母。同理，因共用切點被記給每個可達 item，大於全域 context_split_count。"
    ),
    "per_item.context_split_count": (
        "該 item 可達時的 post-item context 切點數（來自 gain_ledger.per_item"
        "[item].context_split_count；共用切點記給每個可達 item）。"
    ),
    "per_item.context_split_share": (
        "該 item 的 context_split_count 占 sum_allocated_context_split 的比例"
        "（分母是這批 item 的加總，不是全域）。"
    ),
    "per_item.gain_per_split": (
        "context_gain / context_split_count——該 item 每個 context 切點平均分到"
        "的 gain（密度）。"
    ),
    "per_item.context_gain_isolated": (
        "該 item 是「唯一可達 item」時的 context 切點 gain 加總（來自 "
        "gain_ledger.per_item[item].context_gain_isolated）——專屬於此 item、"
        "非與他 item 共用的 context gain。"
    ),
    "per_item.context_gain_isolated_share": (
        "context_gain_isolated / context_gain——該 item 的 context gain 中，"
        "屬於私有（非共用）的比例。"
    ),
    "per_item.isolating_split_count": (
        "該 item 尚可達時發生的 item-id 切點數（來自 gain_ledger.per_item[item]."
        "isolating_split_count）——反映該 item 在 item 路由結構裡被隔離的深度。"
    ),
    "per_item.first_tree_index": (
        "該 item 首次出現（作為 item 切點可達）的 boosting 樹序（來自 "
        "gain_ledger.per_item[item].first_tree_index）。"
    ),
    "per_item.n_trees_touched": (
        "該 item 被觸及（item 切點可達或被記 context gain）的樹數（來自 "
        "len(gain_ledger.per_item[item].trees_touched)）。"
    ),
    "per_item.gain_share_vs_max": (
        "context_gain_share / 各 item 佔比的最大值——相對第一名的集中度。"
        "第一名自己＝1.0。是 context_gain_share 的縮放，非獨立訊號。"
    ),
    "per_item.gain_share_vs_median": (
        "context_gain_share / 各 item 佔比的中位數（median_high，取實際存在的"
        "中位 item）——相對中位 item 的集中度。是 context_gain_share 的縮放，"
        "非獨立訊號。"
    ),
    "per_item.context_gain_vs_total": (
        "context_gain / total_gain——該 item 被切到的 context 容量相當於全模型"
        "總 gain 的多少（涵蓋量，跨 item 加總>100%，因共用切點重計）。"
    ),
    "per_item.context_gain_isolated_vs_total": (
        "context_gain_isolated / total_gain——該 item 的私有（非共用）context "
        "gain 相當於全模型的多少（獨佔量，不重計）。"
    ),
    "per_item.context_split_isolated": (
        "該 item 是唯一可達 item 時的 context 切點數（來自 gain_ledger.per_item"
        "[item].context_split_isolated）——context_gain_isolated 的計數版。"
    ),
    "per_item.context_split_vs_total": (
        "context_split_count / total_split_count——該 item 可達的 context 切點數"
        "相當於全模型切點總數的多少（涵蓋量，跨 item 加總>100%，共用切點重計）。"
    ),
    "per_item.context_split_isolated_vs_total": (
        "context_split_isolated / total_split_count——該 item 的私有 context 切點"
        "數相當於全模型切點總數的多少（獨佔量，不重計）。"
    ),
    "summary.total_split_count": (
        "全模型非葉節點總數（來自 gain_ledger.total_split_count）——split 三分"
        "的分母。"
    ),
    "summary.unaccounted_split_count": (
        "total_split_count − item_id_split_count − context_split_count——item 切點"
        "之前的切點數（殘差，與 unaccounted_gain 對稱）。"
    ),
    "summary.item_id_split_share": "item_id_split_count / total_split_count。",
    "summary.context_split_share": "context_split_count / total_split_count。",
    "summary.unaccounted_split_share": "unaccounted_split_count / total_split_count。",
    "pre_item": (
        "未分配（item 切點之前的未 conditioned 切點）按特徵拆解：{gain_sum, "
        "split_count, by_feature:{feat:{gain, split_count}}}，gain 遞減。其 "
        "gain_sum 恆等於 summary.unaccounted_gain。舊版 ledger／粗帳本為 None。"
    ),
    "first_item_split_depth": (
        "每棵樹最淺 item 切點的 node_depth 分位摘要（root=1）：{min,p25,p50,p75,"
        "max,n_trees_with_item_split}。量 item 條件化坐落多深。舊版／粗帳本為 None。"
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
    "n_trees": None,
    "item_id_split_count": None,
    "context_split_count": None,
    "total_split_count": None,
    "unaccounted_split_count": None,
    "item_id_split_share": None,
    "context_split_share": None,
    "unaccounted_split_share": None,
    "sum_allocated_context_gain": None,
    "sum_allocated_context_split": None,
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


def _split_count(ledger: dict, nested_key: str) -> Optional[int]:
    """讀 ``ledger[nested_key]["split_count"]``——同 :func:`_gain_sum` 只認正式
    巢狀 schema。粗帳本降級時 ``context`` 為 ``None``，這裡回 ``None``（不假裝
    算出 0：0 會被讀成「沒有 context 切點」，那是錯的）。
    """
    nested = ledger.get(nested_key)
    if isinstance(nested, dict) and nested.get("split_count") is not None:
        return int(nested["split_count"])
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
        # 未分配（pre-item）按特徵拆解 ＋ item 切點深度摘要——由 gain_ledger 直接
        # 帶入（Q3-#1/#2）。舊版 ledger／粗帳本降級為 None。必須在**每條** return
        # 路徑都存在（key-set 契約），故放在 out 初始化。
        "pre_item": None,
        "first_item_split_depth": None,
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

    # total_split_count 供 per-item 的 split「vs 全模型」欄用（Q2）；舊版 ledger
    # 缺它時 split 涵蓋／獨佔留 None。
    total_split_for_row = gain_ledger.get("total_split_count")
    total_split_for_row = None if total_split_for_row is None else int(total_split_for_row)

    per_item_raw = gain_ledger.get("per_item") or {}
    allocated_sum = sum(
        float(v.get("context_gain"))
        for v in per_item_raw.values()
        if isinstance(v, dict) and v.get("context_gain") is not None
    )
    allocated_split_sum = sum(
        int(v.get("context_split_count"))
        for v in per_item_raw.values()
        if isinstance(v, dict) and v.get("context_split_count") is not None
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
        csc = entry.get("context_split_count")
        csc = None if csc is None else int(csc)
        split_share = (
            None if csc is None or allocated_split_sum <= 0
            else csc / allocated_split_sum
        )
        # gain ÷ split：兩者任一缺席或 split 為 0 → None，不製造 inf。
        gain_per_split = None if cg is None or not csc else cg / csc
        isolated = entry.get("context_gain_isolated")
        isolated = None if isolated is None else float(isolated)
        # 私有 gain 占該 item 自己 context gain 的比例——分母是 cg，不是全域。
        isolated_share = (
            None if isolated is None or cg in (None, 0.0) else isolated / cg
        )
        isc = entry.get("isolating_split_count")
        isc = None if isc is None else int(isc)
        trees_touched = entry.get("trees_touched")
        n_trees_touched = None if trees_touched is None else len(trees_touched)
        # 跟全模型比（Q2/(b)）：涵蓋＝context_gain 相對全模型 total_gain（會>100%
        # 加總，因共用切點重計）；獨佔＝私有 context gain 相對 total_gain（不重計）。
        vs_total = (
            None if cg is None or total_gain in (None, 0.0) else cg / total_gain
        )
        isolated_vs_total = (
            None if isolated is None or total_gain in (None, 0.0)
            else isolated / total_gain
        )
        # split 版的 vs 全模型（Q2）：分母是全模型 total_split_count（不重計）。
        csi = entry.get("context_split_isolated")
        csi = None if csi is None else int(csi)
        split_vs_total = (
            None if csc is None or not total_split_for_row
            else csc / total_split_for_row
        )
        split_isolated_vs_total = (
            None if csi is None or not total_split_for_row
            else csi / total_split_for_row
        )
        ability_row = ability_by_item.get(str(item), {})
        per_item.append({
            "item": str(item),
            "context_gain": cg,
            "context_gain_share": share,
            "context_gain_vs_total": vs_total,
            "context_split_count": csc,
            "context_split_share": split_share,
            "context_split_vs_total": split_vs_total,
            "gain_per_split": gain_per_split,
            "context_gain_isolated": isolated,
            "context_gain_isolated_share": isolated_share,
            "context_gain_isolated_vs_total": isolated_vs_total,
            "context_split_isolated": csi,
            "context_split_isolated_vs_total": split_isolated_vs_total,
            "isolating_split_count": isc,
            "first_tree_index": entry.get("first_tree_index"),
            "n_trees_touched": n_trees_touched,
            "query_centered_auc": ability_row.get("query_centered_auc"),
        })
    # 排序後（依配置容量份額遞減）——per-item 分配條圖直接吃這個順序，不在
    # render 裡重排（排序權在 compute，見 config_shift／item_ability 同一慣例）。
    per_item.sort(key=lambda r: (
        r["context_gain_share"] is None,
        -(r["context_gain_share"] or 0.0),
        r["item"],
    ))

    # 相對集中度視角（codex §6 的 /max、/median item）：gain 佔比相對「第一名」
    # 與「中位數 item」的倍率。median_high 選實際存在的那個 item（偶數 item 數
    # 取上中位），符合「median item」＝某個真實 item 的語意。兩欄都是 gain 佔比
    # 的縮放、非獨立訊號（呈現層 bullet 明講），保留是因為集中度用「相對第一名／
    # 中位」讀比絕對份額直覺。
    _shares = [r["context_gain_share"] for r in per_item
               if r["context_gain_share"] is not None]
    _max_share = max(_shares) if _shares else None
    _median_share = median_high(_shares) if _shares else None
    for r in per_item:
        cs = r["context_gain_share"]
        r["gain_share_vs_max"] = (
            None if cs is None or not _max_share else cs / _max_share
        )
        r["gain_share_vs_median"] = (
            None if cs is None or not _median_share else cs / _median_share
        )

    unaccounted_gain = (
        None if total_gain is None or item_id_gain is None or context_gain is None
        else total_gain - item_id_gain - context_gain
    )

    def _share(x: Optional[float]) -> Optional[float]:
        return None if x is None or total_gain in (None, 0.0) else x / total_gain

    # split 三分（c）：total_split_count 在→未分配 split ＝ total−item−context
    # （殘差，與 gain 三分對稱）。舊版 ledger 缺 total_split_count → 全留 None。
    item_id_split = _split_count(gain_ledger, "item_id")
    context_split = _split_count(gain_ledger, "context")
    total_split = gain_ledger.get("total_split_count")
    total_split = None if total_split is None else int(total_split)
    unaccounted_split = (
        None if total_split is None or item_id_split is None or context_split is None
        else total_split - item_id_split - context_split
    )

    def _split_share(x: Optional[int]) -> Optional[float]:
        return None if x is None or not total_split else x / total_split

    out["summary"] = {
        "total_gain": total_gain,
        "item_id_gain": item_id_gain,
        "context_gain": context_gain,
        "unaccounted_gain": unaccounted_gain,
        "item_id_gain_share": _share(item_id_gain),
        "context_gain_share": _share(context_gain),
        "unaccounted_gain_share": _share(unaccounted_gain),
        "n_items": len(per_item),
        "n_trees": gain_ledger.get("n_trees"),
        "item_id_split_count": item_id_split,
        "context_split_count": context_split,
        "total_split_count": total_split,
        "unaccounted_split_count": unaccounted_split,
        "item_id_split_share": _split_share(item_id_split),
        "context_split_share": _split_share(context_split),
        "unaccounted_split_share": _split_share(unaccounted_split),
        # 分配分母：per_item 為空（粗帳本降級）時留 None，不假裝算出 0。
        "sum_allocated_context_gain": allocated_sum if per_item_raw else None,
        "sum_allocated_context_split": allocated_split_sum if per_item_raw else None,
    }
    out["per_item"] = per_item
    # pre-item 拆解與 item 切點深度：gain_ledger 直接帶入（可能 None＝舊版/粗帳本）。
    out["pre_item"] = gain_ledger.get("pre_item")
    out["first_item_split_depth"] = gain_ledger.get("first_item_split_depth")
    return out
