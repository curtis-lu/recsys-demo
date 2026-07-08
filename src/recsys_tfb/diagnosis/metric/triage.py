"""Triage：跨三層診斷 dict 合成 per-item 判定表（框架 Ch4 判讀流程落地為 code）。

輸入四個由各自診斷節點產出的 JSON-ready dict（quadrant/reconciliation/
offset_sweep/gain_ledger），輸出 per-item 判定＋槓桿建議＋起手值。任一輸入
缺席或降級都不 raise（best-effort，與其餘 diagnosis metric 模組同一慣例）；
跨側讀 gain_ledger.json 走 catalog 的 optional JSON 載入（見
``recsys_tfb.io.json_dataset.JSONDataset``），本模組本身只吃 dict，不管檔案
怎麼來、也不管訓練側是否已經跑過該 node。

判定表與槓桿映射照診斷框架手冊 Ch4；``_STARVE_RATIO``/``_WEIGHT_CAP`` 是起
手值門檻，非定案，需經快迴路驗證才能升格（見 ``STARTER_CAVEAT``）。
"""
from __future__ import annotations

import math

_STARVE_RATIO = 0.25  # 起手門檻：context_gain_share < 0.25×全 item 最大 share 視為餓死；非定案
_WEIGHT_CAP = 8.0  # 餓死型起手權重上限（w∝1/√P 加上限，手冊3 Ch8）
STARTER_CAVEAT = "起手值，須經快迴路驗證，非定案"

_V_HEALTHY = "健康"
_V_CONFIG = "水準-配置型"
_V_REBALANCE = "水準-指標再平衡型"
_V_STARVED = "餓死型"
_V_FEATURE_MISSING = "特徵缺失型"
_V_NO_STRUCTURAL_EVIDENCE = "餓死型或特徵缺失型（無結構層證據）"

_LEVERS = {
    _V_HEALTHY: "無（維持觀測）",
    _V_CONFIG: "槓桿1：推論期 logQ/offset 修正或修採樣配置（閉式）",
    _V_REBALANCE: "槓桿2：常態化後處理 per-item offset 或 item-aware sample_weight",
    _V_STARVED: "槓桿3：item-aware weight／熱門負類欠採（配 logQ）／HPO 先驗",
    _V_FEATURE_MISSING: "槓桿5：補特徵（診斷只能縮小範圍，補什麼是領域知識）",
    _V_NO_STRUCTURAL_EVIDENCE: "槓桿3 或 5（先補跑 gain_ledger 再分流）",
}


def _config_signal(r: dict | None) -> bool:
    """水準偏移是否有配置面（採樣/權重）理論解釋——band 退化 [0,0] 的
    「可解釋」＝本來就沒偏，不算配置訊號。"""
    if r is None or r.get("verdict") != "可解釋":
        return False
    band_width = (r.get("theory_max", 0) or 0) - (r.get("theory_min", 0) or 0)
    return bool(band_width > 0 or r.get("theory_approx", False))


def _gain_ledger_usable(gain_ledger: dict | None) -> bool:
    if not gain_ledger:
        return False
    if not gain_ledger.get("enabled", False):
        return False
    if gain_ledger.get("fallback", False):
        return False
    return bool(gain_ledger.get("per_item"))


def _max_context_gain_share(per_item: dict) -> float:
    shares = [v.get("context_gain_share") or 0.0 for v in per_item.values()]
    return max(shares) if shares else 0.0


def _max_y_rate(by_item: dict) -> float:
    rates = [(v or {}).get("y_rate") or 0.0 for v in by_item.values()]
    return max(rates) if rates else 0.0


def _config_starter(item: str, r: dict, reconciliation: dict | None) -> dict:
    theory_min = r.get("theory_min")
    theory_max = r.get("theory_max")
    theory_entry = (
        ((reconciliation or {}).get("theory") or {}).get("by_item") or {}
    ).get(item)
    mean = theory_entry.get("mean") if theory_entry else None
    if mean is None:
        mean = ((theory_min or 0.0) + (theory_max or 0.0)) / 2
    return {
        "type": "logq_offset",
        "value": mean,
        "band": [theory_min, theory_max],
        "unit": "log-odds",
        "caveat": STARTER_CAVEAT,
    }


def _rebalance_starter(
    item: str, sweep_by_item: dict, notes: list[str]
) -> dict | None:
    entry = sweep_by_item.get(item)
    if not entry or entry.get("delta_star_centered") is None:
        notes.append(
            "offset_sweep 缺席或無該 item 資料——再平衡型起手值從缺，"
            "待補跑 offset sweep"
        )
        return None
    return {
        "type": "delta_star_centered",
        "value": entry["delta_star_centered"],
        "unit": (
            "logit（centered；跨執行只有相對差可比，加到分數上與 raw 差一共同"
            "平移、排序等價）"
        ),
        "caveat": STARTER_CAVEAT,
    }


def _starved_starter(
    item: str, by_item: dict, notes: list[str]
) -> dict | None:
    y_rate = (by_item.get(item) or {}).get("y_rate")
    if not y_rate or y_rate <= 0:
        notes.append("y_rate 不可用（缺席或非正值）——item_weight 起手值無法計算")
        return None
    max_rate = _max_y_rate(by_item)
    value = round(min(_WEIGHT_CAP, math.sqrt(max_rate / y_rate)), 2)
    return {
        "type": "item_weight",
        "value": value,
        "unit": "sample_weight 相對倍率（w∝1/√P 加上限，手冊3 Ch8）",
        "caveat": STARTER_CAVEAT,
    }


def triage(
    quadrant: dict | None,
    reconciliation: dict | None,
    offset_sweep: dict | None,
    gain_ledger: dict | None,
    parameters: dict,
) -> dict:
    """跨三層診斷 dict → per-item 判定表（框架 Ch4 判讀流程）。

    best-effort：quadrant/reconciliation/offset_sweep/gain_ledger 任一缺席、
    停用或降級都不 raise，改記 notes 並儘量給出可評估的部分。``parameters``
    保留供未來 config 覆寫門檻，目前未讀取任何鍵（門檻是起手值，見模組
    docstring）。
    """
    top_notes: list[str] = []

    quadrant_ok = bool(quadrant) and quadrant.get("enabled", True) and bool(
        quadrant.get("by_item")
    )
    by_item = quadrant.get("by_item", {}) if quadrant_ok else {}
    if not quadrant_ok:
        top_notes.append(
            "quadrant 缺席、停用或 by_item 為空——無法產生逐 item 判定"
        )

    recon_by_item = (reconciliation or {}).get("by_item") or {}
    if not reconciliation or not reconciliation.get("by_item"):
        top_notes.append(
            "reconciliation 缺席或 by_item 為空——水準判定與配置訊號的"
            "部分證據無法評估"
        )

    sweep_by_item = (offset_sweep or {}).get("per_item") or {}
    if not offset_sweep or not offset_sweep.get("per_item"):
        top_notes.append(
            "offset_sweep 缺席或 per_item 為空——再平衡型起手值可能無法計算"
        )

    gl_present = _gain_ledger_usable(gain_ledger)
    gl_per_item = (gain_ledger or {}).get("per_item") or {}
    if not gl_present:
        top_notes.append(
            "gain_ledger 缺席、停用或 fallback——結構層裁決降級為"
            "「無結構層證據」"
        )
    max_share = _max_context_gain_share(gl_per_item) if gl_present else 0.0

    verdicts: dict[str, dict] = {}
    for item in sorted(by_item):
        q = by_item[item] or {}
        r = recon_by_item.get(item)
        sweep_entry = sweep_by_item.get(item)
        gl_entry = gl_per_item.get(item)

        notes: list[str] = []
        auc = q.get("auc")
        auc_reason = q.get("auc_reason")
        disc_status = q.get("disc_status")
        disc_low = disc_status == "差" and auc_reason is None
        if disc_status == "差" and auc_reason is not None:
            notes.append(f"AUC 樣本不足（{auc_reason}）——判別力軸略過，未計入 disc_low")

        # 「無法評估」（gap_vs_global 缺席，例：reconciliation 停用）不是水準偏移
        # ——沒量到的軸不得觸發水準型判定（審查修復 2026-07-08）。
        level_status = q.get("level_status")
        level_off = level_status in ("偏高", "偏低")
        if level_status == "無法評估":
            notes.append("水準軸無法評估（gap_vs_global 缺席）——水準側判定略過")
        config_signal = _config_signal(r)

        if level_off and config_signal:
            verdict = _V_CONFIG
        elif disc_low:
            if gl_present:
                share = (gl_entry or {}).get("context_gain_share")
                share = share if share is not None else 0.0
                if share < _STARVE_RATIO * max_share:
                    verdict = _V_STARVED
                else:
                    verdict = _V_FEATURE_MISSING
                    notes.append("特徵缺失型判定待條件化 SHAP 佐證")
            else:
                verdict = _V_NO_STRUCTURAL_EVIDENCE
                notes.append(
                    "gain_ledger 缺席或降級——無法區分餓死型與特徵缺失型"
                )
        elif level_off:
            verdict = _V_REBALANCE
        else:
            verdict = _V_HEALTHY

        if verdict in (_V_CONFIG, _V_REBALANCE) and disc_low:
            auc_txt = f"{auc:.3f}" if auc is not None else "N/A"
            notes.append(
                f"條件判別力軸同時偏低（AUC {auc_txt}），"
                "修完水準後重量再判（框架 Ch 4 第 5 步）"
            )
        elif verdict == _V_HEALTHY and sweep_entry is not None:
            dsc = sweep_entry.get("delta_star_centered")
            loo = sweep_entry.get("loo_contribution_holdout")
            if dsc is not None and loo is not None and abs(dsc) >= 0.3 and loo > 0:
                notes.append(
                    f"健康判定但 δ*_centered={dsc:.2f} 且 holdout LOO 貢獻為正"
                    "——留意早期水準漂移（框架 Ch 4 δ* 觀測）"
                )

        starter = None
        if verdict == _V_CONFIG:
            starter = _config_starter(item, r, reconciliation)
        elif verdict == _V_REBALANCE:
            starter = _rebalance_starter(item, sweep_by_item, notes)
        elif verdict == _V_STARVED:
            starter = _starved_starter(item, by_item, notes)

        verdicts[item] = {
            "verdict": verdict,
            "lever": _LEVERS[verdict],
            "starter": starter,
            "evidence": {
                "auc": auc,
                "disc_status": disc_status,
                "level_status": q.get("level_status"),
                "gap_vs_global": q.get("gap_vs_global"),
                "recon_verdict": r.get("verdict") if r else None,
                "theory_min": r.get("theory_min") if r else None,
                "theory_max": r.get("theory_max") if r else None,
                "residual": r.get("residual") if r else None,
                "delta_star_centered": (
                    sweep_entry.get("delta_star_centered") if sweep_entry else None
                ),
                "loo_contribution_holdout": (
                    sweep_entry.get("loo_contribution_holdout")
                    if sweep_entry else None
                ),
                "context_gain_share": (
                    gl_entry.get("context_gain_share") if gl_entry else None
                ),
                "y_rate": q.get("y_rate"),
            },
            "notes": notes,
        }

    summary: dict[str, int] = {}
    for v in verdicts.values():
        summary[v["verdict"]] = summary.get(v["verdict"], 0) + 1

    return {
        "enabled": True,
        "gain_ledger_present": gl_present,
        "thresholds": {"starve_ratio": _STARVE_RATIO, "weight_cap": _WEIGHT_CAP},
        "verdicts": verdicts,
        "summary": summary,
        "notes": top_notes,
    }
