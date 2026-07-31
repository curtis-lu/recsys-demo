# Domain Docs

How the engineering skills should consume this repo's domain documentation when exploring the codebase.

## Before exploring, read these

**這個 repo 刻意沒有 `CONTEXT.md`。** 詞彙已經有既存的真實來源，再開一個根目錄 glossary 會造成第三份、而且是唯一沒有測試守著的那份（違反 `CLAUDE.md`「每個主題只有一個真實來源」）。要查詞彙時去這兩個地方：

- **`docs/pipelines/dataset.md` §7.1–7.4** — 三層版本 ID（`base_dataset_version` / `train_variant_id` / `calibration_variant_id`）與 `model_version` 的定義、什麼設定會讓誰翻版。
- **`src/recsys_tfb/core/consistency.py` 模組 docstring** — 一致性不變量的 legend（A 系列＝設定層、B 系列＝資料層），是這些代號的唯一真實來源。

其次：

- **`docs/adr/`** — 動到相關區域前，先讀觸及該區域的 ADR。（首次建立於 dataset 版本語意那一系列 PR；在那之前這個目錄可能還不存在。）
- **`graphify-out/GRAPH_REPORT.md`** — 架構／重構／探索任務的**強制**起點，見 `CLAUDE.md` 路由表。這一步不得用 Explore agent 代替。

If any of these files don't exist, **proceed silently**. Don't flag their absence; don't suggest creating them upfront. The `/domain-modeling` skill (reached via `/grill-with-docs` and `/improve-codebase-architecture`) creates ADRs lazily when decisions actually get resolved.

## File structure

Single-context repo:

```
/
├── docs/
│   ├── adr/                  ← 架構決策紀錄
│   ├── pipelines/            ← 各 pipeline 的權威說明（含版本語意）
│   └── operations/           ← runbook 與踩坑紀錄
└── src/recsys_tfb/
```

沒有 `CONTEXT.md`／`CONTEXT-MAP.md`，理由見上。

## Use the glossary's vocabulary

When your output names a domain concept (in an issue title, a refactor proposal, a hypothesis, a test name), use the term as defined in the sources above — 特別是版本 ID 的名稱與一致性不變量的代號，**逐字照抄，不要憑記憶寫**。Don't drift to synonyms.

If the concept you need isn't defined anywhere yet, that's a signal — either you're inventing language the project doesn't use (reconsider) or there's a real gap (note it for `/domain-modeling`).

## Flag ADR conflicts

If your output contradicts an existing ADR, surface it explicitly rather than silently overriding:

> _Contradicts ADR-0001 (test 日期退出 dataset 版本身分) — but worth reopening because…_
