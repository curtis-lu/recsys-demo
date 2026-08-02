# Domain Docs

How the engineering skills should consume this repo's domain documentation when exploring the codebase.

## Before exploring, read these

**這個 repo 刻意沒有 `CONTEXT.md`。** 規則是：**詞彙的定義一律放在程式碼旁的模組 docstring，`docs/` 只負責解釋為什麼，不承擔定義職責。** 理由是 `CONTEXT.md` 的規格要求它不含實作細節，但這個 repo 最需要被釘死的詞（版本 ID、不變量代號）其精確定義**本身就是實作**——「哪些 config key 進 hash」「哪個 predicate 擋什麼」。寫進一份不能有實作細節的檔案只能寫成近似，而那份近似會是各 skill 優先讀到的一份。加上它過時時沒有任何動作會逼人發現（改 docstring 時 diff 就在旁邊，改 `CONTEXT.md` 沒有這個機制）。

**定義來源（要逐字照抄的名稱去這裡查）：**

- **`src/recsys_tfb/core/versioning.py` 模組 docstring** — 三層版本 ID（`base_dataset_version` / `train_variant_id` / `calibration_variant_id`）與 `model_version` 的定義：各自由哪些設定推導、key 住哪些產物。
- **`src/recsys_tfb/core/consistency.py` 模組 docstring** — 一致性不變量的 legend（A 系列＝設定層、B 系列＝資料層），是這些代號的唯一真實來源。

**理解來源（要知道「為什麼這樣切、代價是什麼」時去這裡讀）：**

- **`docs/pipelines/dataset.md` §7.1–7.4** — 上述版本語意的白話說明：什麼設定會讓誰翻版、`test_snap_dates` 為何被排除、累積語意的代價。這是寫給人讀的解釋文，**不是定義表**；與 docstring 有出入時以 docstring 為準。

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
│   ├── agents/               ← 本系列 skill 的 per-repo 設定
│   ├── pipelines/            ← 各 pipeline 的說明（版本語意的白話解釋）
│   ├── operations/           ← runbook 與踩坑紀錄
│   ├── notes/                ← 長文分析／研究產出
│   └── superpowers/          ← superpowers skill 專屬存檔，本系列不寫入（見 CLAUDE.md §Agent skills）
└── src/recsys_tfb/           ← 詞彙定義所在（見上：versioning.py / consistency.py docstring）
```

沒有 `CONTEXT.md`／`CONTEXT-MAP.md`，理由見上。

## Use the glossary's vocabulary

When your output names a domain concept (in an issue title, a refactor proposal, a hypothesis, a test name), use the term as defined in the sources above — 特別是版本 ID 的名稱與一致性不變量的代號，**逐字照抄，不要憑記憶寫**。Don't drift to synonyms.

If the concept you need isn't defined anywhere yet, that's a signal — either you're inventing language the project doesn't use (reconsider) or there's a real gap (note it for `/domain-modeling`).

## Flag ADR conflicts

If your output contradicts an existing ADR, surface it explicitly rather than silently overriding:

> _Contradicts ADR-0001 (test 日期退出 dataset 版本身分) — but worth reopening because…_
