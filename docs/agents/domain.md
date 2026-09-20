# Domain Docs

How the engineering skills should consume this repo's domain documentation when exploring the codebase.

## Before exploring, read these

**先讀 repo 根目錄的 `CONTEXT.md`。** 它是這個 repo 的詞彙表：框架用哪些詞、每個詞是什麼、哪些同義詞不要用。

**分工規則：`CONTEXT.md` 定義「詞是什麼」；版本 ID 與不變量代號的精確定義仍然只在程式碼旁的模組 docstring。** 這兩類詞的精確定義本身就是實作——「哪些 config key 進 hash」「哪個 predicate 擋什麼」。寫進 `CONTEXT.md` 只能寫成近似，而且過時時沒有任何動作會逼人發現（改 docstring 時 diff 就在旁邊，改 `CONTEXT.md` 沒有這個機制）。所以 `CONTEXT.md` 對它們只寫一句意思並指向 docstring；兩邊有出入時以 docstring 為準。

**精確定義來源（要逐字照抄的名稱去這裡查）：**

- **`src/recsys_tfb/core/versioning.py` 模組 docstring** — 兩層版本 ID（`base_dataset_version` / `train_variant_id`；#411 移除了原本的 `calibration_variant_id` 那一層）與 `model_version` 的定義：各自由哪些設定推導、key 住哪些產物。
- **`src/recsys_tfb/core/consistency.py` 模組 docstring** — 一致性不變量的 legend（A 系列＝設定層、B 系列＝資料層），是這些代號的唯一真實來源。

**理解來源（要知道「為什麼這樣切、代價是什麼」時去這裡讀）：**

- **`docs/pipelines/dataset.md` §7.1–7.4** — 上述版本語意的白話說明：什麼設定會讓誰翻版、`test_snap_dates` 為何被排除、累積語意的代價。這是寫給人讀的解釋文，**不是定義表**；與 docstring 有出入時以 docstring 為準。

其次：

- **`docs/adr/`** — 動到相關區域前，先讀觸及該區域的 ADR。
- **`graphify-out/GRAPH_REPORT.md`** — 架構／重構／探索任務的**強制**起點，見 `CLAUDE.md` 路由表。這一步不得用 Explore agent 代替。

If any of these files don't exist, **proceed silently**. Don't flag their absence; don't suggest creating them upfront. The `/domain-modeling` skill (reached via `/grill-with-docs` and `/improve-codebase-architecture`) adds terms to `CONTEXT.md` and creates ADRs lazily when decisions actually get resolved.

## 為什麼有 `CONTEXT.md`

這個 repo 原本刻意不設 `CONTEXT.md`，理由就是上一節那段「近似會過時」。那個理由對版本 ID 與不變量代號仍然成立，所以分工規則保留了它。

改變的是另一半。2026-09-16 設計第二種使用情境（線上廣告推薦的離線訓練與評估，見 ADR-0021～0023）時，`time`、`entity`、`item`、query group 這類跨模組的詞沒有一個地方集中定義，意思散在各 pipeline 文件與錯誤訊息裡；討論中同一個詞被用成不同意思（例如 `time` 被當成「到秒的曝光時間」，其實框架裡它是分組與切分用的時段）。這類詞的定義不是實作，放進 `CONTEXT.md` 沒有「只能寫近似」的問題。

## 維護 `CONTEXT.md`

- 詞在設計討論中定下來的當下就加（`/domain-modeling` 的做法），不要事後補。
- **只寫「它是什麼」，不寫「框架目前做不做得到」。** 能力與進度會變，寫進詞彙表就會腐爛；那些去查程式碼、ADR 或 `gh`。
- **例外：已裁定但還沒實作的詞**可以先收進詞彙表，但詞名後面要標「（ADR-00xx，尚未實作）」，實作落地時把標記拿掉。不標的後果是實測過的：現在在 `schema.columns` 宣告一個框架還不認得的角色鍵，`core/schema.py::get_schema` 只保留 `_ROLE_KEYS` 裡的鍵、`validate_schema_config` 也不檢查多餘的鍵，所以它會被**靜默丟掉**——照詞彙表寫設定的人不會收到任何訊息。
- 版本 ID 與不變量代號不重抄定義，只寫一句意思並指向 docstring。
- 示例部署的業務詞（客戶、產品、廣告）不當框架詞，列在對應詞的 `_Avoid_`（ADR-0017）。
- 格式照 `/domain-modeling` 的 CONTEXT-FORMAT：每個詞一兩句，同義詞列在 `_Avoid_`，依主題分小節。

## File structure

Single-context repo:

```
/
├── CONTEXT.md                ← 詞彙表：每個詞是什麼
├── docs/
│   ├── adr/                  ← 架構決策紀錄
│   ├── agents/               ← 給 agent 的判準與紀律，以及本系列 skill 的 per-repo 設定
│   ├── handbooks/            ← 給讀者自學的技術手冊（寫作判準在 agents/）
│   ├── pipelines/            ← 各 pipeline 的說明（版本語意的白話解釋）
│   ├── operations/           ← 跨 pipeline 工作流（user-guides/）、開發環境（dev-setup/）與踩坑紀錄
│   ├── notes/                ← 長文分析／研究產出
│   └── superpowers/          ← superpowers skill 專屬存檔，本系列不寫入（見 CLAUDE.md §Agent skills）
└── src/recsys_tfb/           ← 版本 ID 與不變量代號的精確定義（versioning.py / consistency.py docstring）
```

沒有 `CONTEXT-MAP.md`：這是 single-context repo。

## Use the glossary's vocabulary

When your output names a domain concept (in an issue title, a refactor proposal, a hypothesis, a test name), use the term as defined in `CONTEXT.md`；版本 ID 的名稱與一致性不變量的代號照 docstring **逐字照抄，不要憑記憶寫**。Don't drift to synonyms — 特別是 `CONTEXT.md` 列在 `_Avoid_` 的詞。

If the concept you need isn't defined anywhere yet, that's a signal — either you're inventing language the project doesn't use (reconsider) or there's a real gap (note it for `/domain-modeling`).

## Flag ADR conflicts

If your output contradicts an existing ADR, surface it explicitly rather than silently overriding:

> _Contradicts ADR-0001 (test 日期退出 dataset 版本身分) — but worth reopening because…_
