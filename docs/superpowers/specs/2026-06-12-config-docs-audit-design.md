# Config × Docs 盤點與分工架構（design）

日期：2026-06-12
狀態：design（待使用者覆核 → 產出盤點報告 → 逐項確認後改）

## 目標

各 `conf/base/*.yaml` 註解非常密集，與 `docs/`、`README.md` 之間有重複、漂移與落點不清。本次盤點回答三問，並以一份「目標文件架構」作為判準：

1. **Q1 未說明**：哪些 yaml 設定沒有在活文件中說明。
2. **Q2 該搬移**：哪些說明放錯地方、依目標架構應搬到何處（須先有架構，再逐條論證）。
3. **Q3 錯誤/不一致**：哪些 yaml 註解寫錯、過時，或與 docs/README 矛盾。

## 仲裁原則：程式行為為真實來源

判斷「對不對」**不以 docs 為準，而以 `src/recsys_tfb/` 的實際行為與一致性不變量為準**。yaml 註解與 docs 都可能錯，code 才是仲裁者。Q3 尤其採三方對照：**註解 vs code vs docs**，標出寫錯 / 過時 / 互相矛盾。

## 三條底層原則

1. **單一正典管「權威且易變的細節」**：完整說明、易變具體（成員清單、門檻、代號定義）只有一處，其餘連過去。但**短的 orienting gloss 與安全警語允許重複**——條件是「短、陳述穩定/可推導的事實（非易變具體）、並指回正典」；尤其當「就地讀錯代價高（安全 gotcha）」或「有測試釘住一致」時，**應該**就地重複那句警語。單一正典是壓低漂移的手段，不是犧牲就地可讀性的目的。
2. **就近原則（proximity）**：會「跟著 code 一起變」的資訊（不變量邏輯、哪個值被 adapter pop 掉、什麼進 model_version hash），正典放離 code 最近處（該 `.py` 的 docstring）；yaml/docs 只引用。
3. **讀者路徑（reader path）**：理解一個設定的動線固定為 `README（找方向）→ pipeline 文件「關鍵設定」（讀懂這個 key）→ design-principles（為什麼）/ consistency.py（不變量細節）`。yaml 註解服務「正在編這個檔的 operator」，給「夠用 + 安全警語 + 指標」。

### 原則 #1 的放寬判準（決定 gloss 能否重複）

三個層級：**完整權威說明（full，只一處）／ orienting gloss・安全警語（可重複）／ pointer（任意多處）**。單一正典只管 full 與「易變具體」。

| 維度 | 適合放寬（可就地重述） | 維持嚴格（單一正典） |
|---|---|---|
| 事實穩定度 | 很少變（如「model_version 是 training block 的 hash」） | 易變具體（哪些 key 被 hash、確切門檻、A 代號定義文字） |
| 讀錯代價 | 就地讀錯會做出昂貴/靜默錯誤動作 → 值得放警語 | 讀錯只是少看到背景 |
| 重述形態 | 短 gloss / 警語（穩定、可推導） | 完整推導、成員清單、長解 |
| 是否有測試釘住 | 有 test 鎖住一致 → 重複很安全 | 沒測試、純靠人記得同步 |
| 讀者是否不同 | 為不同讀者翻譯措辭（operator vs 學習者） | 易變具體仍單一正典 |

一句話：**「短、穩定/可推導、指回正典」三者皆具 → 可重複；就地讀錯代價高或有測試保證一致時，更應該重複那句警語。**

## 各產物職責分工（charter）

### 設定檔（conf/）

| 產物 | 主要目的 | 擁有哪類資訊 | 不該放 | 指向誰 |
|---|---|---|---|---|
| `parameters.yaml` | 全域 / 跨 pipeline 設定 | `schema` 角色對應、`spark` 範本、`hive`、`logging`、`random_seed` | schema 角色抽象的「為什麼」長篇 | schema→design-principles §1；spark 生效值→`SPARK_CONF_DIR` |
| `parameters_<pipeline>.yaml` | 該 pipeline 的可調值本體 | 值 + 每個 key 的短 gloss（角色 + 必要安全警語 + 指標） | 完整作用/合法值表、不變量意義長解、版本影響規則長篇推導 | 該 pipeline 文件「關鍵設定」；不變量→consistency.py |
| `catalog.yaml` | dataset 註冊表 | dataset name → type / path / format | 「這張表幹嘛用」的業務說明 | 產出/消費它的 pipeline 文件 |
| `conf/spark-local/spark-defaults.conf` | 本機 spark 實際生效值 | 連線 / 記憶體實值 | — | （被 parameters.yaml spark 範本指向） |

### 文件（docs/ + README）

| 產物 | 主要目的 | 擁有哪類資訊 | 不該放 | 指向誰 |
|---|---|---|---|---|
| `README.md` | 入口 + 跨 pipeline 工作流 | 這是什麼、pipeline 總覽、lineage、端到端情境、「哪個檔設什麼」索引、文件地圖 | 單一 key 的逐項參考細節 | 各 pipeline 文件 |
| `docs/pipelines/<p>.md` | 該 pipeline 的 operator 手冊 | 指令與選項、節點流程、**「關鍵設定」＝該 `parameters_<p>.yaml` 每個 key 的 canonical 參考**（作用/合法值/預設/版本影響一句/交叉連結）、重跑語意 | 跨 pipeline 連動完整劇本、不變量代號定義 | change-guide（連動）、design-principles（為什麼）、consistency.py |
| `docs/design-principles.md` | 跨領域的「為什麼」 | schema 角色抽象、**三層版本 + model_version scope 規則表**、一致性不變量哲學、fit/transform、ModelAdapter、宣告式 catalog、生產限制 | 逐 key 表、操作步驟 | pipeline 文件、consistency.py、versioning.py |
| `docs/change-guide.md` | 情境式改動劇本 | 「要改 X → 動哪些檔的哪些 key → 重跑哪些 pipeline → 預期 bust 哪層版本」、**跨 pipeline 連動**（如 `sample_weight_keys`↔`carry_columns`↔`sample_group_keys` 對稱） | 概念背景 | pipeline 文件、design-principles |
| `docs/operations/<o>.md` | 環境與機制 | 本機 spark、切片、worktree、連線分層 | 旋鈕「意義」 | — |
| `docs/handbooks/*.md` | ML 方法自學 | GBDT / LTR / imbalance 方法背景 | repo 專屬設定 | （被 pipeline 文件一句連結引用） |

### 程式（真實來源仲裁者）

| 產物 | 它是什麼的正典 |
|---|---|
| `core/consistency.py` 模組 docstring | 不變量 legend（A1–A14、B1/B5）的唯一定義。yaml/docs 出現 `A7`、`A9b` 只能標代號 + 指這裡（指認本 key 受哪條約束 OK，重述定義文字不行） |
| `core/versioning.py::_model_version_payload` | 什麼進 model_version / 三層版本 bust 規則的唯一定義。design-principles §3 寫人讀版表格一次，他處引用 |

## 運作規則：資訊型別 → 正典（Q2 搬移量尺）

| 資訊型別 | 正典（唯一） | yaml 註解 | docs |
|---|---|---|---|
| key 作用 / 合法值 / 預設 | pipeline 文件「關鍵設定」 | 短 gloss + 指回 | 完整表 |
| 不變量代號 A7/A9b 的意義 | `consistency.py` docstring | 標代號 + 指認本 key 受其約束 | design-principles 講哲學 + 指 consistency.py |
| 進 model_version / bust 哪層 | `versioning.py` + design-principles §3 | 穩定 gloss；改錯代價高者保留安全警語（如「bust base_dataset_version、需重建」） | §3 規則表 |
| 跨 pipeline 連動 / 對稱 | change-guide | 一句約束警語（靜默 no-op 風險）+ 指過去 | change-guide 情境 |
| 設定怎麼推導（editor/suggest 工具） | README「設定怎麼來」+ 該 pipeline 文件 | 一句指向工具 | README + pipeline 文件 |
| ML 方法背景（LTR/imbalance） | `docs/handbooks` | 不放 | pipeline 文件一句連結 |

## 已確認決策

1. **「關鍵設定」section 即逐 key 正典**，沿用現有 pipeline 文件骨架，不另開獨立設定參考總表。
2. **`catalog.yaml` 納入盤點**，但偏結構、註解少 → 輕掃即可。
3. 「資訊型別→正典」對照表照上表（已含原則 #1 放寬）。

## 盤點方法

- **範圍**：9 個 `conf/base/*.yaml`（含 `catalog.yaml`，輕掃）；活文件＝`README.md`、`docs/pipelines/*`、`docs/operations/*`、`docs/change-guide.md`、`docs/design-principles.md`、`docs/handbooks/*`。`docs/superpowers/specs|plans/*` 視為歷史快照、不納入對照。
- **Q1**：逐一列出每個 yaml key/block → 比對活文件是否說明 → 以 `src/` 確認實際行為 → 產出「key → 文件落點(或無) → 缺口分級」。
- **Q2**：對每個「說明落點」依上方架構判定是否錯位 → 逐條給「現落點 → 應落點 → 依據哪條原則/哪一列」（套用原則 #1 放寬：該留的安全 gloss 不算錯位）。
- **Q3**：抽出註解中可查證主張（版本影響、不變量代號、「由 adapter pop 掉」、SoT 指向哪個 `.py`、預設/合法值）→ 回 `src` 驗 → 三方對照標 wrong / stale / 矛盾。

## 產物與流程

- 報告落點：`docs/config-docs-audit.md`（單一 markdown，三節分明，每筆附 `file:line` 與 code 證據）。
- 流程：本 spec 覆核 → 產出報告 → 使用者逐項確認 → 才動手改 yaml 註解 / docs。
- 分支：`feat/config-docs-audit` worktree；本任務不跑 Spark（純讀 `src/` + 寫 markdown/yaml）。
