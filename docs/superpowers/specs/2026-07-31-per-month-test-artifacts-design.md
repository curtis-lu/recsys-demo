# Per-month test 產物：決策推導與 Phase B 前期調查

> **這份文件不是建置計畫。** 可建置的 spec 是 **issue #128**（Phase A：cache 佈局、cache node
> 契約、predict 逐月跳過、evaluation 月份把關），本檔不複述它。
>
> 本檔保留兩樣 #128 不承載、但重查一次很貴的東西：
> 1. **為什麼 per-month 取代視窗模型**的推導（決策記錄將另立 ADR-0003，本節是其素材）；
> 2. **Phase B（月份相依的診斷搬到 evaluation）的前期調查**——尚未開票，開票前先讀這段。
>
> 取代 spec #123 的「PR ②」整段。ADR-0001／#125、ADR-0002／#126 不受影響。

## 決策：per-month 取代視窗模型

使用者定調：**test 的每個 snap_date 應該獨立、互不干擾**。理由是這符合實際的機器學習實驗
過程——test dataset 本來就會因為新資料可得而陸續更新，而 evaluation 每次只針對新的 snap_date
做評估。月份不是一個集合參數，是一個個各自獨立的評估對象。

視窗模型（`test_windows/20260131_20260228/`）把「有哪些月」壓成一個複合鍵，於是「加一個月」
必須讓整個複合鍵失效、整批重抄。per-month 把月份自己當鍵，加一個月就是多一個鍵。

**查證過沒有任何情境是視窗模型比較安全的**：

| 情境 | 視窗模型 | per-month |
|---|---|---|
| 加月份 | 整份重抄（正確但浪費） | 只抄新月 |
| 移除月份 | 新目錄、重抄 | 舊目錄變孤兒、不被讀 |
| 舊月資料回補 | **一樣 stale**（視窗名沒變） | 一樣 stale（靠重算旗標） |

第三列是關鍵：兩者一樣壞，都得靠重算旗標。per-month 完全涵蓋視窗模型要解的問題，並把
複製成本從 ∝N 變成 ∝1。

### 貫穿全域的身分規則

> **依賴 test 資料的產物，身分是 `(model_version, snap_date)`；不依賴的，身分是 `model_version`。**

「同一個 `model_version` 在不同月份有不同的診斷產物」不是不一致，是正確的——就像 evaluation
報表本來就落在 `data/evaluation/<model_version>/<YYYYMMDD>/`。**不為此新增任何設定鍵**：月份
的唯一來源就是 `snap_date` 本身。

### spec #123 Out of Scope 那兩條理由為何不再成立

- 「多出半完成的月份等中間狀態」——當時設想的形狀應是「單一大目錄內做 per-month 標記」。
  改成**一月一目錄、各自 `_SUCCESS`** 之後，半完成的月份反而比原設計更容易辨識，且既有的
  partial-cache recovery（有目錄、無 `_SUCCESS` → 清除重建）直接適用，粒度剛好。
- 「省下的只是一次本機複製」——低估了，因為當時沒把 predict 也 ∝N 算進去。

### 順序約束維持不變：cache 改動仍須早於 #125

若 #125 先落地而 cache 仍是整批一份：加月份不再翻 `base_dataset_version` → cache 路徑不變 →
`_SUCCESS` 還在 → cache 命中 → 新月份永遠不會被複製進來，**而且不報錯**。
「先架護欄，再拆牆」在 per-month 模型下同樣成立。

---

## Phase B 前期調查：月份相依的診斷搬到 evaluation

**尚未開票。** 開票前先跑一輪 `/grill-with-docs`——這是 pipeline 邊界題，不是照做題。

### 接縫是既有依賴自己畫出來的

training 現有 7 個診斷 node，依賴關係自動分成兩堆：

| 留在 training（鍵＝`model_version`） | 搬到 evaluation（鍵＝`(model_version, snap_date)`） |
|---|---|
| `compute_feature_statistics`（吃 train handle） | `compute_shap_diagnostics` |
| `compute_feature_importance`（純模型） | `select_shap_population` |
| `compute_gain_ledger`（純模型） | `compute_quadrant_profiles` |
| | `compute_quadrant_cases` |

右欄四個全部依賴 test 資料，左欄三個都不依賴。

### 為什麼不是 `--from-node`

不是慢或醜，是**它會靜默覆寫**。`diagnosis/model/paths.py` 的 `diagnostics_dir()` 解析到
`data/models/<model_version>/diagnostics/`，**全檔沒有 `snap_date` 這一層**。用
`--from-node compute_shap_diagnostics` 為新月份重跑，產物會直接蓋掉前一個月的 SHAP 圖與
`per_quadrant.json`——得到的不是「每月各有診斷」，是「最後跑的那個月贏」。

而且 node 本身**無從得知現在是哪個月**：training 沒有 `--snap-date` 旗標。要補齊就得
「training 加 `--snap-date` ＋ `diagnostics_dir()` 加 snap_date 層 ＋ handle 選月邏輯」——
做完這三件事，等於在 training 裡把 evaluation 的參數化重蓋一遍。

### 成本盤點

**便宜的部分**：

- **業務邏輯不用搬**：模組已在 `src/recsys_tfb/diagnosis/model/`，不在 `pipelines/training/`。
  搬的只是 pipeline 接線。
- **載入 model 零成本**：catalog 的 `model` 條目是 `data/models/${model_version}/model.txt`，
  evaluation 早就有 `model_version` 在 runtime params → node inputs 加一個 `model` 即可。
- evaluation 已有 Spark（`select_shap_population` 需要）、已有
  `data/evaluation/<model_version>/<YYYYMMDD>/` 的產物慣例。

**真正的成本**（第 2 項最尖）：

1. `diagnostics_dir()` 要分家——`persist_sample_weight_report` 也用它（取 `.parent`），
   但那是 model-intrinsic 的。
2. **SHAP 需要 driver-local parquet**：它刻意走 pyarrow 只讀分區欄做分層（原註解
   「避免全量物化 test」），而 evaluation 沒有 cache node。要嘛 evaluation 自己也有一個，
   要嘛 cache node 搬到共用位置——**會碰到「零跨 pipeline import」這條目前 100% 成立的邊界**
   （已查證：各 pipeline 只 import 自己，共用邏輯一律在 `core/`／`utils/`）。設計這一題時
   建議先套 `/codebase-design` 的 deep-module 詞彙談 seam 放哪。
3. **只能在 `--post-training` 模式存在**：default 模式讀 `ranked_predictions`，沒有 test
   model_input。evaluation 的 node set 會再長一個模式分支。
4. **MLflow 語意**：`log_experiment` 現在把 SHAP／象限／cases manifest 一起 log。搬走後這些
   要嘛不進 MLflow、要嘛 evaluation 也得開 MLflow run。

### Phase A 期間 SHAP 的狀態（暫時措施，不是答案）

#128 期間 SHAP 維持今日語意完全不變（讀所有設定月份的聯集），因此其產物仍只由
`model_version` 鍵定、內容隨月份累積而變——既不是單月、也不是穩定的模型屬性。

**此期間不建議重跑 full training**；包裝好的動線只跑 predict 節點切片（實測 3 / 21 nodes），
不會觸發診斷。
