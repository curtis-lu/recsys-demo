# recsys_tfb — 批次產品推薦排序框架

> 本文件為公司環境使用者 / 維運者 / 後續開發者的入口文件。
> 內容皆以目前 repo 的程式與設定為準（`src/recsys_tfb/`、`conf/`、`scripts/`）。
> 細節文件：
> - [docs/config-and-versioning.md](docs/config-and-versioning.md)：設定讀取規則、Schema 資料契約、版本 hash 規則
> - [docs/pipeline-runbook.md](docs/pipeline-runbook.md)：各 pipeline 與 scripts 操作、restart、promote、evaluation、錯誤排查
> - [docs/change-sop.md](docs/change-sop.md)：增加 feature / product / schema / training 設定的修改 SOP
> - [docs/metrics.md](docs/metrics.md)：評估指標（程式實際算什麼、輸出格式、報表分段）；概念語意見 [docs/metrics_concept_map.html](docs/metrics_concept_map.html)

---

## 0. 快速上手 (TL;DR)

**這個 repo 在做什麼？** 給銀行行銷團隊一張「每位客戶對每個金融產品的興趣分數排名表」，用來決定要主動聯繫誰、推哪一支產品。每月底拍一張客戶快照（snapshot）後跑這套 pipeline，產出寫進 Hive 表（欄位：`snap_date, cust_id, prod_name, score, rank`）。

**前置條件**：Python 3.10（`>= 3.10, < 3.12`）； Spark / Hive 的設定檔已於平台右上角資料源連線管理中設定。


```bash
# 第一次使用前（在 repo 根目錄、虛擬環境內）
pip install -e .
```

**最小可跑流程**（以 `2026-01-31` 月底快照為例）。請依「步驟一 → 步驟二 → 步驟三」順序完成；步驟一、二是**一次性 setup**（決定資料合約與 ETL SQL），日後標準週期只需重跑步驟三的 CLI。

### 步驟一：編寫配置檔 `conf/base/parameters.yaml`

這份檔案是後續所有 pipeline 的入口，定義「資料長什麼樣子」與「Hive 在哪裡」。三件事必須先決定：

1. **`hive.db`**：請改成你個人名下的開發 db（例如 `dev_<yourname>_recsys`），避免與他人共用造成相互覆寫。各環境的覆寫值可分別放在 `conf/local/parameters.yaml`（本機 / 個人開發）與 `conf/production/parameters.yaml`（驗證或正式區）；執行 pipeline 時用 `--env local` / `--env production` 切換，ConfigLoader 會把對應 env 的設定 deep-merge 覆寫 base（規則見 §5）。
2. **`schema.columns`**：定義 `time` / `entity` / `item` / `label` 等角色對應到實際欄位名稱（預設 `snap_date` / `cust_id` / `prod_name` / `label`）。這套命名是後續所有 table 之間的**資料合約** ── feature_table / label_table / sample_pool 都必須遵守，所有 join 與衍生表都吃這些 key。一旦決定就應該凍結，避免之後再動造成大規模重跑。
3. **`schema.categorical_values`**：列出你要建模的所有 item（例如所有金融產品代號）。這份清單是 item 宇集的**單一真實來源**（single source of truth）── 後續 `inference.products`、`sample_pool` 出現的 item、`label_table` 出現的 item 都必須與它一致；任何差異都會在 CLI 入口被一致性閘 fail-loud 擋下（§6 的 A4–A6 / B1）。

### 步驟二：編寫 ETL SQL（`conf/sql/etl/`）

framework 把上游資料的取得 / 變形分成三條獨立 ETL pipeline，各自產出一張下游必備的 Hive table。

1. **三條 ETL 各自的最低欄位要求**（欄位名必須對齊步驟一的 schema）：
   - **`feature_etl`** → `feature_table`：特徵表。欄位必須包含 schema 的 `time` & `entity`（預設 `snap_date, cust_id`），其餘為自由發揮的特徵欄。
   - **`label_etl`** → `label_table`：標籤表（ground truth，0/1 表示該客戶該月有沒有承作該產品）。欄位必須包含 schema 的 `time` & `entity` & `item` 以及 `label` 欄。
   - **`sample_pool_etl`** → `sample_pool`：訓練 / 評估的母體候選表。欄位必須包含 schema 的 `time` & `entity` & `item`，以及所有要拿來做分群抽樣的 `sample_group_keys` 欄位（例如 `cust_segment_typ`）。
2. **單條 pipeline 可包含多份 SQL**：每條 ETL 可以有多個 SQL 檔（例如 `feature_aum.sql` → `feature_sav.sql` → `feature_concat.sql`），一份 SQL 對應一張中介或最終 Hive table。會被下游讀取的 table 必須在 `conf/base/catalog.yaml` 註冊。
3. **重複執行的語意**：依各 SQL 設定的 `partition_by` 做 `INSERT OVERWRITE`（同一 partition 重跑會被覆寫，不會 append、不會留歷史副本）。重跑同一個 `snap_date` 是冪等的。
4. **執行順序與相依關係**：寫在對應的 `conf/base/parameters_<feature|label|sample_pool>_etl.yaml` 的 `depends_on` 欄位（例如「必須先執行 `feature_concat` 才能執行 `feature_table`」）。同 pipeline 內依宣告順序執行；跨 pipeline 由你照「步驟三」的呼叫順序控制。
5. **三張表怎麼被下游用**：
   - **`sample_pool`** → `dataset` pipeline 依 `sample_group_keys` 做 train / train_dev / val / test / calibration 的分群抽樣，產出各 split 的 `*_keys` table（記錄「哪些 `(snap_date, cust_id, prod_name)` 三元組屬於哪個 split」）。
   - **`feature_table`** → `dataset` pipeline 在 train 集上 fit 出 categorical 欄位的編碼字典與其他 preprocessor 元件，產出 `preprocessor` + `preprocessed_feature_table`（前處理完成的寬表）。
   - **`label_table`** → 抽樣與前處理都做完後，把各 split 的 `*_keys` join `preprocessed_feature_table` 與 `label_table`，產出 `*_model_input` table（training pipeline 直接吃這個）。

### 步驟三：執行 pipeline

預設範例都用 `--env local`（本機 dev cluster / 個人 db）。**所有 CLI 指令都沒有 `run` 子指令、也沒有 `--pipeline` flag**；指令名就是 pipeline 名。

```bash
# 1. Source ETL：三條獨立的 pipeline，沒有「一鍵 ETL」。
#    --target-dates 指定當月 snap_date，逗號分隔可一次跑多月。
python -m recsys_tfb feature_etl     --env local --target-dates 2026-01-31
python -m recsys_tfb label_etl       --env local --target-dates 2026-01-31
python -m recsys_tfb sample_pool_etl --env local --target-dates 2026-01-31

# 2. 抽樣 → 前處理 → 訓練。版本由 config 自動算 hash，不需手動指定。
python -m recsys_tfb dataset  --env local
python -m recsys_tfb training --env local
#    ↑ training 結束時 stdout 印一行：
#        model_version=a05d0244
#      8 碼 hex hash 即為這次訓出的 model_version（由 training 參數 +
#      base_dataset_version + train/calibration variant 一起決定）。
#      想看所有訓過的版本：`ls data/models/`。

# 3. 評估新模型（產 report.html，做為「要不要 promote 上線」的依據）
python -m recsys_tfb evaluation --env local --post-training \
    --model-version a05d0244
#    ↑ 開報表：data/evaluation/<model_version>/<snap_date>/report.html

# 4. 決定上線：手動切換 data/models/best symlink（不動舊版本目錄）
python scripts/promote_model.py a05d0244

# 5. 跑推論，把分數寫進下游用的排名表
python -m recsys_tfb inference --env local
#    結果：Hive 表 <hive.db>.ranked_predictions, snap_date='2026-01-31'

# 6.（選用）線上監控：讀 inference 結果再跑一次 evaluation，看當期實際品質
python -m recsys_tfb evaluation --env local
```

> **關於 `--env`**：所有 CLI 都吃 `--env`；預設值為 `local`，代表本機 / 個人 dev cluster。`--env production` 保留給未來上線正式區使用 ── ConfigLoader 會用 `conf/<env>/*.yaml` 對 `conf/base/` 做 deep-merge 覆寫（規則見 §5）。

**怎麼確認每步跑成功？**

| 跑完什麼 | 去哪看結果 / 怎麼驗證 |
|---|---|
| `feature_etl` / `label_etl` / `sample_pool_etl` | Hive 表 `<hive.db>.feature_table` / `label_table` / `sample_pool` 出現新 `snap_date` 分區 |
| `dataset` | `data/dataset/latest` symlink 更新；底下有 `preprocessor` / `*_model_input` 等檔 |
| `training` | stdout 印 `model_version=<8 碼 hex>`；`data/models/<model_version>/model.txt` 存在 |
| `evaluation --post-training` | `data/evaluation/<model_version>/<snap_date>/report.html` 可開啟，看 `overall_map` / `mAP@k` |
| `scripts/promote_model.py` | `readlink data/models/best` 指向你 promote 的 `model_version` |
| `inference` | `SELECT COUNT(*) FROM <hive.db>.ranked_predictions WHERE snap_date='2026-01-31'` 不為 0 |

任一步指令失敗時，程式會以 exit code 1 結束、把所有設定 / 資料不一致的問題一次列出（見 §9 常見錯誤）。每步的進階用法、選項、版本機制見後續章節。

---

## 1. 名詞速查

本文件中反覆出現的詞，先在這裡定義一次，後面章節不再重複解釋。

| 名詞 | 意思 |
|---|---|
| **snap_date** | 月底快照日（例如 `2025-01-31`）；所有資料以「該日這位客戶的狀態」為基準。 |
| **cust_id** / **entity** | 客戶 ID。預設欄位名 `cust_id`，可在 `schema.entity` 改。 |
| **prod_name** / **item** / **product** | 同一件事的三種講法：金融產品代號。預設欄位名 `prod_name`。 |
| **label** | 0 / 1，表示該客戶該月有沒有承作該產品。 |
| **score** / **rank** | 模型輸出的分數與「該客戶內」的排名。 |
| **segment**（`cust_segment_typ`） | 客群分類欄位（例如 VIP / 一般）；用於分群抽樣與分群評估。 |
| **sample_pool** | 「哪些 `(snap_date, cust_id, prod_name)` 三元組要進入訓練 / 評估」的候選集。 |
| **source ETL** | `feature_etl` / `label_etl` / `sample_pool_etl` 三個獨立指令的合稱；**沒有單一 `source_etl` 指令**。 |
| **dataset** / **training** / **inference** / **evaluation** | 四個主要 pipeline，各自獨立 CLI 指令，執行順序見 §0 / §3。 |
| **model_version** | 一次 training 的版本識別字串，格式為 **8 碼 hex hash**（例：`a05d0244`），由 training 參數 + `base_dataset_version` + `train_variant_id` +（選用）`calibration_variant_id` 一起 sha256 後取前 8 碼決定（見 §7）。training 結束時印在 stdout，也可 `ls data/models/` 看到所有版本。 |
| **promote** | 把 `data/models/best` symlink 切到某個 `model_version`。**inference 預設讀 `best`，所以不 promote 就不會生效**。手動跑 `scripts/promote_model.py`。 |
| **`data/models/best`**（線上版本指標） | symlink，指向「現在線上用的 `model_version`」。inference / evaluation 未帶 `--model-version` 時讀這個。 |
| **`latest` symlink**（最近一次產出指標） | `data/dataset/latest`、`data/models/latest` 等等，各層版本（`base_dataset_version` / `train_variant_id` / `calibration_variant_id` / `model_version`）都有一個 `latest`，指「該層最近一次跑出來的版本」。training CLI 的 `--base-dataset-version` / `--train-variant` / `--calibration-variant` 不帶時各自吃對應的 `latest`。**`latest` ≠ `best`**：`latest` 只是方便不必每次貼 hash，不代表線上用哪個。 |
| **`training_eval_predictions`** vs **`enriched_eval_predictions`**（兩張 Hive 表） | `training` 寫 test-set 預測到 `ml_recsys.training_eval_predictions`（供 `evaluation --post-training` 讀，做新模型驗收）；`evaluation` 跑完後把 prepared 預測（已 join label / segment、已 rank）持久化到 `ml_recsys.enriched_eval_predictions`（供之後 `--compare-only` 重用）。前者是 training 內部產出的原始預測、後者是 evaluation 已加工過的快取，兩者不互通。 |
| **`A1`–`A9` / `B1`**（一致性不變量代號） | 設定靜態閘 9 條規則（`A1`–`A9`）與資料閘 1 條規則（`B1`）的代號。違反時 fail-loud 訊息會引用代號，例如 `ConfigConsistencyError [A5]: ...`。一般跑流程不需記，遇到錯誤時對代號查 §6 或 [docs/config-and-versioning.md](docs/config-and-versioning.md) 即可。 |
| **fail-loud** | 設定或資料不一致時程式立刻 raise 並 exit 1，而非帶錯偷跑。錯誤訊息會一次列出所有問題，讓你一次修完。 |
| **HPO** / **trial** | Hyperparameter Optimization。用 Optuna 跑 N 次不同超參組合，每次叫一個 trial，最後挑指標最佳的那組。 |
| **calibration** | 機率校準。模型原始分數不一定接近真實機率（0.7 不一定真的「70 % 會買」），用 isotonic regression 對齊真實機率分布，供下游決策使用。 |
| **base_dataset_version** / **train_variant_id** / **calibration_variant_id** / **model_version** | 多層獨立 hash，讓你只改抽樣設定時不必重跑前處理。詳見 §7 版本管理。 |

---

## 2. 專案定位

這是一套**批次排序推薦框架**。問題形式固定為：

```
customer / entity  ×  product / item  ×  binary label  ->  ranking score
```

對每個 `(snap_date, cust_id)` 群組內的所有候選產品輸出分數並排名，供下游依排名做推薦優先順序。預設場景是**銀行金融產品推薦**（每月底 snapshot、客戶 × 多類金融產品 × 是否承作），但欄位命名與資料契約皆可設定化，可移植到其他「實體 × 品項 × 二元標籤 → 排序」的批次 ranking 場景（見 [docs/change-sop.md](docs/change-sop.md)）。

核心特性：

- **內建輕量 pipeline 框架**（`src/recsys_tfb/core/`）：靈感來自 Kedro 但更精簡，不需另外安裝 orchestrator（Airflow / Prefect / Ploomber 等）。內部結構見 §10。
- **多層版本管理**：訓練產出依 `base_dataset_version` / `train_variant_id` / `calibration_variant_id` 三層獨立 hash，**只改抽樣設定不會作廢前處理 artifact**，可重複利用 preprocessed 資料訓多個版本。細節見 §7。
- **設定與資料一致性閘**：跑 pipeline 前先驗所有 config（`A1`–`A9`）與 Hive 資料（`B1`）的不變量；違反時 fail-loud、一次列出所有錯，讓你一次修完。
- **建模技術**：LightGBM 訓練 + Optuna 超參搜尋（HPO），支援機率校準（calibration）與 per-segment / per-product 的 sample weight。

---

## 3. 標準執行流程

§0 是濃縮版；本節是完整版（含選項表、Evaluation 多模式、輔助 script 串接）。CLI 一律是 `python -m recsys_tfb <command> [--options]`，**沒有 `run` 子指令、沒有 `--pipeline` flag**。指令清單以 `src/recsys_tfb/__main__.py` 為準；輔助 scripts 以 `python scripts/<name>.py` 執行。

> 第一次跑前請注意：
> - **Source ETL 是三個獨立指令**（`feature_etl` / `label_etl` / `sample_pool_etl`），沒有「一鍵 ETL」。
> - **訓練完不會自動上線**。`training` 只是把新 `model_version` 產到 `data/models/<model_version>/`；inference 預設讀 `data/models/best` symlink。要讓新模型生效，必須跑 `scripts/promote_model.py <model_version>` 手動切換（步驟 5）。
> - **下方步驟 1a / 1b 是按需的輔助 script**（`suggest_categorical_cols.py` / `sampling_overrides_editor.py`），只在「新增 categorical feature」或「調整抽樣比例 / 冷門產品 sample weight」時需要；**首次跑可略過**。它們的輸出都要人工貼回 `conf/` 才生效，不會自動進入 pipeline。

### 標準一輪流程（以月底 snap_date 為例）

```bash
# 1. Source ETL：產出 feature_table / label_table / sample_pool（三個獨立指令）
python -m recsys_tfb feature_etl     --env local --target-dates 2026-01-31
python -m recsys_tfb label_etl       --env local --target-dates 2026-01-31
python -m recsys_tfb sample_pool_etl --env local --target-dates 2026-01-31

# 1a.（選用，僅新增/調整 categorical feature 時）掃「已存在」的表推測 categorical 欄位
#     掃描目標必須已存在：feature_etl 完成後的 feature_table，或某個既有上游來源表。
python scripts/suggest_categorical_cols.py <hive.db>.feature_table
#   -> data/profiling/<stem>_categorical.yaml  （人工檢視後貼回 parameters_dataset.yaml）

# 1b.（選用，僅調抽樣 / 冷門產品加權時）profile sample_pool → 瀏覽器編輯 → 產 YAML snippet
#     需 sample_pool_etl 已完成（sample_pool 已存在）。
python scripts/sampling_overrides_editor.py profile <hive.db>.sample_pool
#   -> data/profiling/sampling_overrides_editor.html  （瀏覽器編輯後 Export JSON）
python scripts/sampling_overrides_editor.py to-yaml data/profiling/sampling_overrides_export.json
#   -> 貼回 parameters_dataset.yaml (sample_ratio_overrides) /
#           parameters_training.yaml (sample_weights)

# 2. Dataset：一致性閘 → 抽樣切分 → 前處理 → 各 split model_input（版本由參數自動推導）
python -m recsys_tfb dataset --env local

# 3. Training：LightGBM + Optuna HPO，產出 versioned model + test-set 預測寫入
#    <hive.db>.training_eval_predictions（不會自動 promote）
python -m recsys_tfb training --env local
#    ↑ 結束時 stdout 會印 `model_version=<8 碼 hex>`（例：`a05d0244`）；
#      也可以用 `ls data/models/` 看到所有訓出來的版本目錄（按時間排序）。
#      把這個字串貼到下一步 `--model-version` 後面。

# 4. Evaluation（post-training）：用 training 剛產出的 test-set 預測做模型驗收
#    讀 <hive.db>.training_eval_predictions（而非 inference 的 ranked_predictions）。
#    產出 report.html 並把 eval_predictions 持久化到 <hive.db>.enriched_eval_predictions
#    （後續 --compare-only 會用到）。決定要不要 promote 就看這份報表。
python -m recsys_tfb evaluation --env local --post-training --model-version <model_version>

# 5. 手動 promote：建立 / 更新 data/models/best symlink
python scripts/promote_model.py <model_version>

# 6. Inference：對當期 snap_date 打分，寫入 ranked_predictions
python -m recsys_tfb inference  --env local

# 7. Evaluation（線上監控）：讀 ranked_predictions 算指標，產 report.html
python -m recsys_tfb evaluation --env local
```

步驟 1a / 1b 是**按需的準備 / 調參輔助步驟**（非每輪固定要跑）：只在新增 categorical feature 或調整抽樣 / sample weight 時需要。兩者都讀「已存在」的表/檔（`suggest_categorical_cols` 掃 `feature_etl` 後的 `feature_table` 或既有上游來源表；`sampling_overrides_editor` 掃 `sample_pool_etl` 後的 `sample_pool`），輸出皆為 `data/profiling/` 下的 snippet，**需人工貼回 `conf/` 對應檔案**後，後續 `dataset` / `training` 才會吃到。因此放在對應來源表已產出之後、`dataset` 之前。

### 指令選項（以 `__main__.py` 為準）

| 指令 | 選項 | 說明 |
|---|---|---|
| `feature_etl` / `label_etl` / `sample_pool_etl` | `--env/-e`、`--target-dates`、`--restart-from` | `--target-dates` 為逗號分隔日期；未在 config 設定 `target_dates` 時必填 |
| `dataset` | `--env/-e` | 每次都從參數重算版本，無版本選項 |
| `training` | `--env/-e`、`--base-dataset-version`、`--train-variant`、`--calibration-variant` | 三個 version 選項預設為對應 `latest` symlink |
| `inference` | `--env/-e`、`--model-version` | 未指定 `--model-version` 時讀 `models/best` |
| `evaluation` | `--env/-e`、`--model-version`、`--post-training`、`--compare <key>`、`--compare-only <key>` | `--post-training` 讀 `training_eval_predictions`（驗收新訓 model），否則讀 `ranked_predictions`（線上監控）；`--compare` / `--compare-only` 詳見下節 |

各 pipeline 在 CLI entry 會先跑 `validate_schema_config` 與 `validate_config_consistency`，任何設定矛盾會在跑 pipeline 前一次列出並以 exit code 1 結束（fail-loud）。操作細節（restart、promote 規則、evaluation 兩種模式）見 [docs/pipeline-runbook.md](docs/pipeline-runbook.md)。

### Evaluation 模式

`evaluation` 同時負責**新模型驗收**與**線上監控**，兩種來源用 `--post-training` 切換：

| 情境 | 指令 | 讀取的預測來源 | 產出 | 何時用 |
|---|---|---|---|---|
| **1. 新模型驗收**（§3 標準流程的步驟 4） | `evaluation --post-training --model-version <mv>` | Hive `ml_recsys.training_eval_predictions`（training pipeline 寫入的 test-set 預測） | `report.html`；同時把 `eval_predictions` 寫入 Hive `ml_recsys.enriched_eval_predictions`（供之後 `--compare-only` 重用） | training 完成後、promote 之前。看完報表決定要不要 `promote_model.py` |
| **2. 線上監控** | `evaluation` | Hive `ml_recsys.ranked_predictions`（inference 寫入） | `report.html` + `eval_predictions` 持久化 | inference 跑完、要回頭看當期實際分數分布／recall 時 |

> **新人讀到這裡就夠了**：標準流程跑這兩種；下方「進階：模型比較」是想把新模型跟舊版 / A/B / 外部專案並排對比時才需要。

#### 進階：模型比較（`--compare` / `--compare-only`）

| 情境 | 指令 | 讀取的預測來源 | 產出 | 何時用 |
|---|---|---|---|---|
| **3. 比較（一輪內同時跑）** | `evaluation [--post-training] --compare <key>` | 同情境 1 / 2（依 `--post-training`） | `report.html` + `report_comparison.html`（兩個 model 並排） | 想在跑當期 evaluation 的同時，也看新舊／A/B 兩個 model 的差異 |
| **4. 比較（用既有結果）** | `evaluation --compare-only <key>` | Hive `ml_recsys.enriched_eval_predictions`（**先前** evaluation 已持久化的當期結果） | 只有 `report_comparison.html` | 當期 `report.html` 已經跑過、只想多比一個 source；避免重算指標 |

`<key>` 必須事先在 `conf/base/parameters_evaluation.yaml` 的 `evaluation.compare_sources` 註冊。例（檔內已附說明）：

```yaml
evaluation:
  compare_sources:
    v_prev:                              # CLI flag 帶這個 key
      kind: model_version                # 比另一個我們自己的 model_version
      model_version: "abcdef12"               # 8 碼 hex hash
      # source 預設 enriched_eval_predictions（B 也跑過 evaluation 就用這個）；
      # 其他選項：ranked_predictions（B 只跑過 inference）|
      # training_eval_predictions（B 只跑過 training，用於 --post-training 比對）
      # source: ranked_predictions
      label: "v_prev (上一版)"
    ext_proj_x:
      kind: external_hive                # 比外部專案的預測表
      table: other_project.predictions
      label: "External Project X"
      columns: {cust_id: customer_id, snap_date: as_of_date,
                prod_name: item_code, score: pred_score}
      prod_mapping: {ext_fund_a: fund_stock, ext_fund_b: fund_bond}
      unmapped_policy: fail              # 或 drop
```

行為要點：

- `--compare` 與 `--compare-only` 互斥；同時帶兩個會 fail-loud。
- `--compare-only` 要求 Hive `ml_recsys.enriched_eval_predictions` 已經有對應 `(snap_date, model_version)` 分區（即同一個 `--model-version` 之前已用普通 `evaluation` 或 `evaluation --post-training` 跑過）；沒有時會 fail-loud（B4），訊息會告訴你要先跑哪個指令。
- `source` 預設 `enriched_eval_predictions`（A/B 對稱，B 也必須之前跑過 evaluation）。**若是從舊版升上來且原本 omit `source:`**，預設值已從 `ranked_predictions` 改掉 — 想保留舊行為要明確寫 `source: ranked_predictions`。
- 比較會把兩邊 restrict 成共同的 `(cust_id, snap_date, prod_name)` 集合再重排序；覆蓋率不滿時報表會顯示 partial-coverage 警告但不會失敗。
- 兩個 report 都寫到 `data/evaluation/<model_version>/<snap_date>/`：`report.html` 與 `report_comparison.html`。
- popularity baseline 是 `evaluation` pipeline 內部的一個節點（`compute_baseline_metrics`），由 `evaluation.baseline.lookback_months` 控制，與 evaluation 一起執行、寫進同一份 `report.html` 的 baseline 段。

---

## 4. Pipeline 與資料流

> 想看 framework 內部（`Node` / `Pipeline` / `Runner` / `Catalog` / `ConfigLoader` 怎麼實作）請跳到 §10 內部架構（進階）。本節聚焦在「資料怎麼從上游流到下游 artifact」。

### Pipeline 清單（`pipelines/__init__.py` 註冊）

`dataset`、`training`、`inference`、`evaluation`。Source ETL 走獨立的 `SQLRunner`（不在上述 registry，由 `feature_etl` / `label_etl` / `sample_pool_etl` 指令驅動）。

### 資料流與 lineage（含 pipeline 與 scripts）

```text
   公司上游來源表
        │ feature_etl / label_etl / sample_pool_etl
        ▼ (SQLRunner，CTAS/INSERT OVERWRITE + checks)
   feature_table   label_table   sample_pool
        │               │            │
        │               │            │   ── 選用/按需（來源表已產出後才能跑）──────────┐
        │               │            │   scripts/suggest_categorical_cols.py            │
        │（讀已存在的 feature_table 或既有上游表）────────►  → data/profiling/*.yaml      │
        │               │            │                                                 │
        │               │            │（讀 sample_pool）  scripts/sampling_overrides_   │
        │               │            └────────────────►  editor.py profile → 瀏覽器     │
        │               │                                編輯 → to-yaml → snippet       │
        │               │                                                               ▼
        │               │                       人工貼回 conf/base/parameters_dataset.yaml
        │               │                       (categorical_columns / sample_ratio_overrides)
        │               │                       與 parameters_training.yaml (sample_weights)
        │               │                                                               │
        └───────┬───────┴─────┬──────┘   ◄─── dataset/training 讀合併後的 parameters ────┘
                ▼             ▼
            ┌──────────────────────────────────────────────────────────────┐
            │ dataset  (validate_data_consistency → 抽樣切分 → fit/apply     │
            │          preprocessor → build_model_input per split)          │
            └──────────────────────────────────────────────────────────────┘
                │ preprocessor / category_mappings / *_model_input
                ▼                                              （版本: base / train_variant / calibration_variant）
            ┌──────────────────────────────────────────────────────────────┐
            │ training (cache → Optuna HPO → (calibration) → predict_test → │
            │          compute_test_mAP_spark → diagnostics → mlflow)       │
            └──────────────────────────────────────────────────────────────┘
                │ data/models/<model_version>/{model.txt,best_params,        training_eval_predictions
                │ evaluation_results,manifest}                               (Hive)
                ▼
        scripts/promote_model.py  (手動：比對 evaluation_results.json mAP)
                │ data/models/best -> <model_version>  (symlink)
                │                                              training_eval_predictions
                │                                              (Hive — training 寫入 test-set 預測)
                │                                                     │
                ▼                                                     │
            ┌──────────────────┐                                       │
            │ inference        │                                       │
            │ → ranked_        │                                       │
            │   predictions    │                                       │
            └──────────────────┘                                       │
                │ ranked_predictions                                   │
                ▼                                                     ▼
            ┌──────────────────────────────────────────────────────────────┐
            │ evaluation                                                   │
            │  • 預設讀 ranked_predictions（線上監控）                       │
            │  • --post-training 讀 training_eval_predictions（新模型驗收）  │
            │  • 內含 popularity baseline（compute_baseline_metrics）       │
            │  • 持久化 eval_predictions 到 Hive ml_recsys.enriched_eval_predictions │
            │  • --compare / --compare-only：產 report_comparison.html      │
            │  prepare_eval_data → compute_metrics → report.html            │
            └──────────────────────────────────────────────────────────────┘
```

Lineage 對照表（artifact → 產生者 → 消費者 → 對應版本）：

| Artifact | 產生者 | 消費者 | 版本層級 |
|---|---|---|---|
| `data/profiling/<stem>_categorical.yaml` | `scripts/suggest_categorical_cols.py` | 人工貼回 `parameters_dataset.yaml` | 無（離線輔助）|
| `data/profiling/sampling_overrides_editor.html` / `_export.json` | `scripts/sampling_overrides_editor.py profile` / 瀏覽器 | `scripts/sampling_overrides_editor.py to-yaml` | 無（離線輔助）|
| `feature_table` / `label_table` / `sample_pool`（Hive）| `feature_etl` / `label_etl` / `sample_pool_etl` | `dataset`、`evaluation` | 由上游 snap_date 分區 |
| `preprocessor` / `category_mappings` / `val/test_model_input` | `dataset` | `training` | `base_dataset_version` |
| `train/train_dev_model_input` | `dataset` | `training` | `base` + `train_variant_id` |
| `calibration_model_input` | `dataset`（calibration 啟用）| `training` | `base` + `calibration_variant_id` |
| `data/models/<mv>/{model.txt,best_params,evaluation_results,manifest}` | `training` | `promote_model.py`、`inference`、`evaluation` | `model_version` |
| `training_eval_predictions`（Hive）| `training` | `evaluation --post-training`、`compute_test_mAP_spark` | `model_version` |
| `data/models/<mv>/diagnostics/{feature_statistics,feature_importance,shap_diagnostics}.json` + `*.png` | `training`（diagnostic nodes）| MLflow（`log_experiment` 上傳）/ 人工 | `model_version` |
| `data/models/best`（symlink）| `scripts/promote_model.py`（手動）| `inference` / `evaluation`（未指定 `--model-version` 時）| 指向某 `model_version` |
| `ranked_predictions` / `score_table`（Hive）| `inference` | `evaluation`（預設模式）| `model_version` |
| `ml_recsys.enriched_eval_predictions`（Hive）| `evaluation`（每次都 persist；catalog 自動寫入）| `evaluation --compare-only` | `(model_version, snap_date)` 分區 |
| `data/evaluation/<mv>/<snap_date>/report.html` | `evaluation` | 人工 / 監控 | `model_version` |
| `data/evaluation/<mv>/<snap_date>/report_comparison.html` | `evaluation --compare` / `--compare-only` | 人工（A/B 對比） | `model_version` |

### Training 診斷產物（feature stats / native importance / SHAP）

`training` 在 `compute_test_mAP_spark` 後、`log_experiment` 前跑三個純計算 node（不碰 Spark），產物寫到 `data/models/<mv>/diagnostics/` 並由 `log_experiment` 整包上傳 MLflow（另記 `n_dead_features` / `n_high_null_features` / `n_single_value_features` scalar）：特徵基本統計、LightGBM split+gain importance、SHAP（全域 + per-item + 代表性個例 + PNG）。設定在 `parameters_training.yaml` 的 **top-level `diagnostics:` block**（與 `mlflow`/`cache` 同層，刻意不放進 `training:` → 不影響 `model_version`）。

SHAP 旋鈕（`diagnostics.shap`）：

| 參數 | 作用 |
|---|---|
| `enabled` | SHAP 總開關（最重的診斷；`false` 時整個 node 略過） |
| `sample_rows` | 從 test set 抽多少列算 SHAP；成本隨「樣本數 × 樹數」線性成長，**唯一的成本主旋鈕** |
| `top_k` | 全域 / per-item 各保留前 K 重要特徵（只影響輸出大小，不影響計算量） |
| `n_examples` | 取預測分數最高/最低各 N 筆輸出逐特徵 SHAP（「為何這客戶分數高/低」），並保證每產品至少一筆高分個例（含稀有產品） |
| `min_rows_per_item` | per-item 分層每產品下限（不足則全取）；同時是 `low_coverage` 旗標門檻 |
| `max_budget` | `sample_rows × 樹數` 上限，超過自動降抽樣並 warn |

效率關鍵：只呼叫**一次** `TreeExplainer.shap_values`（`tree_path_dependent`，無 background dataset），全域 / per-item / 個例全從同一矩陣聚合，不會因 22 類產品而重算。per-item 為「族群代表」抽樣（產品內隨機，不偏高分）；「為何被推薦」的高分故事看 `n_examples` 那段。（`diagnostics.feature_stats` 另有獨立的 `sample_rows`，預設 500000，針對 train parquet，與 SHAP 的不互通。）

---

## 5. 設定讀取邏輯（摘要）

`ConfigLoader(conf_dir, env)`（`core/config.py`）：

1. 讀 `conf/base/*.yaml`，再讀 `conf/<env>/*.yaml`。
2. 對每個檔名（stem），用 env 的內容對 base 做 **deep-merge override**（dict 遞迴合併，非 dict 直接取代）。
3. `get_parameters()` 把所有 `parameters.yaml` 與 `parameters_*.yaml` 合併成一包 parameters。
4. `get_catalog_config()` 對 `catalog.yaml` 做 `${...}` runtime placeholder 替換（支援巢狀 key，如 `${hive.db}`、`${base_dataset_version}`）。

> ⚠️ 多個 `parameters_*.yaml` 合併時**沒有保證的穩定優先順序**（程式以 set 走訪 stem）。請避免不同 parameter 檔案出現同名 key；若無法避免，務必確認 deep-merge 結果是你要的。

完整規則（含 placeholder、env overlay 行為）見 [docs/config-and-versioning.md](docs/config-and-versioning.md)。

---

## 6. Schema 與資料契約（重點）

> 何時讀本節：**要新增 / 修改 feature 欄位、新增產品、改抽樣 group、或遇到「Data consistency check failed」錯誤訊息時**。只是按既有設定跑流程不需要讀。

`schema.columns`（`conf/base/parameters.yaml`）定義角色欄位，預設值見 `core/schema.py`：

| 角色 | 預設欄位 |
|---|---|
| `time` | `snap_date` |
| `entity` | `[cust_id]`（永遠 normalize 成 list）|
| `item` | `prod_name` |
| `label` | `label` |
| `score` | `score` |
| `rank` | `rank` |

- `identity_columns` 為**程式推導**：`[time] + entity + [item]`，預設 `[snap_date, cust_id, prod_name]`。
- 進入 dataset pipeline 的 `feature_table`、`label_table`、`sample_pool` 都必須遵守這套欄位命名。
- `sample_pool` 至少要含 `identity_columns`；若 sampling/group/carry 用到 `cust_segment_typ`、`label` 等欄位，這些欄位也必須存在於 `sample_pool`。
- `schema.item`（`prod_name`）必須是 categorical feature，且必須出現在 `schema.categorical_values`。
- `inference.products` 必須與 `schema.categorical_values[<item>]` 為**相同集合**。
- `sample_pool` 的 item 覆蓋率必須**等於**宣告產品集合（雙向集合相等）。
- `label_table` 不能出現未宣告產品。
- **新增 / 移除 product 不能只改 `schema.categorical_values` 與 `inference.products`**：`conf/sql/etl/label/*.sql`（`candidate_prod` CTE 與 `CASE WHEN apply_type` mapping）、`conf/sql/etl/sample_pool/sample_pool.sql`（`prod` CTE）都有 hardcoded 產品清單；新增 label source category 還要動 `conf/base/parameters_label_etl.yaml` 與 `label_table.sql`。完整 SOP 見 [docs/change-sop.md §「增加 product」/「移除 product」最小檢查清單](docs/change-sop.md)。
- train / calibration / val / test 的 snap_date 集合**兩兩不可重疊**。
- `feature_table` 必須涵蓋 dataset 用到的所有 snap_date（train ∪ calibration ∪ val ∪ test）。
- ranking task 中 item 欄位**必須留在 feature columns**（即 `prod_name` 要在 `dataset.prepare_model_input.categorical_columns`），否則模型無法區分同一 customer 下不同 product，HPO mAP 會塌成常數。

這些不變量由 `core/consistency.py`（設定靜態閘 A1–A9）與 dataset pipeline 第一個節點 `validate_data_consistency`（資料閘 B1）強制；違反時 fail-loud。完整清單與錯誤訊息對照見 [docs/config-and-versioning.md](docs/config-and-versioning.md)。

---

## 7. 版本管理（重點）

> 何時讀本節：**要評估「改某個參數會 bust 哪一層版本、要不要重跑 dataset」、或想理解為什麼 training 不會自動上線時**。一般跑流程不需要記住表格內容。

目前是**多層 hash 版本機制**（`core/versioning.py`），不是單層 `dataset_version`。dataset pipeline 每次依參數重算版本並更新 `latest` symlink；training 產出 versioned model 目錄但**不**自動 promote。

| 版本 | 由什麼決定 | 影響的 artifact |
|---|---|---|
| `base_dataset_version` | 非抽樣 dataset 參數 + canonical schema（含 `categorical_values`）+ feature_table fingerprint（欄位名+型別，**有序**）| preprocessor、category_mappings、preprocessed_feature_table、val/test model_input |
| `train_variant_id` | train 抽樣設定：`sample_ratio`、`sample_ratio_overrides`、`sample_group_keys`、`train_dev_ratio` | train / train_dev model_input |
| `calibration_variant_id` | calibration 抽樣設定（僅在啟用 calibration 時）| calibration model_input |
| `model_version` | model-defining training 參數（`training:` block）+ `base_dataset_version` + `train_variant_id` +（選用）`calibration_variant_id` | model.txt、best_params、evaluation_results、manifest |

關鍵規則：

- `training:` block 進 `model_version` hash；其中 `algorithm_params` 的 `verbosity`、`log_period`、`num_threads` **不**影響 `model_version`。
- `spark`、`mlflow`、`cache`、`diagnostics` 等 top-level ops-only 設定**不**影響任何版本（`model_version` 只雜湊 `training:` block）。
- `training.sample_weights` 屬 `training:` block → **改它會 bust `model_version`，但不會改 `train_variant_id`**。
- `dataset.carry_columns` 不是抽樣 key → 改它會 **bust `base_dataset_version`**（parquet schema 變）。
- `sample_group_keys` 同時屬 train 與 calibration 抽樣 → 改它會同時改 `train_variant_id` 與 `calibration_variant_id`，但不改 `base_dataset_version`。
- `manifest.json` 記錄 `version` / `pipeline` / `created_at` / `git_commit` / `parameters` / 各層版本 / `artifacts` 等 lineage。

哪些修改改哪個版本的完整表格見 [docs/config-and-versioning.md](docs/config-and-versioning.md) 與 [docs/change-sop.md](docs/change-sop.md)。

---

## 8. 輔助 Scripts

只列與公司流程相關的 scripts（皆為 standalone Typer / argparse 工具，不屬 production DAG，但屬建模流程一環）。詳細選項與流程見 [docs/change-sop.md](docs/change-sop.md)。

### `scripts/suggest_categorical_cols.py`

```bash
python scripts/suggest_categorical_cols.py ml_recsys.feature_table   # Hive table
python scripts/suggest_categorical_cols.py /path/to/x.parquet        # 或 parquet 路徑
```

掃 Hive table 或 parquet 推測 categorical 欄位：string / bool 直接視為 categorical；低 cardinality numeric（預設 nunique ≤ `--max-cardinality 20`）也建議為 categorical。輸出 YAML snippet 到 `data/profiling/<stem>_categorical.yaml`，**人工檢視後貼進** `conf/base/parameters_dataset.yaml` 的 `categorical_columns`。**用於定義 / 新增 categorical feature 時。** 透過 `spark.table()` / `spark.read.parquet()` 讀**已存在**的表/檔，故掃描目標必須先存在——`feature_etl` 完成後的 `feature_table`，或某個既有上游來源表；不能在來源表尚未產出前執行。

### `scripts/sampling_overrides_editor.py`

```bash
python scripts/sampling_overrides_editor.py profile ml_recsys.sample_pool   # 或 parquet 路徑
python scripts/sampling_overrides_editor.py to-yaml data/profiling/sampling_overrides_export.json
```

`profile`：對 `sample_pool` 中 train snap_dates 的 per-`cust_segment_typ` × `prod_name` 算 positive/negative，依 target neg:pos 與 cold-product 公式給建議值，輸出 self-contained HTML editor。瀏覽器編輯 ratio / weight 後 Export JSON。`to-yaml`：把 JSON 轉成兩段 sparse YAML（會重用一致性 predicate 做 A5 / A9 驗證，未宣告產品 fail loud）：

- `dataset.sample_ratio_overrides` → 貼回 `conf/base/parameters_dataset.yaml`；key 格式 `"<cust_segment_typ>|<prod_name>|0"`（label 分量固定 `0`，代表 downsample 負例）。
- `training.sample_weights` → 貼回 `conf/base/parameters_training.yaml`；key 格式 `"<cust_segment_typ>|<prod_name>"`。

**用於調整 downsampling ratio / 冷門產品 sample weight 時。** 版本影響：`sample_ratio_overrides` 改 `train_variant_id`（需重跑 dataset）；`sample_weights` 改 `model_version`（不需重跑 dataset）。

### `scripts/promote_model.py`

```bash
python scripts/promote_model.py <model_version>      # 指定版本
python scripts/promote_model.py                      # 自動選 overall_map 最高
python scripts/promote_model.py --dry-run            # 只列各版本比較，不 promote
```

手動建立 / 更新 `data/models/best` symlink。promote 前檢查必要 artifact（`model.txt`、`best_params.json`、`evaluation_results.json`），缺則報錯。自動選版時依各版本 `evaluation_results.json` 的 `overall_map` 取最高。**training 完成後必須執行此步，inference 預設模型才會切換。**

---

## 9. 常見錯誤（速查）

訊息欄是「stderr / 拋出的 exception 開頭」實際樣式（具體欄名 / 路徑會依設定變動）。一致性閘錯誤皆會以 exit code 1 結束、把所有問題一次列出，可一次修完再重跑。

| 你看到的訊息 / 症狀 | 多半原因 | 下一步動作 |
|---|---|---|
| `inference` 啟動即 raise `FileNotFoundError` / `data/models/best` 不存在或不是 symlink | training 後忘記 promote | 跑 `scripts/promote_model.py <model_version>`；不確定要 promote 哪個版本就 `python scripts/promote_model.py --dry-run` 看各版本 mAP 後再選 |
| `ConfigConsistencyError: feature_table missing required snap_dates: [...]` | `feature_table` 缺 dataset 要用到的某個 `snap_date` 分區 | 確認 `feature_etl --target-dates` 是否涵蓋 `parameters_dataset.yaml` 的 train / val / test / calibration 所有日期；缺哪個就補哪個 |
| training stdout 每個 Optuna trial 印出的 mAP 幾乎一樣（例如全部 = popularity baseline） | `prod_name`（item）沒列入 `dataset.prepare_model_input.categorical_columns`，模型看不到「這是哪個產品」這個 feature，所以同一客戶下所有產品分數相同 | 在 `conf/base/parameters_dataset.yaml` 的 `categorical_columns` 加入 `prod_name`，重跑 `dataset` 與 `training`（會 bust `base_dataset_version`）|
| `ConfigConsistencyError: inference.products disagrees with schema.categorical_values[<item>]` | 兩處宣告的產品清單不一致 | 編輯 `conf/base/parameters_inference.yaml` 的 `inference.products` 與 `conf/base/parameters.yaml` 的 `schema.categorical_values.<item>`，使兩者集合相等 |
| `DataConsistencyError: Data consistency check failed: sample_pool item set != declared products` 或 `label_table contains unknown items: [...]` | sample_pool 的 item 集合不等於宣告產品；或 label_table 出現未宣告產品 | sample_pool 多半是 `sample_pool.sql` 的 `prod` CTE 漏改、label_table 多半是 `label_<source>.sql` 的 `candidate_prod` / `CASE WHEN` 漏改；要正式新增 / 移除產品照 [docs/change-sop.md「增加 product」/「移除 product」最小檢查清單](docs/change-sop.md) 走，避免只改一半 |
| dataset 啟動即 raise 報缺 identity / group / carry 欄位（例如 `sample_pool missing column: cust_segment_typ`）| `sample_pool` 沒帶 `sample_group_keys` / `carry_columns` 用到的欄位 | 在 `sample_pool_etl` 的 SQL 把缺的欄位 SELECT 出來再重跑 |
| ETL 報 `restart_from='...' not found in tables` | `--restart-from` 帶的 table 名拼錯 | 對齊 `conf/base/sources/<etl>.yaml` 的 `tables[].name` 抄一次（區分大小寫）|
| training 抱怨 cache 行為異常 / partial cache | `cache.root` 不可寫；或上次 run 中斷留下沒 `_SUCCESS` 的目錄 | 沒 `_SUCCESS` 的目錄會自動清掉重建；若仍然失敗，檢查 `parameters_training.yaml` 的 `cache.root` 是否指到一個可寫的本機路徑 |

完整排查步驟與更詳細的錯誤對照表見 [docs/pipeline-runbook.md](docs/pipeline-runbook.md)。

---

## 10. 內部架構（進階）

> 想改 framework 本身、或想理解 pipeline 怎麼跑起來時才需要讀。**只是按既有 pipeline 跑流程不需要看本節。**

### 框架元件（`src/recsys_tfb/core/`）

- **`Node`**（`core/node.py`）：包一個 function，宣告 `inputs` / `outputs` 名稱。
- **`Pipeline`**（`core/pipeline.py`）：一組 Node，依資料依賴做 Kahn 拓樸排序；**獨立的零入度節點按 list 宣告順序執行**（所以 dataset 把一致性閘放第一個是有意義的）。
- **`Runner`**（`core/runner.py`）：依拓樸順序逐一執行 Node；輸入名稱前綴 `@` 代表傳入 catalog dataset handle（而非載入資料）；中間 `MemoryDataset` 用完即釋放。
- **`DataCatalog`**（`core/catalog.py`）：依 `catalog.yaml` 建立 dataset 實例（`HiveTableDataset` / `ParquetDataset` / `JSONDataset` / `ModelAdapterDataset` / `PickleDataset` / `TextDataset`）；存到未註冊名稱時自動建 `MemoryDataset`。
- **`ConfigLoader`**（`core/config.py`）：讀取與合併 YAML，見 §5。
