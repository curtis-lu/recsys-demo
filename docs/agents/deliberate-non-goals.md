# 刻意不做的事（給 AI agent 的地雷圖）

這份檔案只收一種東西：**「看起來該修、但刻意不修」的決定，以及它背後的理由。**

為什麼需要它：這類決定在程式碼裡看不見。一個 agent 讀到「這個 config 鍵缺了會 `KeyError`，太嚴格了」，
會很自然地想把它改寬鬆——除非有人告訴它那是使用者拍板的。**被延後的事幾乎都有原因**
（依賴未定、等公司環境資料、等使用者決策），「順手做掉」反而添亂。

## 怎麼用這份檔案

- **動到某一區之前掃一眼該區**。命中條目 → 那件事要先問，不要自己做。
- **狀態、票號、進度不在這裡**。那些查 `gh issue list` / `gh pr list`（見 `docs/agents/issue-tracker.md`）。
  這份檔案只寫「為什麼不做」，不寫「做到哪了」——寫了就會腐爛。
- **條目失效就刪**。使用者改變主意、或延後條件達成後，這條就沒有存在意義了。

---

## 一、使用者已裁決，不要「修好」它

| 事項 | 裁決 | 別做什麼 |
|---|---|---|
| `dataset.train_snap_dates` 必填 | 使用者 2026-08-04 明示。缺鍵會在 CLI 建月份計畫時就 `KeyError`（Spark 還沒開工），不是等到 `apply_preprocessor_to_features` 才爆 | 看到「CLI 對缺鍵太嚴格」想改成 `.get()` 寬鬆版 → 這是刻意的 |
| training 側 `overall_map` 跨月合併 | 它是跨所有累積 test 月份的加權合併值，而 `model_version` 身分裡已沒有 test 月份 → 同一個鍵的數字會隨月份累積而變。使用者傾向逐 `snap_date` 區隔，但**決定先 grill 不先做** | 別自己開一輪、更別直接實作。它與「SHAP／象限診斷搬到 evaluation」是同一個接縫問題，要併進同一輪 grill → spec。過渡用的「A 軌」（涵蓋月份＋各月 n_queries 進 `evaluation_results`、promote 排名時 WARN）也刻意還沒做 |
| 診斷框架 `score_shift` | 使用者 2026-07-22 明示暫緩，改先清掉舊的 score-offset 程式碼 | 規劃檔與 `scripts/per_item_score_shift_*.py` spike 腳本刻意保留當日後評估依據，不要當成死碼清掉 |
| 評估報表「每-query 正例數分佈」 | 使用者明示跳過 | — |
| 報表呈現層的其餘調整 | 使用者：「報表內容先不調整，等有明確反饋再一起改」 | 別因為讀者 subagent 提了意見就動 |
| `_format_slice_plan` 的「將(重)訓」標註（Tier 0） | 刻意撤回：`model` 是 training-only dataset，泛用標註只會在 training 觸發，而那裡已經有 WARN，故冗餘 | 勿重提實作 |
| shaprx（SHAP-on-loss 開源套件構想） | 使用者 2026-07-07 明示未定案、擱置。規劃記錄在 `~/projects/shaprx` | 任何新規劃**不得引用它當既有資產或邊界** |
| `calibration_snap_dates: []` 不比照 A23 擋掉 | 空清單時 `select_calibration_keys` 整池照收（`restrict_to_months_or_all`），校準集會收進 train 與 test 月份、該 test 月的評估變成 in-sample，而 A24 對空集合永遠成立、不出聲。與 A23 擋掉的 `train_snap_dates: []` 是同一個洞。使用者 2026-08-19 決定不擴，理由是 calibration 未來要移除 | 別順手把 calibration 加進 `train_snap_dates_errors`。也別記到 `--only-test-months` 頭上——旗標跳過那個 node，反而是比較安全的一邊，全量跑才會真的撞。calibration 移除後這條就可以刪 |
| 架構約束的例外登記（R1–R4） | 加一筆必須先問使用者 | 別為了讓自己的新程式碼合規而擴充登記，或新增一條規則 |

## 二、延後中，且延後條件明確

動這些之前先確認「條件達成了沒」——沒達成就還是別做。

| 事項 | 在等什麼 |
|---|---|
| catalog deep-merge 對 type-discriminated entry 的 bug（env 覆蓋 `type` 後 base 的 stale key 會傳給新 constructor → `TypeError`） | 等 pandas backend 棄用時一起做大重構。現行 workaround＝`conf/base/catalog.yaml` 放完整定義。若只想修 deep-merge：`ConfigLoader._load` 對 `stem == "catalog"` 改 first-level replace，其餘 stem 維持 deep-merge |
| 非數值特徵欄閘（B6）的後兩階段 | 需**生產事實**：Phase 1＝依 backstop 列出的真實兇手欄名逐欄判斷 declare/drop，然後重建 dataset（bump `base_dataset_version`）；Phase 2＝記憶體結構解。設計在 `docs/superpowers/specs/2026-07-11-nonnumeric-feature-gate-design.md` §7 |
| `sample_weight` 的多槽 `.bin` cache | 等公司 production log 確認症狀真的是「舊 `.bin` 被重用」，而非 config 沒讀到／key 值不符。機制見 `docs/operations/known-pitfalls.md` |
| evaluation 的 `eval_predictions` 快取／早落地／SparkListener 觀測 | 使用者定案「**先觀測、量出熱點再決定優化**」。等公司環境量測數據出來且使用者決定後才開下一個 PR |
| evaluation／baseline metrics 的 skip-if-exists 快取 | 等實測證明 dev-loop 真的太慢。要做時**兩者必須一起**做、同生命週期失效，否則 delta 不一致 |
| 資料閘 B2（leakage）／B3（零正樣本）、Layer-3 單一來源（`conf/base/products.yaml`＋source_etl pre-flight） | 獨立 plan，尚未排程 |
| feature_table「改既有欄邏輯」的漂移防護 | `compute_feature_table_fingerprint` 是 schema-only（只看 name+dtype），改值不會 bump 版本。後續方案＝上游掛顯式 `feature_build_version` tag 折進 `compute_base_dataset_version`。物理減欄、型別變更、歷史分區回填亦同 |
| 外部 segment 表（如 `holding_combo`）的 source_etl | 要做再規劃；預設 config 留作註解範例 |
| sampling editor 的 (t, α) 參數化與進 HPO search space | 目前編輯器只產**靜態** YAML |
| 訓練診斷 P3（Optuna 診斷＋train/train-dev 學習曲線） | 依賴 `hpo_checkpointing`；術語一律 train/train-dev，不是 train/val |

## 三、已完成工作留下的、程式碼讀不出來的取捨

| 事實 | 為什麼要知道 |
|---|---|
| `etl_audit_log` 在 `SIGTERM`/`SIGKILL` 下會遺失**整批** audit（紀錄在 flush 前只活在記憶體 buffer） | 這是「用無小碎檔換掉逐筆持久化」的刻意取捨，**不是 regression**。terminal 與 `logs/<pipeline>_<run_id>.jsonl` 仍有逐筆紀錄可當 fallback |
| inference 的缺特徵母體成員標記刻意**只留 log**，不下推到任何表 | 使用者先說下推、後反轉為不下推。#188 之後連那個 in-memory 欄位也不存在了——它原本活在被合併掉的 `scoring_dataset` 上，而 `inference_population_features` 存的是「特徵全集扣掉 item」，多一個布林旗標會破壞那個定義。留下的是 `build_inference_population_features` 每月一行的缺特徵成員數 log |
| `inference_population` 的 grain 唯一性靠 source_etl 的 `primary_key`＋`quality_checks`，**刻意不動 `consistency.py`** | 這正是「當初 source_etl 沒保證 feature_table 當母體」那個缺口的修法 |
| per-item 指標**沒有 precision** | precision 是 per-query 量，無法歸因到單一 item。刻意的，不是漏掉 |
| 診斷家族計數一律 count-free（寫「各診斷／N」不寫「五項」） | 避免增刪診斷時留下 5-vs-4 的矛盾 |
| `diagnostics` config 必須是 top-level（與 `mlflow`/`cache` 同層），不可進 `training:` | `compute_model_version` 只雜湊 `training:` block；放進去會讓診斷旋鈕 bust model_version。`tests/test_core/test_versioning.py` 守著 |
| HPO 搜尋診斷寫在 `tune_hyperparameters` **尾端**、不是新 DAG node | 這樣對 `RESUME_CONTRACTS` 隱形，`--from-node finalize_model` 跳過 HPO 的行為不變 |
| `hpo_checkpointing`、`release_during_hpo` 等旋鈕放頂層 config | 放進 `training:` 會 churn model_version |
| 已退場的診斷項目：`triage`／`quadrant`／`discrimination`／`pair_ledger`／`cross_purchase`／`offset_sweep`／`occupancy` | 勿復活 |
| inference 的 `entity_bucket` 分區欄**刻意不做「讀的桶數與寫的桶數分開」**（讀 40 桶控 driver、每 4 桶存一次維持分區檔大小） | 技術上可行且保留為逃生口，但它讓「一個 chunk」變成兩個不同的東西，spec 與續跑判準都要跟著分裂。目前預設 10 桶落在健康窗口（5–20）中間，不需要這個自由度。論證見 ADR-0010「考慮過但否決的選項」 |
| inference 的 driver 峰值**只有下界推算，沒有實測** | `_pdf_to_X` 的 `X_df.values` 共同 dtype 由所有欄決定：只要有一欄 int32／int64 特徵（`_cast_feature_floats_to_float32` 刻意只轉 Decimal 與 Double），共同型別升成 float64、那一步多吃一倍。所以文件上的數字是下界不是估計值，實際值取決於生產 `feature_table` 的 dtype 分佈 |
| `pipelines/dataset/nodes.py` 與 `pipelines/inference/nodes.py` 從 `recsys_tfb.preprocessing` import 帶底線的私有名（`_encode_categoricals`、`_cast_feature_floats_to_float32`） | 審查會建議改成公開名。#176 已把 `steps/` 內五個底線名去掉，**刻意沒動這兩個**——rename 要同時改兩條 pipeline 的呼叫點，而它本身不修任何行為。同模組後來加入的 `encodable_categoricals`／`warn_unknown_encodings`（#185 從 `steps/` 搬來）用的是公開名，所以這個模組現在是混合命名的：那是刻意的現況，不是「還沒改完」。理由見 ADR-0008「這條 ADR 沒有解決的事」。不要順手改 |
| 診斷報表的鐵則：**只呈現資料、不下結論** | 禁 severity／verdict／「該先查誰」／「偏高低」等替讀者詮釋或評級的字。完整原則見 `docs/operations/diagnosis-report-presentation.md` |

## 四、還沒有結論、卡住其他事的

- ~~**issue #63**（inference 的 item 特徵欄落差）~~ **已有結論**：由 #183 的票 A（issue #185）取代並修掉——item 在塊內佔兩個位置（identity 留字串、特徵放整數 code），特徵順序與子集的權威改為 `model.feature_names()`。論證見 ADR-0010 §4／§6 與 ADR-0011 §5；本機 local Spark 的實跑斷言在 `scripts/local_e2e.sh` 末段。**這一條留在這裡只是為了讓「#63 卡住 inference」這個舊說法有反駁；它不再卡任何東西。**
- **HPO 後 SparkContext 被誰停掉（Layer 1）** 仍未證實。復原機制已修（偵測到死亡先清 Python 端單例再重建），但根因要叢集端證據，使用者只拿得到應用層 stdout/stderr。**下次失敗時怎麼判讀 `spark_context_dead` 事件，寫在 `src/recsys_tfb/utils/spark.py` 的模組註解**（含為什麼預設設定下該事件本身就排除了閒置回收）——不要憑本條摘要，去讀那段。另一個未驗證的取捨：重建＝在 YARN 上重新提交新 application，公司環境若對此有稽核限制，設計需調整。
- **A1 稽核用 `nodes*.py` glob 當「模組含不含 node」的代理**（`tests/test_core/test_architecture_constraints.py:151`、`:169`），兩個方向都失準。**issue #163，仍 OPEN。**
  - 原本觸發這條的 `data_gate.py` 已在 #169 消失（它的 node 併進 `pipelines/dataset/nodes.py`），dataset 一帶改由 S1 守（要求每個 `Node(...)` 的第一參數必須 `def` 在 `nodes.py`），**#163 的 Q1 因此被繞開而非解決**——回報已留言在 #163。
  - **glob 對 dataset 以外仍然失準**（例：`pipelines/evaluation/comparison_nodes.py` 不符 `nodes*` 前綴）。等使用者裁決；**不得靠改檔名迴避，也不得新增一條讓自己合規的規則**。

## 五、文件與手冊線

- **Spark 優化手冊**：ch9（reverse ETL）與 ch10（PySpark）內容偏薄，是下一輪內容補強的候選；全書 `.md` 審定後才轉離線 HTML。
- **GBDT 手冊系列**：手冊 5＝完整機率校準（Platt/isotonic、LTR 分數校回機率、跨 item 分數可比性）待寫。
- **README／docs 重構**：已出貨，等使用者 dogfood 反饋後再做修訂輪。
- **`writing-technical-handbooks` skill**：已部署到 `~/.claude/skills/`，tracked 源在 `feat/handbook-writing-skill`；PR 未開，待使用者決定。
