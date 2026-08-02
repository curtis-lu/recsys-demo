# 交接檔：test 相依指標的逐月化（併入 #128 Phase B 的 grill）

> 建立：2026-08-02。給續作 session：讀完本檔＋下列「唯一真實來源」即可開工，**不需要舊對話**。
> 下一步是 **`/grill-with-docs`，不是 `/implement`**——本檔最後一節說明為什麼。

## 為什麼有這份檔

做 #130（predict 逐月跳過已完成月份，PR #136）時，使用者追問 `compute_test_mAP_spark` 的語意，查證後發現一件**既有的、不是 #130 造成的**問題：

**training 側的 `overall_map` 是「這個 `model_version` 目前累積的所有 test 月份」的合併值，而 `model_version` 的身分裡已經沒有 test 月份了（ADR-0001）。** 於是同一個鍵對應的數字會隨月份累積而變，且**它是 promote 的排名鍵**。

使用者的傾向：**`compute_test_mAP_spark` 也應該逐 `snap_date` 區隔**。方向合理，但落地方式有數個未決的決策點（見下），所以先 grill。

## 已查證的事實（附證據，不必重查）

| 事實 | 證據 |
|---|---|
| 節點載到的是該 `model_version` 的**整張**表，不分月 | catalog `training_eval_predictions` 只有 `partition_filter: model_version`；`HiveTableDataset.load()` 只注入該 WHERE |
| 一個 query ＝ `time × entity`，所以每個 query 屬於單一月份，但平均跨月 | `src/recsys_tfb/evaluation/metrics_spark.py:202` |
| 因此 `overall_map` 是**按各月 query 數加權**的跨月合併值 | 本機實測（mv `ffb88332`）：2026-01-31 → 5232 列 / 654 query；2026-02-28 → 5544 列 / 693 query；合計 1347 query |
| 該數字進 MLflow | `src/recsys_tfb/pipelines/training/nodes.py:1292`（另 `uncalibrated_overall_map` :1305） |
| **它是 promote 的排名鍵**（人工觸發，但腳本會印 `Recommended:`） | `scripts/promote_model.py:39-49` 讀各版本 `evaluation_results.json` 取 `overall_map` 排序；:133 印建議 |
| 第二個消費者 | `scripts/model_capacity_diagnosis.py:15` 讀同一份 json，取 `overall_map` / `per_item_map_attr` |
| 產物路徑**沒有月份層**（與 `diagnostics_dir()` 同型的覆寫風險） | `conf/base/catalog.yaml:214` → `data/models/${model_version}/evaluation_results.json` |
| 逐月的**列數／正樣本數**其實已經算好了，只是頭條指標沒有 | `metrics_spark.compute_all_metrics` 的 `dataset_overview.by_snap_date` |
| predict-only 切片**不會**重寫 `evaluation_results.json`（該節點不在切片內） | `docs/operations/adding-an-eval-month.md` 步驟 3（實測 3 of 21 nodes） |

**實際會咬人的情境**：A 版訓練時設定只有 1 個月、B 版訓練時已有 3 個月 → 兩個 mAP 量的不是同一件事，卻被並排排序並附一行 `Recommended`，畫面上沒有任何東西標示涵蓋範圍不同。「加月份變便宜」之後，這種版本間涵蓋不一致會從罕見變常態。

## 唯一真實來源（先讀這些）

1. **`gh issue view 128`** — parent spec。**Out of Scope 第一條明文要求 Phase B（月份相依診斷搬到 evaluation）另開一輪 `/grill-with-docs` → `/to-spec`**；本題應**併入那一輪**，不要自己開一輪（見下節理由）。
2. `docs/superpowers/specs/2026-07-31-per-month-test-artifacts-design.md` — Phase B 前期調查（接縫分堆、`--from-node` 為何走不通、四項成本盤點）。
3. `docs/adr/0001-test-dates-out-of-dataset-version-identity.md` — 本問題的**成因**：test 日期退出版本身分是刻意取捨，這個指標是它沒收尾的邊。
4. `docs/adr/0003-per-month-test-artifacts.md` — 身分規則（依賴 test 資料的產物身分應為 `(model_version, snap_date)`）＋「這條 ADR 沒有解決的事」。
5. PR #136（#130 實作）— predict 已逐月化的形狀，可當範本：純函式 plan、三份清單 manifest、`--rebuild-dates` 逃生口。

## grill 要解的決策點（這就是它不是 `/implement` 的原因）

1. **停在 training 還是搬到 evaluation？** 在 training 裡逐月算，等於在 Phase B 認定「應該搬走」的那一側加東西，可能之後整包再搬一次。這是**同一個接縫問題**——SHAP／象限診斷逐月化問的是同一句話，所以兩者要一起談，分開 grill 會得出互相打架的答案。
2. **逐月結果落在哪？** `evaluation_results.json` 沒有月份層。加一層？換路徑？還是根本不該由 training 產？
3. **`promote_model.py` 用哪個數字排名？** 最新月／各月平均／各月都要贏？——這是**決策語意**，只有使用者能定，不是實作細節。
4. **MLflow `overall_map` 的相容性**：改名或改語意會讓歷史 run 不可比。
5. **兩個消費者的破壞面**：`promote_model.py`、`model_capacity_diagnosis.py`（行號見上表）。
6. **向後相容**：既有版本的 `evaluation_results.json` 沒有逐月欄位，跨版本比較怎麼處理。

## 範圍邊界（不要順手做）

- **A 軌（讓數字自我描述）是另一件事**，使用者當下選擇先不做：把「涵蓋哪些月份 ＋ 各月 n_queries」加進 `evaluation_results`、promote 排名時印涵蓋範圍並在範圍不同時 WARN。它純加欄、不預設架構，可獨立成票。**若 grill 的結論是大改，A 軌仍可當過渡**。
- **不要動 promote 的行為**：model promote 是使用者明文保留的人工步驟。
- **#131（票 5／5，evaluation snap_date 必須屬於 test 月份）是另一張票**，與本題無關，已不被阻擋。
- **不要用 `/triage`**：那是給外來 issue 用的；本題是自己開的工作。

## 建議 skills

1. **`/grill-with-docs`** — 主入口。開場請一併餵 issue #128 與本檔，並明說「本輪要同時涵蓋 Phase B 的診斷搬遷**與** test mAP 的逐月化，因為是同一個接縫」。
2. → **`/to-spec`** → **`/to-tickets`**（多 session 規模的可能性高：牽涉 pipeline 邊界、MLflow 語意、兩個腳本消費者）。三步請維持在**同一個不中斷的 context window**。
3. 每張票 → **`/implement`**（內部走 `/tdd`，收尾 `/code-review`），**票與票之間清 context**。
4. 談「接縫放哪」時可拉 **`/codebase-design`** 的 deep-module 詞彙（設計文件已建議過同一件事）。

## 本機環境狀態（續作可直接用）

- ⚠ **更新於 2026-08-02（PR #137 收尾）：原本記在這裡的本機資料已經沒了。** 當時的 worktree `.worktrees/predict-incremental-months`（PR #136 已 merge）連同它自己的 `data/` 樹一起被清掉，**兩個月的 `training_eval_predictions`（2026-01-31、2026-02-28）與該 worktree 的 local warehouse 隨之消失**。原文寫「資料已在，把 `2026-02-28` 加回 `test_snap_dates` 即可重現跨月合併」——**這句已不成立**。
- 要重現跨月合併的現象，得在新 worktree 重跑一次：`test_snap_dates` 設兩個月 → `dataset` → `training --only-node predict_and_write_test_predictions`。當時的參照值：`base_dataset_version=67088d8f`、`model_version=ffb88332`（同一份 config 應可重現，但沒有再驗證過）。實測列數／query 數見上表第 3 列，那些數字仍是查證過的事實。
- 跑法一律：`export SPARK_CONF_DIR=$PWD/conf/spark-local`＋`PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m recsys_tfb <pipeline> --env local`。
- 本檔已於 2026-08-02 進版控（PR #138）。
