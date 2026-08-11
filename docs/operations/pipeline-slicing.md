# Pipeline 切片：從某個 node 開始跑 / 只跑某個 node

設計 spec：`docs/superpowers/specs/2026-06-10-pipeline-node-slicing-design.md`

## 使用

四個 pipeline 指令（`dataset` / `training` / `inference` / `evaluation`）皆支援：

```bash
python -m recsys_tfb training --list-nodes                       # 看 node 名與接續成本
python -m recsys_tfb training --from-node finalize_model --dry-run   # 只印執行計畫
python -m recsys_tfb training --from-node finalize_model         # 從該 node（含其後全部）接續
python -m recsys_tfb dataset  --only-node build_train_model_input # 只跑單一 node
python -m recsys_tfb dataset  --only-test-months                  # 具名切片：只加評估月份
```

- `--from-node X`：X 與拓撲序在其後的全部 node。涵蓋失敗接續／改了下游程式碼重跑／跳過昂貴上游。
- `--only-node X`：只跑 X。單獨 debug 某 node 用。
- `--only-test-months`（**只有 `dataset` 有**）：宣告「這次只加評估月份」，跑資料閘 ＋ 整條 test 鏈。見下節。
- 三者互斥；皆會在開跑前印 `[plan]` 執行計畫（skipped / auto-included / 警語）。
- `--dry-run`：印計畫即退，不執行、不寫任何 pipeline 產物（run log 照常寫）。
- `--list-nodes`：列出 node 與各自的接續成本即退；不可與上述三個切片旗標並用。

## `--only-test-months`：具名切片（dataset）

新增一個 `test_snap_dates` 月份時，dataset 的 15 個 node 有十個會全量重算，再把**逐位元相同**的內容覆寫回同一批 partition——多一個 test 月不改變它們的內容。這個旗標讓你把「這次只加評估月份」講出來，於是只跑五個：

```
[plan] mode=only-test-months; requested: validate_data_consistency, filter_test_model_input
[plan] auto-included (missing input/write target -> producer re-run):
[plan]   select_test_keys  <- test_keys
[plan]   apply_preprocessor_to_features  <- preprocessed_feature_table
[plan]   build_test_model_input  <- test_model_input_unfiltered
[plan] running 5 of 15 nodes
```

**旗標只寫死兩個 node 名**（`__main__.py` 由 `pipelines/dataset/pipeline.py` 的
`ONLY_TEST_MONTHS_NODES` 取得），其餘三個是上一節那套 DAG 擴張推出來的——test 鏈日後多一個
node 不必回頭改這個旗標。兩個名字各有各的理由：

- `filter_test_model_input` 是**終點**，上游由擴張反推。
- `validate_data_consistency` 是 Layer-2 資料閘，列它是**結構性的必要**而非政策選擇：擴張只
  沿著 producer map 走，而那張表由 `node.outputs` 建，**零輸出的 node 永遠不可能被自動拉回**。
  不列＝新動線上沒有資料閘。

`--rebuild-dates` 可以併用（`--only-test-months --rebuild-dates 2026-01-31` 重算既有月份），
且**不會**再印「請不帶切片旗標再跑一次」——那句警語現在的條件是「這次的切片少了 test 鏈的某個
node」，而不是「有沒有帶切片旗標」。`--from-node` 剛好選滿整條鏈時同樣不印。

manifest 的 `extra_metadata` 留 `preset: only-test-months` 的痕跡（對應
`--from-node` 的 `resumed_from` 與 `--only-node` 的 `only_node`）。

> 這是**節點邊界**接續：整個 `tune_hyperparameters` 跑完並落地後，才能用 `--from-node finalize_model` 跳過它。HPO **跑到一半** crash 的接續（只補跑剩餘 trial）是另一層，由 `hpo_checkpointing` 機制處理，見 [`hpo-resume.md`](hpo-resume.md)。

## 自動擴張補跑

被跳過 node 的輸出若「catalog 有定義且存在」（`catalog.exists()`），直接從落地讀；
否則（memory-only、或落地但上次沒跑到）自動把生產者 node 拉回必跑集合、遞迴向上，
直到全部輸入可得。最壞情況退化成 full run——任何起點都合法，絕不靜默缺料。
昂貴 node 若被拉回，會出現在計畫的 auto-included 清單，跑之前看得到。

**dataset 的三個增量產物多問一件事**：`preprocessed_feature_table`、`test_keys`、
`test_model_input` 是逐月延伸的，表從第一次跑完就一直在，所以「存在」對它們永遠是真，
會缺的是**這次要的月份**。它們的判準因此是「這次的月份計畫還有沒有要處理的月」——
`[months]` 那幾行印的就是這個計畫（[ADR-0012](../adr/0012-month-aware-slicing-not-per-artifact-skip.md)）。
加了一個 `test_snap_dates` 月份之後 `--only-node filter_test_model_input`，
auto-included 會列出三個：`build_test_model_input`（上一段那條規則——中間產物
`test_model_input_unfiltered` 是 memory-only），以及月份判準拉回來的
`select_test_keys` 與 `apply_preprocessor_to_features`。
`fit_preprocessor_metadata` 不在其中（`preprocessor` 是落地的 JSON，沒有月份）。
其餘三個 pipeline 沒有增量產物，判準仍是 `exists()` 一個。

## 使用前提與限制

- **參數未變**才能接續：`exists()` 不驗證落地產物是否由當前參數產生。版本化路徑
  （`${base_dataset_version}` 等）天然防呆；**不帶版本的覆寫式 Hive 表**
  （`recsys_prod_train_keys` 等）存在 ≠ 新鮮，風險自負（計畫輸出有固定警語）。
- **training 改了 model-defining 參數會「漂移」到新 `model_version`**：版本化路徑此時
  把你導向一個空的新版本目錄，`--from-node`（例如從 `predict_and_write_test_predictions`
  接續）因而自動補跑 `finalize_model` 等上游＝**重新訓練一個不同的模型**，而非沿用既有
  finalized 模型。training 偵測到此情形（切片把 `model` 的生產節點拉回必跑集合）會在開跑前
  印 `[retrain]` 警告——含算出的 `model_version`、將被重訓的節點、最接近的既有 `completed`
  版本與 diff 提示——但**仍照跑、不擋**（`--dry-run` 下也看得到）。若只是想對既有模型重跑
  下游，先把 `parameters_training.yaml` 的 `training:` 區塊還原到該模型 finalize 時的狀態
  （可比對 `data/models/<model_version>/manifest.json` 的 `parameters`）。
- **side-effect node（outputs=None）不重跑**：位於起點前的守門 node
  （如 dataset 的 `validate_data_consistency` B1/B5 資料閘）在接續時跳過、
  不重新驗證，計畫輸出會列出。資料有變請跑 full run。
  例外是**被明確點名**的時候——`--only-test-months` 就是這樣把資料閘留在切片裡的。
- manifest 照常寫（開跑前先落一份 `status: running` stub 供崩潰溯源，完成後覆寫為
  `status: completed`；既有 manifest 採 skip-if-present，stub 不會覆蓋它），metadata 多
  `resumed_from` / `only_node` 留痕。
- manifest 的 `artifacts` 清單只列版本目錄第一層檔案，**不含 `hpo/` 子目錄**
  （`hpo/model.txt`、`hpo/model_meta.json`）；稽核 manifest 時請知悉。

## 開發守則（改 pipeline 結構的人必讀）

接續點品質是會被新增 node 默默破壞的契約：

1. node 輸出要不要進 catalog 落地，判準＝「是不是某個宣告接續點的必要輸入」×
   「重算貴不貴」。便宜的（view、handle、cheap transform）留 memory-only，
   讓擴張補跑；貴的（HPO 輸出）落地。
2. `tests/test_pipelines/test_resume_contracts.py` 的 `RESUME_CONTRACTS`
   釘住各 pipeline（含 calibration-enabled training 變體）承諾的接續點與
   允許補跑集合。改壞會紅燈——要嘛給新產物補 catalog 條目，要嘛修改契約
   並在 PR 說明為什麼接受變貴。
3. 改完跑 `--list-nodes` 肉眼確認各 node 的接續成本。

## 已知設計決議

- `hpo_best_model` 落地在 `data/models/${model_version}/hpo/model.txt`
  ——`ModelAdapterDataset` 的 `model_meta.json` sidecar 寫在 filepath 同目錄，
  與 `model.txt` 同目錄會互踩（calibration meta 串台）。
- `hpo_best_model` 不做 None 防護：HPO 第一個 trial 必然寫入 best model
  （score ≥ 0 > 初始 -1.0）；`n_trials=0` 在 `study.best_params` 就先炸。
- `tune_hyperparameters` 會被跳過的前提是三個輸出（`best_params` /
  `best_iteration` / `hpo_best_model`）都已落地——缺一個就會整顆重跑 HPO。
- 落地 `hpo_best_model` 後，full run 的 `finalize_model` 也會吃到磁碟
  round-trip 的 adapter（行為不變：LightGBM `save_model` 預設截斷至
  best_iteration，預測結果一致；`best_iteration` 另以 JSON 落地顯式傳遞）。
