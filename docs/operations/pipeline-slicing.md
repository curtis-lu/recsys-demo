# Pipeline 切片：從某個 node 開始跑 / 只跑某個 node

設計 spec：`docs/superpowers/specs/2026-06-10-pipeline-node-slicing-design.md`

## 使用

四個 pipeline 指令（`dataset` / `training` / `inference` / `evaluation`）皆支援：

```bash
python -m recsys_tfb training --list-nodes                       # 看 node 名與接續成本
python -m recsys_tfb training --from-node finalize_model --dry-run   # 只印執行計畫
python -m recsys_tfb training --from-node finalize_model         # 從該 node（含其後全部）接續
python -m recsys_tfb dataset  --only-node build_train_model_input # 只跑單一 node
```

- `--from-node X`：X 與拓撲序在其後的全部 node。涵蓋失敗接續／改了下游程式碼重跑／跳過昂貴上游。
- `--only-node X`：只跑 X。單獨 debug 某 node 用。
- 兩者互斥；皆會在開跑前印 `[plan]` 執行計畫（skipped / auto-included / 警語）。
- `--dry-run`：印計畫即退，不執行、不寫任何 pipeline 產物（run log 照常寫）。
- `--list-nodes`：列出 node 與各自的接續成本即退；不可與 `--from-node`/`--only-node` 並用。

> 這是**節點邊界**接續：整個 `tune_hyperparameters` 跑完並落地後，才能用 `--from-node finalize_model` 跳過它。HPO **跑到一半** crash 的接續（只補跑剩餘 trial）是另一層，由 `hpo_checkpointing` 機制處理，見 [`hpo-resume.md`](hpo-resume.md)。

## 自動擴張補跑

被跳過 node 的輸出若「catalog 有定義且存在」（`catalog.exists()`），直接從落地讀；
否則（memory-only、或落地但上次沒跑到）自動把生產者 node 拉回必跑集合、遞迴向上，
直到全部輸入可得。最壞情況退化成 full run——任何起點都合法，絕不靜默缺料。
昂貴 node 若被拉回，會出現在計畫的 auto-included 清單，跑之前看得到。

## 使用前提與限制

- **參數未變**才能接續：`exists()` 不驗證落地產物是否由當前參數產生。版本化路徑
  （`${base_dataset_version}` 等）天然防呆；**不帶版本的覆寫式 Hive 表**
  （`recsys_prod_train_keys` 等）存在 ≠ 新鮮，風險自負（計畫輸出有固定警語）。
- **side-effect node（outputs=None）不重跑**：位於起點前的守門 node
  （如 dataset 的 `validate_data_consistency` B1/B5 資料閘）在接續時跳過、
  不重新驗證，計畫輸出會列出。資料有變請跑 full run。
- manifest 照常寫，metadata 多 `resumed_from` / `only_node` 留痕。
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
