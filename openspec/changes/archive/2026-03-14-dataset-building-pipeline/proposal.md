## Why

Phase 1（框架骨架）已完成，SQL ETL 範例檔也已就緒。現在需要實作 Dataset Building Pipeline——從 ETL 產出的 feature/label 表出發，經過抽樣、切分、特徵準備，產出可直接餵入 LightGBM 的訓練/驗證資料集。這是 MVP 的前置步驟，沒有 dataset pipeline 就無法進入 Training Pipeline。

## What Changes

- 建立合成假資料（Parquet）供開發環境使用，schema 對齊 SQL ETL 產出
- 實作 Dataset Building Pipeline 的所有 nodes：分層抽樣、時間切分、特徵 join、模型輸入準備
- 將 nodes 組裝為 Pipeline，註冊到 pipeline registry
- 新增 `parameters_dataset.yaml` 設定檔
- 新增 dataset pipeline 的單元測試與整合測試

## Capabilities

### New Capabilities
- `synthetic-data`: 合成假資料生成，schema 對齊 SQL ETL 產出的 feature_table 和 label_table
- `dataset-nodes`: Dataset Building Pipeline 的純函數 nodes（抽樣、切分、join、prepare）
- `dataset-pipeline`: Pipeline 定義與參數設定

### Modified Capabilities
- `data-catalog`: 新增 dataset pipeline 所需的中間資料集定義（sample_keys, train/val sets 等）
- `pipeline-registry`: 將 "dataset" pipeline 從空 stub 替換為實際實作

## Impact

- `src/recsys_tfb/pipelines/dataset/`：新增 `nodes.py`、更新 `pipeline.py` 和 `__init__.py`
- `conf/base/catalog.yaml`：新增中間資料集定義
- `conf/base/parameters_dataset.yaml`：新增 dataset 參數
- `conf/local/catalog.yaml`：新增開發環境路徑
- `data/`：新增合成 Parquet 檔案
- `tests/test_pipelines/test_dataset/`：新增測試
