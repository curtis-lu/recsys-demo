## Context

目前三個 pipeline 的版本管理狀態不一致：
- **Dataset**: 固定路徑（`data/dataset/`），無版本化，每次執行覆蓋
- **Training**: 時間戳版本目錄（`data/models/YYYYMMDD_HHMMSS/`），但缺乏追溯資訊
- **Inference**: 固定路徑（`data/inference/`），無版本化

此外，`preprocessor.pkl` 和 `category_mappings.json` 由 dataset pipeline 產生，卻存放在 model 目錄中，造成歸屬混淆。

版本邏輯目前直接寫在 `__main__.py` 中（4 行程式碼），隨著三個 pipeline 都需要版本化，需要集中管理。

## Goals / Non-Goals

**Goals:**
- 三個 pipeline 的產出都有版本化目錄，保留歷史紀錄
- 每個版本可追溯：對應的參數、上游依賴版本、git commit
- Dataset/Training 使用參數 hash（相同設定 = 相同版本，可覆蓋重跑）
- Inference 使用 model_version/snap_date 組織（每週產生新資料）
- 版本邏輯集中在 VersionManager 模組，易於測試和維護
- 與現有 catalog template 機制（`${variable}`）完全相容

**Non-Goals:**
- 不實作自動清理舊版本的機制
- 不改變 pipeline node 的邏輯（純 I/O 層變更）
- 不遷移或刪除現有的 `YYYYMMDD_HHMMSS` 模型目錄
- 不改變 MLflow 的 experiment tracking 機制
- 不支援 HDFS symlink（HDFS 不支援，production 用目錄複製或 manifest 指向）

## Decisions

### 1. Hash-based 版本 ID（Dataset/Training）vs 時間戳

**選擇**: 8 字元 SHA-256 hex hash

**理由**:
- 相同參數設定 = 相同版本 ID，語意明確
- 重跑同樣設定會覆蓋而非產生新目錄，避免版本膨脹
- Training hash 包含 dataset_version，確保不同 dataset 訓練出的 model 有不同 ID

**替代方案**: 繼續用時間戳 — 簡單但無法從 ID 推論參數關係，且每次重跑都產生新目錄。

### 2. preprocessor/category_mappings 歸屬

**選擇**: 移至 dataset 版本目錄

**理由**: 這些物件由 dataset pipeline 的 `prepare_model_input` node 產生，邏輯上屬於資料前處理產出。Model 的 manifest 記錄 `dataset_version`，可追溯到對應的 preprocessor。

**替代方案**: 兩邊都存 — 增加儲存空間且違反 single source of truth。

### 3. VersionManager 集中模組 vs 分散在 __main__.py

**選擇**: 新增 `src/recsys_tfb/core/versioning.py` 模組

**理由**: 版本邏輯包含 hash 計算、manifest 生成、symlink 管理、版本解析等多個關注點。集中管理符合 Kedro 設計哲學的「分離關注點」原則，且每個函式都可獨立測試。

### 4. Manifest 寫入時機

**選擇**: 在 `__main__.py` 中 pipeline 執行完成後寫入 manifest

**理由**: manifest 需記錄完整的產出檔案清單，只有在 pipeline 成功完成後才有意義。若 pipeline 中途失敗，不應留下 manifest。

### 5. Model promotion 改用 symlink

**選擇**: `data/models/best` 從目錄複製改為 symlink 指向版本目錄

**理由**: 避免複製大型 model 檔案；symlink 語意更清楚（best 就是某個版本）。注意：HDFS 不支援 symlink，production 環境需用 manifest 文件記錄 best 指向的版本，讀取時先解析 manifest。

### 6. Inference 版本結構

**選擇**: `data/inference/{model_version}/{snap_date}/`

**理由**: 兩層結構 — model_version 標示用哪個模型，snap_date 標示推論哪一週的資料。配合 manifest 記錄完整上下文。

### 7. Inference 的 dataset_version 解析

**選擇**: 從 model manifest 中自動讀取 `dataset_version`

**理由**: model manifest 已記錄訓練時使用的 dataset_version，inference 需要讀取同一版本的 preprocessor。自動解析避免人為錯誤。

## Risks / Trade-offs

- **[Breaking change] 舊版本目錄格式不相容** → 舊的 `YYYYMMDD_HHMMSS` 目錄保留不刪除。`compare_model_versions` 和 `promote_model.py` 需同時支援新舊格式。遷移為手動可選。

- **[Hash collision] 8 字元 hex 的碰撞風險** → 16^8 ≈ 43 億種組合，對於此場景（數十到數百個版本）碰撞機率極低。若未來需要可擴展到更長 hash。

- **[HDFS 無 symlink]** → Production 環境需用 `best_manifest.json` 文件替代 symlink，記錄當前 best 版本的 ID。ConfigLoader 或 VersionManager 需處理兩種模式（local symlink vs HDFS manifest）。

- **[Manifest 一致性]** → 若 pipeline 部分成功後中斷，版本目錄存在但無 manifest。VersionManager 的 `resolve_version` 應檢查 manifest 存在性。

## Open Questions

- Production 環境（HDFS）的 best/latest 解析機制：是在 VersionManager 中實作 HDFS-aware 邏輯，還是在 production catalog config 中用不同的 template？（建議先只實作 local，HDFS 留待後續）
