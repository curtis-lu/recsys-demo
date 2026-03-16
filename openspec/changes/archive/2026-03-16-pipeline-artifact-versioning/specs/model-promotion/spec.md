## MODIFIED Requirements

### Requirement: promote_model CLI script
系統 SHALL 提供 `scripts/promote_model.py` CLI script，將指定版本的 model artifacts 以 symlink 方式設為 `data/models/best`。

#### Scenario: 指定版本 promote
- **WHEN** 執行 `python scripts/promote_model.py a1b2c3d4`
- **THEN** 系統 SHALL 將 `data/models/best` 建立為指向 `data/models/a1b2c3d4/` 的 symlink

#### Scenario: 不指定版本時選 mAP 最高
- **WHEN** 執行 `python scripts/promote_model.py`（不帶版本參數）
- **THEN** 系統 SHALL 自動選擇 overall_map 最高的版本進行 promote

#### Scenario: 支援 hash 和舊時間戳格式
- **WHEN** `data/models/` 下同時有 hash 格式和 `YYYYMMDD_HHMMSS` 格式的版本目錄
- **THEN** 系統 SHALL 能 promote 兩種格式的版本，auto-select 時掃描兩種格式

#### Scenario: 指定版本不存在
- **WHEN** 執行 `python scripts/promote_model.py nonexistent`（不存在的版本）
- **THEN** 系統 SHALL 輸出錯誤訊息並以非零 exit code 結束

#### Scenario: 支援自訂 models_dir
- **WHEN** 執行 `python scripts/promote_model.py --models-dir /custom/path a1b2c3d4`
- **THEN** 系統 SHALL 將 `/custom/path/best` 建立為指向 `/custom/path/a1b2c3d4/` 的 symlink

### Requirement: promote 前驗證 artifacts 完整性
promote script SHALL 在操作前驗證版本目錄中包含所有必要的 artifacts。

#### Scenario: artifacts 不完整
- **WHEN** 版本目錄中缺少 model.pkl
- **THEN** 系統 SHALL 輸出錯誤訊息列出缺少的檔案，並以非零 exit code 結束

#### Scenario: artifacts 完整
- **WHEN** 版本目錄中包含 model.pkl、best_params.json、evaluation_results.json
- **THEN** 系統 SHALL 執行 promote 並輸出成功訊息

### Requirement: promote 輸出摘要
promote script SHALL 在完成後輸出 promote 結果摘要。

#### Scenario: 成功 promote 摘要
- **WHEN** promote 成功完成
- **THEN** 系統 SHALL 輸出：promoted 版本 ID、overall_map、per_product_ap 摘要、目標路徑

### Requirement: promote 處理既有 best
promote script SHALL 正確處理 best 從舊的目錄複製格式遷移到 symlink 格式。

#### Scenario: best 是目錄（舊格式）
- **WHEN** `data/models/best` 是一個實際目錄（非 symlink）
- **THEN** 系統 SHALL 移除該目錄後建立 symlink

#### Scenario: best 是 symlink（新格式）
- **WHEN** `data/models/best` 已是 symlink
- **THEN** 系統 SHALL 移除舊 symlink 後建立新 symlink
