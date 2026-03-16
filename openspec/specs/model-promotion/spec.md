## ADDED Requirements

### Requirement: promote_model CLI script
系統 SHALL 提供 `scripts/promote_model.py` CLI script，將指定版本的所有 artifacts 複製到 `data/models/best/` 目錄。

#### Scenario: 指定版本 promote
- **WHEN** 執行 `python scripts/promote_model.py 20260316_153000`
- **THEN** 系統 SHALL 將 `data/models/20260316_153000/` 下的所有 artifacts 複製到 `data/models/best/`

#### Scenario: 不指定版本時選 mAP 最高
- **WHEN** 執行 `python scripts/promote_model.py`（不帶版本參數）
- **THEN** 系統 SHALL 自動選擇 overall_map 最高的版本進行 promote

#### Scenario: 指定版本不存在
- **WHEN** 執行 `python scripts/promote_model.py 99999999_999999`（不存在的版本）
- **THEN** 系統 SHALL 輸出錯誤訊息並以非零 exit code 結束

#### Scenario: 支援自訂 models_dir
- **WHEN** 執行 `python scripts/promote_model.py --models-dir /custom/path 20260316_153000`
- **THEN** 系統 SHALL 從 `/custom/path/20260316_153000/` 複製到 `/custom/path/best/`

### Requirement: promote 前驗證 artifacts 完整性
promote script SHALL 在複製前驗證版本目錄中包含所有必要的 artifacts。

#### Scenario: artifacts 不完整
- **WHEN** 版本目錄中缺少 model.pkl
- **THEN** 系統 SHALL 輸出錯誤訊息列出缺少的檔案，並以非零 exit code 結束，不執行複製

#### Scenario: artifacts 完整
- **WHEN** 版本目錄中包含 model.pkl、preprocessor.pkl、best_params.json、evaluation_results.json、category_mappings.json
- **THEN** 系統 SHALL 執行複製並輸出成功訊息，包含版本 ID 和 overall_map

### Requirement: promote 輸出摘要
promote script SHALL 在複製完成後輸出 promote 結果摘要。

#### Scenario: 成功 promote 摘要
- **WHEN** promote 成功完成
- **THEN** 系統 SHALL 輸出：promoted 版本 ID、overall_map、per_product_ap 摘要、目標路徑
