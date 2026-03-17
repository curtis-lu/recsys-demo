## MODIFIED Requirements

### Requirement: 驗證模型 artifacts 完整性
`promote_model.py` SHALL 檢查模型目錄中的必要 artifacts 是否存在。`REQUIRED_ARTIFACTS` 清單 MUST 包含 `model.txt`（而非 `model.pkl`），以對應 LightGBMDataset 的實際輸出格式。

#### Scenario: 新格式模型通過驗證
- **WHEN** 模型目錄含 `model.txt`、`best_params.json`、`evaluation_results.json`
- **THEN** `validate_version()` 回傳空清單（無缺失 artifacts）

#### Scenario: 舊格式 model.pkl 不再作為必要檢查項
- **WHEN** 模型目錄含 `model.txt` 但不含 `model.pkl`
- **THEN** `validate_version()` 回傳空清單（通過驗證）
