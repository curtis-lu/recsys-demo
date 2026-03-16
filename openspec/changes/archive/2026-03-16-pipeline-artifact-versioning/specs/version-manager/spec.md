## ADDED Requirements

### Requirement: Hash-based 版本 ID 計算
`VersionManager` SHALL 提供 `compute_dataset_version(params_dataset: dict) -> str` 和 `compute_model_version(params_training: dict, dataset_version: str) -> str` 方法。Dataset 版本 ID 為 `parameters_dataset.yaml` 內容的 SHA-256 hash 前 8 字元（hex）。Model 版本 ID 為 `parameters_training.yaml` 內容與 `dataset_version` 字串串接後的 SHA-256 hash 前 8 字元。

#### Scenario: 相同參數產生相同 dataset 版本 ID
- **WHEN** 以相同的 `parameters_dataset.yaml` 內容呼叫 `compute_dataset_version` 兩次
- **THEN** 兩次回傳值 SHALL 完全相同

#### Scenario: 不同參數產生不同 dataset 版本 ID
- **WHEN** 以不同的 `parameters_dataset.yaml` 內容呼叫 `compute_dataset_version`
- **THEN** 回傳值 SHALL 不同

#### Scenario: Model 版本包含 dataset 版本
- **WHEN** 以相同 `parameters_training.yaml` 但不同 `dataset_version` 呼叫 `compute_model_version`
- **THEN** 回傳值 SHALL 不同

#### Scenario: 版本 ID 格式
- **WHEN** 呼叫任一 compute 方法
- **THEN** 回傳值 SHALL 為 8 字元小寫 hex 字串（符合 `^[0-9a-f]{8}$`）

### Requirement: Manifest 寫入
`VersionManager` SHALL 提供 `write_manifest(version_dir: Path, metadata: dict) -> None` 方法，將 metadata 以 JSON 格式寫入 `version_dir/manifest.json`。

#### Scenario: Dataset manifest 內容
- **WHEN** dataset pipeline 完成後寫入 manifest
- **THEN** manifest.json SHALL 包含：`version`（str）、`pipeline`（"dataset"）、`created_at`（ISO 8601）、`git_commit`（str or None）、`parameters`（dict，parameters_dataset.yaml 完整內容）、`artifacts`（list of str，產出檔案名稱）

#### Scenario: Model manifest 內容
- **WHEN** training pipeline 完成後寫入 manifest
- **THEN** manifest.json SHALL 包含：`version`（str）、`pipeline`（"training"）、`created_at`（ISO 8601）、`git_commit`（str or None）、`dataset_version`（str）、`parameters`（dict，parameters_training.yaml 完整內容）、`artifacts`（list of str）

#### Scenario: Inference manifest 內容
- **WHEN** inference pipeline 完成後寫入 manifest
- **THEN** manifest.json SHALL 包含：`version`（str）、`pipeline`（"inference"）、`created_at`（ISO 8601）、`model_version`（str）、`dataset_version`（str）、`parameters`（dict，parameters_inference.yaml 完整內容）

### Requirement: Symlink 管理
`VersionManager` SHALL 提供 `update_symlink(target: Path, link: Path) -> None` 方法。若 link 已存在（symlink 或目錄），SHALL 先移除再建立新 symlink。

#### Scenario: 建立新 symlink
- **WHEN** link 路徑不存在
- **THEN** SHALL 建立指向 target 的 symlink

#### Scenario: 更新既有 symlink
- **WHEN** link 路徑已存在為 symlink
- **THEN** SHALL 移除舊 symlink 並建立指向新 target 的 symlink

#### Scenario: 取代既有目錄（舊版 best）
- **WHEN** link 路徑已存在為目錄（非 symlink）
- **THEN** SHALL 移除該目錄並建立 symlink

### Requirement: Manifest 讀取
`VersionManager` SHALL 提供 `read_manifest(version_dir: Path) -> dict` 方法，讀取並回傳 `version_dir/manifest.json` 的內容。

#### Scenario: manifest 存在
- **WHEN** version_dir 下有 manifest.json
- **THEN** SHALL 回傳解析後的 dict

#### Scenario: manifest 不存在
- **WHEN** version_dir 下無 manifest.json
- **THEN** SHALL raise FileNotFoundError

### Requirement: 版本解析
`VersionManager` SHALL 提供 `resolve_dataset_version(dataset_dir: Path, version: str | None) -> str` 和 `resolve_model_version(models_dir: Path, version: str | None) -> str` 方法。

#### Scenario: Dataset 版本解析 — 指定版本
- **WHEN** version 參數非 None
- **THEN** SHALL 回傳該版本字串（不驗證目錄存在）

#### Scenario: Dataset 版本解析 — 使用 latest
- **WHEN** version 參數為 None 且 latest symlink 存在
- **THEN** SHALL 回傳 latest symlink 指向的目錄名稱

#### Scenario: Model 版本解析 — 使用 best
- **WHEN** version 參數為 None 且 best symlink 存在
- **THEN** SHALL 回傳 best symlink 指向的目錄名稱

#### Scenario: Git commit 取得
- **WHEN** 寫入 manifest 時
- **THEN** SHALL 嘗試取得當前 git HEAD commit hash（short form）；若不在 git repo 中 SHALL 記錄為 None
