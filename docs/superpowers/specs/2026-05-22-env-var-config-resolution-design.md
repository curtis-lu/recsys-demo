# 設計：YAML 設定值的環境變數解析（`${env.X}`）

- 日期：2026-05-22
- 分支：`feat/env-var-config-resolution`
- 狀態：設計核可，待寫實作計畫

## 動機

目前 YAML 設定檔（`conf/base/*.yaml`）的值無法引用環境變數。具體需求是讓
`parameters_training.yaml` 的 `mlflow.tracking_uri` 能讀 `MLFLOW_TRACKING_URI`，
但這是通用需求 —— Hive db、路徑等欄位同樣會想用環境變數覆寫。

`ConfigLoader.get_parameters()` 目前只做 `yaml.safe_load` + deep-merge，**沒有任何
substitution**。`${...}` 插值只發生在 catalog（`get_catalog_config` → `_substitute`），
且代入來源是其他 parameter 值、不是 `os.environ`。

repo 內已存在 `${env.NAME}` 語法（`utils/vdclient_resolver.py` 的
`resolve_env_placeholders`），但只套用在 spark config 區塊。經查 `conf/` 全目錄，
**目前沒有任何 spark config 實際使用 `${env.X}`**（`parameters.yaml:14` 的
`${env.NODE_IP}` 是註解掉的範例）。

## 範圍決策

採「Loader 全域統一」方案：env 解析提升到 `ConfigLoader` 層級，一次涵蓋所有
`parameters_*` 與 `catalog`（含 spark 區塊）。`vdclient_resolver.py` 內的
`resolve_env_placeholders` 因而變死碼，移除；該檔僅保留 `resolve_vdclient_placeholders`
（`${vdclient.X}` 依賴 production-only 套件 `vdclient_magic`、且需 laptop/CI 上的
drop-on-missing 語意，與 env 無關，留在原處）。

不採用的方案：
- 「spark 區塊 carve-out」—— 會讓同一 `${env.X}` 語法依「是否在 `spark:` 底下」有
  兩種語意（drop vs raise），且保留重複的 resolver。
- 「只解 `get_parameters()`」—— `${env.X}` 寫在 catalog 或經 `get_parameters_by_name`
  讀的欄位會靜默失效，與「通用」初衷矛盾。

## placeholder 文法

正規式：`\$\{env\.([A-Za-z_]\w*)(\|([^}]*))?\}`

| 寫法 | 意義 |
|---|---|
| `${env.NAME}` | 必填。環境變數未設 → raise |
| `${env.NAME\|default}` | 有預設。未設 → 用 `default`（字面字串） |
| `${env.NAME\|}` | 明確空字串預設。未設 → `""` |

- 「有無 `\|` 分隔符」是「必填 vs 有預設」的唯一判準。`${env.NAME\|}` 視為「有預設、
  值為空字串」（正規式 group 2 為 `None` ⇒ 必填；非 `None` ⇒ 有預設，group 3 即
  default、可為 `""`）。
- 支援單一字串中多個 placeholder，以及嵌在較長字串中（如
  `file://${env.HOME}/mlruns`）—— 以 `re.sub` 逐一代換。
- 只比對 `env.` 前綴。catalog 的 `${hive.db}`、`${base_dataset_version}`、ETL 的
  `${target_db}`、SQL 檔的 `${target_date}` 前綴皆不同，不受影響。
- 單次代換，不遞迴：default 字串與環境變數值本身若含 `${...}` 一律當字面字串，不再
  解析（避免注入）。

## 解析語意與錯誤處理

- 環境變數值一律是字串；此機制限定字串欄位，不做型別轉換。非字串值（int/bool/list/
  dict）原樣通過。
- collect-all：遞迴走訪整棵設定樹，蒐集所有「必填但未設」的 placeholder，最後一次
  raise `ConfigEnvError`（新例外，定義於 `core/config.py`，`ValueError` 子類）——
  與既有 `ConfigConsistencyError` 的 collect-all 哲學一致。
- 錯誤訊息點名「檔案 stem + dotted key 路徑 + 環境變數名」，並提示如何補預設值：

```
ConfigEnvError: 2 個必填環境變數未設定:
  parameters_training.yaml -> mlflow.tracking_uri : 環境變數 'MLFLOW_TRACKING_URI' 未設定
      (如需預設值請改寫 ${env.MLFLOW_TRACKING_URI|<default>})
  catalog.yaml -> feature_table.database : 環境變數 'HIVE_DB' 未設定
```

## 架構與整合點

env 解析作為 `ConfigLoader._load()` 末端的一個 pass：載入 base/env YAML、deep-merge
完成後，對整棵 `self._config` 做一次遞迴 env 代換。之後 `get_parameters()` /
`get_parameters_by_name()` / `get_catalog_config()` 回傳的都是已解析的值；解析發生在
`validate_config_consistency` 之前。

解析邏輯放在 `core/config.py` 內（module-level 私有函式，沿用該檔現有的 `_apply`
遞迴 walker 慣例），不另開模組。

| 檔案 | 改動 |
|---|---|
| `src/recsys_tfb/core/config.py` | 新增 `ConfigEnvError(ValueError)`；新增 `_resolve_env()` 遞迴 walker 與 `_resolve_env_string()`；`_load()` 末端呼叫 `_resolve_env(self._config)` |
| `src/recsys_tfb/__main__.py` | `_load_config_and_setup`：把 try/except 範圍往上擴，涵蓋 `ConfigLoader(...)` 建構（L77），使 `ConfigEnvError` 被捕捉、CLI 乾淨 exit code 1。`_load_spark_config`：移除 `resolve_env_placeholders` import + 呼叫（L56、L71），保留 `resolve_vdclient_placeholders` |
| `src/recsys_tfb/utils/spark.py` | fallback session 路徑（L119-122）移除 `resolve_env_placeholders` import + 呼叫；此路徑的 `ConfigLoader` 一樣會在 `_load` 解析 env |
| `src/recsys_tfb/utils/vdclient_resolver.py` | 刪除 `resolve_env_placeholders` 函式與 `_ENV_PATTERN`；更新 module docstring（移除 `${env.<NAME>}` 段） |

## 設定檔改動

- `conf/base/parameters_training.yaml`：`tracking_uri: mlruns` →
  `tracking_uri: ${env.MLFLOW_TRACKING_URI|mlruns}`（環境變數有設則用之，未設退回
  `mlruns`）。
- `src/recsys_tfb/pipelines/training/nodes.py:738`：
  `mlflow_params.get("tracking_uri", "mlruns")` 保留不動 —— 傳入值已解析完畢，`.get`
  的 default 僅為無害的雙保險。
- `conf/base/parameters.yaml:14` 註解範例 `# spark.driver.host: ${env.NODE_IP}` →
  改為 `${env.NODE_IP|}` 以反映新文法（裸 `${env.NODE_IP}` 在新語意下未設即 raise）。

## 測試

新增（`tests/test_core/test_config.py`）：
- 環境變數有設 → 代換為環境變數值
- 未設 + 有 default → 用 default
- 未設 + 空 default（`${env.X|}`）→ `""`
- 未設 + 無 default → raise `ConfigEnvError`
- 單一字串中多個 placeholder
- placeholder 嵌在較長字串中（前後有其他文字）
- collect-all：多筆未設變數一次回報
- 非字串值（int/bool/list）原樣通過
- `${hive.db}` / `${target_db}` 等非 `env.` 前綴不被碰
- env 解析發生在 `validate_config_consistency` 之前（整合驗證）

移除 / 調整：
- `tests/test_utils/test_vdclient_resolver.py`：移除涵蓋 `resolve_env_placeholders`
  的測試；保留 `resolve_vdclient_placeholders` 測試。
- `tests/test_utils/test_spark.py`：若 assert 到 `resolve_env_placeholders` 一併調整。

不受影響：`test_nodes.py` / `test_versioning.py` 的 mlflow 測試直接傳 dict、不經
`ConfigLoader`。

效能：純 Python 字串處理、無 Spark action，無測試效能疑慮。

## 不做（YAGNI）

- 型別轉換（限定字串欄位）。
- 巢狀 / 遞迴解析。
- `docs/spark-connection-architecture.md` 大改（僅 `vdclient_resolver.py` docstring
  必改）。
- `CLAUDE.md` 改動 —— `ConfigEnvError` 與 consistency gate 無關，不屬一致性不變量。
