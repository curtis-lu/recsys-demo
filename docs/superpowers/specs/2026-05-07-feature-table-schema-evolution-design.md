# feature_table Schema Evolution: Migrate Command + Pipeline Verify Node

**Status:** Draft
**Date:** 2026-05-07
**Author:** curtis-lu (with Claude)

## Goal

在 user 對 `feature_table` 上游加新欄位時，提供一條明確、有意識的路徑同步下游 6 張 Hive output table 的物理 schema，讓 dataset pipeline 不會被「table 28 cols vs DataFrame 29 cols」這類 Hive 全域 schema 限制卡住，同時不引入 silent 的 ALTER 行為。

## Motivation

PR #2（`feature_table_fingerprint`）合進 main 後，邏輯版本層解決了：feature_table schema 改了 → fingerprint 改了 → `base_dataset_version` 改了 → cache / partition 不再撞 hash。但**物理層**沒解決：`train_model_input` 等 6 張 Hive managed table 第一次建表後 schema 鎖死，新欄位沒辦法用 `df.write.insertInto()` 寫進去（Spark 會 raise `target table has 28 column(s) but the inserted data has 29 column(s)`）。

目前 user 唯一手段是手動 `DROP TABLE`，但 managed table 的 DROP 會一併刪 HDFS 資料 → 舊 base_v partition 全失。對 ML 場景而言「加新特徵」是常規操作，不應每次都付出歷史資料代價，也不應仰賴 user 記得這 6 張表的清單。

## Non-Goals

- **不**自動處理刪欄 / 改型 / 重命名。這類變更語意危險，留給 user 決策（fail-loud + 文件指引）。
- **不**做 silent ALTER。所有 schema migration 必須 user 明確觸發（獨立 CLI command + dry-run 預設）。
- **不**改 `core/versioning.py` / `feature_table_fingerprint`。fingerprint 是邏輯版本層的職責，跟物理 ALTER 解耦。
- **不**改 inference 端的 `score_table` / `validated_predictions`（與 feature_table schema 無連動）。
- **不**自動清理 schema widening 留下的閒置欄位（user 改回去後多出的欄位無害但不清除）。
- **不**做 production (CDP/YARN) cutover 設計 —— dev-cluster 跑通先；prod 切換待議。

## Architecture

### 設計原則

1. **檢核 vs 執行分離**：dataset pipeline 只負責 fail-fast 偵測；ALTER 動作獨立 CLI command。
2. **顯式 gate**：`migrate_schema` 兩段式 —— dry-run 預設 + `--apply` 才動 metastore。
3. **Catalog metadata driven**：受影響表透過 `catalog.yaml` 標 `tracks_feature_table_schema: true`，不靠啟發式偵測。
4. **Kedro Node 純函式不變**：受影響表清單在 `create_pipeline()` 階段從 catalog 抽出，閉包進 verify Node。

### 三條路徑

```
[user 改 feature_table 上游加欄]
            │
            ▼
   python -m recsys_tfb dataset
            │
   ┌────────▼────────┐
   │ verify Node     │ ← dataset pipeline 第一個 Node
   │ (fail-fast)     │
   └────────┬────────┘
            │ diff 偵測 → raise IncompatibleSchema...
            ▼
   訊息提示跑 migrate_schema
            │
            ▼
   python -m recsys_tfb migrate_schema           (dry-run，僅印 plan)
            │
            ▼
   python -m recsys_tfb migrate_schema --apply   (執行 ALTER)
            │
            ▼
   重跑 python -m recsys_tfb dataset → 通過
```

### 核心 module: `src/recsys_tfb/io/schema_evolution.py`

新檔，跟 `hive_table_dataset.py` 並列。提供以下 public 介面：

```python
@dataclass(frozen=True)
class SchemaDiff:
    table_fqn: str
    added: list[tuple[str, str]]          # [(name, dtype_simple_string), ...]
    removed: list[str]
    type_changed: list[tuple[str, str, str]]  # [(name, old_dtype, new_dtype), ...]

    @property
    def is_compatible(self) -> bool:
        return not (self.removed or self.type_changed)

    @property
    def is_empty(self) -> bool:
        return not (self.added or self.removed or self.type_changed)


class IncompatibleSchemaChangeError(RuntimeError):
    """Raised when schema diff contains removed/type-changed columns."""


def plan_migrations(
    tracked_tables: list[tuple[str, list[tuple[str, str]]]],  # [(table_fqn, expected_cols), ...]
    spark,
) -> list[SchemaDiff]:
    """Compute diff per tracked table. Skips tables that don't yet exist.

    Raises IncompatibleSchemaChangeError if any diff is incompatible.
    """


def format_plan(diffs: list[SchemaDiff]) -> str:
    """Render diff list as human-readable plan (used by dry-run output)."""


def apply_migrations(diffs: list[SchemaDiff], spark) -> None:
    """Execute ALTER TABLE ... ADD COLUMNS (...) CASCADE per non-empty diff.

    Sequential execution; partial failure does NOT rollback already-applied
    tables (ALTER ADD COLUMNS is metadata-only and idempotent).
    """
```

### Catalog metadata 標註

`conf/base/catalog.yaml` 對 6 張受影響表加 `tracks_feature_table_schema: true`：

```yaml
train_model_input:
  type: HiveTableDataset
  database: ml_recsys
  table: train_model_input
  partition_columns: [base_dataset_version, train_variant_id, snap_date]
  tracks_feature_table_schema: true     # ← 新增
  ...
```

6 張：`preprocessed_feature_table`、`train_model_input`、`train_dev_model_input`、`val_model_input`、`test_model_input`、`calibration_model_input`。

`preprocessed_feature_table` 是 dataset pipeline 把 feature_table 套 preprocessor 後的中間結果（identity_columns + encoded feature_columns），catalog.yaml 寫成 `HiveTableDataset` 並有 `partition_filter: base_dataset_version` —— 跟其他 5 張 model_input 一樣會因 feature_columns 寬度而 widen，要納入 migration 範圍。

### `HiveTableDataset` 小幅擴充

`src/recsys_tfb/io/hive_table_dataset.py`：

1. 建構子接受新 kwarg：`tracks_feature_table_schema: bool = False`，存成 attribute（DataCatalog 從 yaml 把這個 flag 傳進來）。
2. 新增 method：

```python
def current_table_columns(self, spark) -> list[tuple[str, str]] | None:
    """Return [(col_name, dtype_simple_string), ...] from metastore.
    Returns None if table does not exist."""
```

實作走 `spark.catalog.tableExists(self._qualified_name)` + `DESCRIBE TABLE` 解析（避開 partition columns，只取 data columns）。

### dataset pipeline 第一個 Node：`verify_feature_table_schema`

`src/recsys_tfb/pipelines/dataset/pipeline.py:create_pipeline()` 加參數 `catalog: dict | None = None`，掃 catalog 找出標註過的表名，把 `(fqn, expected_cols)` 清單透過 closure 注入 verify Node：

```python
def create_pipeline(backend="pandas", enable_calibration=False, catalog=None):
    tracked_table_fqns = _collect_tracked_tables(catalog or {})

    def _verify_feature_table_schema(feature_table, parameters):
        spark = feature_table.sparkSession
        feature_cols = _derive_feature_columns(feature_table, parameters)
        tracked = [(fqn, _expected_cols(feature_cols, parameters, fqn))
                   for fqn in tracked_table_fqns]
        diffs = plan_migrations(tracked, spark)
        non_empty = [d for d in diffs if not d.is_empty]
        if non_empty:
            raise FeatureTableSchemaOutOfSync(
                "feature_table schema 與下列下游表不一致：\n"
                + format_plan(non_empty)
                + "\n請先跑：python -m recsys_tfb migrate_schema"
            )
        return feature_table  # passthrough → 後續 Node 仍 input feature_table

    nodes = [
        Node(_verify_feature_table_schema,
             inputs=["feature_table", "parameters"],
             outputs="feature_table_verified",
             name="verify_feature_table_schema"),
        # ↓ 後續 Node 改 input feature_table_verified（pure passthrough，零行為差異）
        ...
    ]
```

`_collect_tracked_tables(catalog)` 從 catalog dict 找 `type == "HiveTableDataset"` 且 `tracks_feature_table_schema is True` 的 entry，回傳 `[f"{database}.{table}", ...]`。

`_derive_feature_columns` 沿用 `recsys_tfb/preprocessing/_spark.py:_compute_feature_columns` 的邏輯（feature_table.columns − drop − non-categorical-identity − label）。

`_expected_cols(feature_cols, parameters, fqn)` 對應每張表 build 應有的 `[(name, dtype), ...]`，需查 `dataset/_spark.py:build_model_input` 等 node 的實際輸出 schema（identity_columns + feature_columns + label）。

> **設計筆記**：`feature_table_verified` 是純 passthrough sentinel（值就是 `feature_table` 本身）。後續所有原本 input `feature_table` 的 Node 改 input `feature_table_verified` —— 等同強制 DAG 拓樸先過 verify 再做事。Runner 不需任何修改。

### CLI 指令：`migrate_schema`

`src/recsys_tfb/__main__.py` 加 thin command（~20 行）：

```python
@app.command(name="migrate_schema")
def migrate_schema(
    env: str = typer.Option("dev", "--env"),
    apply: bool = typer.Option(False, "--apply", help="Execute the migration plan"),
):
    """Sync Hive output tables to current feature_table schema."""
    config_loader = ConfigLoader(env=env)
    catalog_dict = config_loader.get("catalog")
    catalog = DataCatalog(catalog_dict)
    spark = get_or_create_spark_session()

    tracked = _collect_tracked_with_expected(catalog_dict, spark)
    diffs = plan_migrations(tracked, spark)
    non_empty = [d for d in diffs if not d.is_empty]

    if not non_empty:
        typer.echo("No schema changes needed.")
        raise typer.Exit(0)

    typer.echo(format_plan(non_empty))
    if not apply:
        typer.echo("\nRun with --apply to execute.")
        raise typer.Exit(0)

    apply_migrations(non_empty, spark)
    typer.echo(f"Applied {len(non_empty)} migration(s).")
```

行為：
- 預設（無 `--apply`）：印 plan + 提示，exit 0。
- `--apply`：執行 ALTER，exit 0。
- 偵測到不相容變更 → `IncompatibleSchemaChangeError` 從 `plan_migrations` raise，CLI 捕獲後印錯誤訊息 + exit 1。
- 無 diff：印 `No schema changes needed.`，exit 0。

## Error Handling

### 不相容變更（刪欄 / 改型 / 重命名）

`IncompatibleSchemaChangeError` 訊息：

```
feature_table 偵測到不相容 schema 變更（非加新欄位）：
  removed: ['old_feat']
  type_changed: [('aum', 'double' → 'string')]

migrate_schema 只支援加新欄位。如要套用此變更請手動 DROP 影響表後重跑：
  for t in ['train_model_input', 'train_dev_model_input',
            'val_model_input', 'test_model_input', 'calibration_model_input']:
      spark.sql(f'DROP TABLE IF EXISTS ml_recsys.{t}')

注意：DROP managed Hive table 會同步刪 HDFS 資料，舊 base_v partition 會遺失。
```

verify Node 與 `migrate_schema` CLI 共用此訊息文案（透過 exception message）。

### Apply 中途失敗

ALTER 序貫執行，失敗某張就 raise，**不 rollback**。理由：
- ALTER ADD COLUMNS 是 metadata-only，不破壞物理資料。
- 操作 idempotent：下次重跑 plan 階段會偵測到已對齊、自動跳過。
- User 修問題後重跑即收斂。

### 表還不存在

`HiveTableDataset.current_table_columns()` 在 table 不存在時回傳 `None`。`plan_migrations` 把 `None` 視為「不需要 ALTER」（dataset pipeline 第一次跑會由 `CREATE TABLE IF NOT EXISTS` 用當下 DataFrame schema 建表）。

### CASCADE 預設

`ALTER TABLE ... ADD COLUMNS (...) CASCADE`：所有 partition metadata 同步，跨工具讀取一致。對 ML 行為影響為零（舊 base_v 只讀舊 partition，舊 model 的 `feature_columns` 不會引用新欄位）。

### User 改了又改回去

feature_table 加欄 → migrate → 又拿掉 → fingerprint 變回舊值，6 張表多出來的欄位閒置但無害（任何 base_v 的訓練/推論都不會 reference）。如要清理需手動 DROP 重建。

## Testing

### 單元測試

**`tests/io/test_schema_evolution.py`**（新檔）

| 測試 | 驗證 |
|---|---|
| `test_plan_migrations_no_diff_returns_empty` | schema 一致 → 空 list |
| `test_plan_migrations_added_columns` | 多欄 → diff.added 正確 |
| `test_plan_migrations_removed_raises` | 少欄 → `IncompatibleSchemaChangeError` |
| `test_plan_migrations_type_changed_raises` | 同名異型 → `IncompatibleSchemaChangeError` |
| `test_plan_migrations_table_missing` | tracked 表未建 → diff 為 empty（略過） |
| `test_apply_migrations_emits_alter_cascade` | mock `spark.sql`，斷言 `ALTER TABLE ... ADD COLUMNS (...) CASCADE` |
| `test_apply_migrations_idempotent` | 連跑兩次第二次 plan 為空 |
| `test_format_plan_human_readable` | 輸出含表名 + 欄位列表 |

Spark 全 mock，不依賴 dev-cluster。

### Catalog metadata 解析

**`tests/core/test_catalog.py`** 加 case：

- `test_dataset_config_preserves_tracks_flag`：`tracks_feature_table_schema=True` 在 DataCatalog 載入後仍可被讀到。

### Pipeline Node 注入

**`tests/pipelines/dataset/test_pipeline_verify.py`**（新檔）

- `test_create_pipeline_collects_tracked_tables`：給含標註的 catalog dict → 第一個 Node 是 `verify_feature_table_schema` 且 closure 含正確 6 張 fqn。
- `test_verify_node_passes_when_aligned`：mock spark + feature_table，無 diff → return passthrough。
- `test_verify_node_raises_with_actionable_message`：mock 出 diff → raise + 訊息含 `migrate_schema`。

### CLI

**`tests/test_cli.py`** 加 case：

- `test_migrate_schema_dry_run_default`：mock catalog + spark，無 `--apply` → exit 0、stdout 含 plan、未呼叫 `apply_migrations`。
- `test_migrate_schema_apply_executes`：`--apply` → 呼叫 `apply_migrations`。
- `test_migrate_schema_no_diff_silent_ok`：無變更 → exit 0 + `No schema changes needed.`。
- `test_migrate_schema_incompatible_change_exits_nonzero`：mock 出 type change → exit 1。

### 端到端驗收（dev-cluster，手動跑）

不寫進自動化 CI（dev-cluster 啟停慢），但列為**完工驗收 checklist**：

1. `scripts/setup_hive_dev.py` 重建 dev 表
2. 跑 `python -m recsys_tfb dataset --env production` → 6 張下游表生成（preprocessed + 5 張 model_input）
3. 在 setup 腳本加 `test_extra_feat DOUBLE` 重灌 feature_table
4. 重跑 `dataset` → verify Node fail，訊息提示 `migrate_schema`
5. `python -m recsys_tfb migrate_schema --env production` → 印出 6 張表 ADD COLUMNS plan
6. `python -m recsys_tfb migrate_schema --env production --apply` → 執行
7. 重跑 `dataset` → 通過、新 base_v partition 正確寫入
8. `SELECT * FROM ml_recsys.train_model_input WHERE base_dataset_version='<舊>' LIMIT 1` → `test_extra_feat` 顯示 NULL
9. `SELECT * FROM ml_recsys.train_model_input WHERE base_dataset_version='<新>' LIMIT 1` → `test_extra_feat` 有實值

## Open Questions

- `_expected_cols(feature_cols, parameters, fqn)` 內部要重複 dataset/_spark.py 的 column derivation 邏輯，可能漂移。考慮抽 helper 共用，但這屬於實作細節，留給 implementation plan 階段細化。
- production 環境的 `migrate_schema` 觸發流程（誰跑、什麼時候跑、怎麼 audit）—— 目前 dev-only。
