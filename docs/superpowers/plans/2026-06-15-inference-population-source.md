# Inference 推論母體來源表（`inference_population`）Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 為 inference 引入明確的推論母體來源表 `inference_population`，把「誰該被推論（membership）」與「他有什麼特徵（`feature_table` enrichment）」分開；缺特徵的母體成員允許保留並以 `feature_present` 在 in-memory 註記 + log 回報。

**Architecture:** 比照 training 端 `sample_pool` 的既有模式——新增一張由 source_etl 產出、grain `(time, entity)`、唯一性由 ETL `primary_key` + `quality_checks` 保證的 Hive 來源表。`build_scoring_dataset` 改以該表為母體、移除 `dropDuplicates`、left-join `feature_table` 接特徵並標記 `feature_present`。下游 `predict_scores` / `rank_predictions` / `validate_predictions` 邏輯不變；`feature_present` **不**寫入 Hive 輸出表。

**Tech Stack:** PySpark 3.3.2、自製 source_etl（`SQLRunner` + YAML `tables`）、自製 `DataCatalog`/`Pipeline`、pytest（conftest `spark` fixture，`local[1]`）。

**Spec:** `docs/superpowers/specs/2026-06-14-inference-population-source-design.md`

---

## Base 與排序（已處理）

- PR #84（audit-fix）已 merge 進 main（`f63ebee`）。本分支已在**實作前**乾淨 rebase 到該 main 之上（branch 當時只有 docs commits，無 src 衝突）。
- 因此 base 已含 audit-fix：`build_scoring_dataset` 為 #84 版（含 snap_dates date-cast、missing-date 檢查、`dropDuplicates`），`predict_scores` 為 #84 的 `feature_names` 版。
- Task 3 以「整段替換 `build_scoring_dataset`」進行：取代 #84 在該函式引入的 missing-date 檢查與 `dropDuplicates`，改以 `inference_population` 為母體。**`predict_scores` 不改**，維持 #84 的 `feature_names` 版本。

## 檔案結構（先 map）

| 檔案 | 動作 | 責任 |
|---|---|---|
| `conf/sql/etl/inference_population/inference_population.sql` | Create | 母體業務邏輯（示例：對 feature 來源取 distinct `(time, entity)`） |
| `conf/base/parameters_inference_population_etl.yaml` | Create | ETL 設定：table、partition、`primary_key`、`quality_checks` |
| `conf/base/catalog.yaml` | Modify | 新增 `inference_population`（`HiveTableDataset`, read_only） |
| `src/recsys_tfb/__main__.py` | Modify | 新增 `@app.command("inference_population_etl")` |
| `scripts/generate_synthetic_data.py` | Modify | `generate_inference_population()` + 寫 parquet |
| `scripts/local_spark_setup.py` | Modify | `TABLES` 加 `inference_population` |
| `src/recsys_tfb/pipelines/inference/nodes_spark.py` | Modify | 重寫 `build_scoring_dataset` |
| `src/recsys_tfb/pipelines/inference/pipeline.py` | Modify | `build_scoring_dataset` 接 `inference_population` |
| `tests/test_pipelines/test_inference/test_nodes_spark.py` | Modify | `build_scoring_dataset` 新行為測試 |
| `tests/scripts/test_generate_synthetic_data.py` | Create/Modify | 母體 grain 唯一性測試 |
| `README.md` / `docs/pipelines/inference.md` / `docs/pipelines/evaluation.md` | Modify | 文件（隨程式落地） |

執行測試一律：
`PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest <paths> -q`（在 worktree root，`export SPARK_LOCAL_IP=127.0.0.1` 避免 driver bind 噪音；Spark 測試逐檔分開跑）。

---

## Task 1: `inference_population` 來源表（ETL 設定 + SQL + catalog + CLI 命令）

**Files:**
- Create: `conf/sql/etl/inference_population/inference_population.sql`
- Create: `conf/base/parameters_inference_population_etl.yaml`
- Modify: `conf/base/catalog.yaml`（在 `sample_pool` entry 後）
- Modify: `src/recsys_tfb/__main__.py`（在 `sample_pool_etl` 命令後）
- Test: `tests/test_pipelines/test_source_etl/test_inference_population_config.py`

- [ ] **Step 1: 寫失敗測試（ETL 設定可載入且 grain 正確）**

`tests/test_pipelines/test_source_etl/test_inference_population_config.py`：

```python
from pathlib import Path
import yaml

CONF = Path(__file__).resolve().parents[3] / "conf" / "base"


def test_inference_population_etl_config_shape():
    cfg = yaml.safe_load(
        (CONF / "parameters_inference_population_etl.yaml").read_text()
    )
    assert "inference_population_etl" in cfg
    tables = cfg["inference_population_etl"]["tables"]
    assert len(tables) == 1
    t = tables[0]
    assert t["name"] == "inference_population"
    assert t["sql_file"] == "inference_population/inference_population.sql"
    # grain = (time, entity) = (snap_date, cust_id)；唯一性由 ETL 保證
    assert t["primary_key"] == ["snap_date", "cust_id"]
    assert t["quality_checks"]["max_duplicate_key_ratio"] == 0.0


def test_inference_population_catalog_entry():
    cat = yaml.safe_load((CONF / "catalog.yaml").read_text())
    entry = cat["inference_population"]
    assert entry["type"] == "HiveTableDataset"
    assert entry["table"] == "inference_population"
    assert entry["read_only"] is True
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_pipelines/test_source_etl/test_inference_population_config.py -q`
Expected: FAIL（檔案不存在 → `FileNotFoundError`/`KeyError`）。

- [ ] **Step 3: 建立 ETL SQL**

`conf/sql/etl/inference_population/inference_population.sql`（示例最小版＝取 distinct 母體；使用者之後放真實業務資格邏輯）：

```sql
-- inference 推論母體：每個 snap_date 要被評分的 (time, entity)。
-- grain = (snap_date, cust_id)，一 entity 一列。
-- 示例最小版＝對 feature 來源取 distinct；正式環境改為「在世/未流失/符合資格」客戶。
SELECT DISTINCT
    snap_date,
    cust_id
FROM ${target_db}.feature_concat
WHERE snap_date = DATE('${target_date}')
```

- [ ] **Step 4: 建立 ETL 設定（比照 `parameters_sample_pool_etl.yaml`）**

`conf/base/parameters_inference_population_etl.yaml`：

```yaml
# Inference 推論母體 ETL pipeline 配置
# 上游依賴 feature_etl 產出的 feature_concat。對應 training 端的 sample_pool，
# 但 grain 為 (snap_date, cust_id) 且時間切點為 inference 當期 snap_date。

inference_population_etl:
  dry_run: false   # 與 sample_pool 一致：local 也實寫表
  variables:
    target_db: "ml_recsys"

  source_checks: {}

  tables:
    - name: inference_population
      sql_file: inference_population/inference_population.sql
      partition_by:
        snap_date: DATE
      primary_key: [snap_date, cust_id]
      quality_checks:
        max_duplicate_key_ratio: 0.0

  audit:
    database: "${target_db}"
    table: etl_audit_log
```

- [ ] **Step 5: 新增 catalog entry（在 `sample_pool` 之後）**

`conf/base/catalog.yaml`，於 `sample_pool` entry 後加入：

```yaml
inference_population:
  type: HiveTableDataset
  database: ${hive.db}
  table: inference_population
  read_only: true
```

- [ ] **Step 6: 新增 CLI 命令（在 `sample_pool_etl` 命令之後）**

`src/recsys_tfb/__main__.py`，於 `sample_pool_etl` 命令定義後加入（複製 `feature_etl` 命令簽名，僅改名稱與 docstring）：

```python
@app.command(name="inference_population_etl")
def inference_population_etl(
    env: str = typer.Option("local", "--env", "-e", help="Config environment"),
    target_dates: Optional[str] = typer.Option(
        None,
        "--target-dates",
        help="Comma-separated target dates, e.g. 2024-01-31,2024-02-29",
    ),
    restart_from: Optional[str] = typer.Option(
        None,
        "--restart-from",
        help="Restart from this table name (skip earlier tables in the list)",
    ),
    source_check: bool = typer.Option(
        False, "--source-check",
        help="只跑該 stage 的上游 source_checks（preflight），不執行 ETL／不寫表；"
             "全部跑完後有任一失敗即以非零碼結束。",
    ),
):
    """Run the inference population ETL pipeline (inference_population)."""
    _run_etl(
        "inference_population_etl", env, target_dates, restart_from,
        source_check_only=source_check,
    )
```

- [ ] **Step 7: 跑測試確認通過**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_pipelines/test_source_etl/test_inference_population_config.py -q`
Expected: PASS（2 passed）。

- [ ] **Step 8: 確認 CLI 命令已註冊**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m recsys_tfb --help`
Expected: 輸出含 `inference_population_etl`。

- [ ] **Step 9: Commit**

```bash
git add conf/sql/etl/inference_population/inference_population.sql \
        conf/base/parameters_inference_population_etl.yaml \
        conf/base/catalog.yaml src/recsys_tfb/__main__.py \
        tests/test_pipelines/test_source_etl/test_inference_population_config.py
git commit -m "feat(source_etl): 新增 inference_population 母體來源表（ETL+catalog+CLI）"
```

---

## Task 2: 合成資料 + 本機 Hive 註冊（讓本機 inference 可端到端跑）

**Files:**
- Modify: `scripts/generate_synthetic_data.py`（`generate_sample_pool` 後 + `main()`）
- Modify: `scripts/local_spark_setup.py`（`TABLES` dict）
- Test: `tests/scripts/test_generate_synthetic_data.py`

- [ ] **Step 1: 寫失敗測試（母體 grain 唯一 + 為 feature 客戶子集）**

`tests/scripts/test_generate_synthetic_data.py`（若已存在則新增測試函式）：

```python
import numpy as np
from scripts.generate_synthetic_data import (
    generate_feature_table,
    generate_inference_population,
)


def test_inference_population_grain_unique():
    rng = np.random.default_rng(0)
    ft = generate_feature_table(rng)
    pop = generate_inference_population(ft)
    # 一 (snap_date, cust_id) 一列
    assert not pop.duplicated(subset=["snap_date", "cust_id"]).any()
    # 母體 ⊆ feature_table 的 (snap_date, cust_id)
    ft_keys = set(map(tuple, ft[["snap_date", "cust_id"]].drop_duplicates().values))
    pop_keys = set(map(tuple, pop[["snap_date", "cust_id"]].values))
    assert pop_keys <= ft_keys
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `PYTHONPATH=. /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/scripts/test_generate_synthetic_data.py::test_inference_population_grain_unique -q`
Expected: FAIL（`ImportError: cannot import name 'generate_inference_population'`）。

- [ ] **Step 3: 新增 generator（`generate_sample_pool` 之後）**

`scripts/generate_synthetic_data.py`：

```python
def generate_inference_population(feature_table: pd.DataFrame) -> pd.DataFrame:
    """Inference 推論母體：每個 snap_date 的 distinct (snap_date, cust_id)。

    示例最小版＝feature 客戶全集；正式環境由 ETL SQL 放入資格邏輯。
    """
    return (
        feature_table[["snap_date", "cust_id"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
```

- [ ] **Step 4: 接進 `main()` 的產出與寫檔**

`scripts/generate_synthetic_data.py` `main()`：在 `sample_pool = generate_sample_pool(...)` 後加：

```python
    inference_population = generate_inference_population(feature_table)
```

並在寫檔的 `for path, df in [...]` 清單加入一列：

```python
        ("data/inference_population.parquet", inference_population),
```

- [ ] **Step 5: 本機 Hive 註冊**

`scripts/local_spark_setup.py` 的 `TABLES` dict 加入：

```python
    "inference_population": DATA / "inference_population.parquet",
```

- [ ] **Step 6: 跑測試確認通過**

Run: `PYTHONPATH=. /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/scripts/test_generate_synthetic_data.py::test_inference_population_grain_unique -q`
Expected: PASS。

- [ ] **Step 7: 重建本機資料 + 確認表存在**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/inference-population-source
export SPARK_CONF_DIR=$PWD/conf/spark-local
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python scripts/local_spark_setup.py --reset
```
Expected: `SHOW TABLES IN ml_recsys` 含 `inference_population`。

- [ ] **Step 8: Commit**

```bash
git add scripts/generate_synthetic_data.py scripts/local_spark_setup.py \
        tests/scripts/test_generate_synthetic_data.py
git commit -m "feat(scripts): 合成 inference_population + 本機 Hive 註冊"
```

---

## Task 3: 重寫 `build_scoring_dataset` 以母體為準 + pipeline 接線

**Files:**
- Modify: `src/recsys_tfb/pipelines/inference/nodes_spark.py`（`build_scoring_dataset`）
- Modify: `src/recsys_tfb/pipelines/inference/pipeline.py`
- Test: `tests/test_pipelines/test_inference/test_nodes_spark.py`

- [ ] **Step 1: 寫失敗測試（membership / 缺特徵保留+flag / 不需 dropDuplicates / missing snap_date raise）**

`tests/test_pipelines/test_inference/test_nodes_spark.py`，新增 class（自含資料，僅依賴 conftest `spark` fixture）：

```python
class TestBuildScoringDatasetPopulation:
    def _params(self):
        return {
            "schema": {"columns": {
                "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
                "score": "score", "rank": "rank",
            }},
            "inference": {"snap_dates": ["2024-03-31"],
                          "products": ["fund_stock", "fund_bond"]},
        }

    def _pop(self, spark):
        # 母體 3 位客戶（c3 之後在 feature_table 缺特徵）
        return spark.createDataFrame(
            [("2024-03-31", "c1"), ("2024-03-31", "c2"), ("2024-03-31", "c3")],
            ["snap_date", "cust_id"],
        )

    def _features(self, spark):
        # 只有 c1, c2 有特徵；另含母體外的 c9（不該出現在輸出）
        return spark.createDataFrame(
            [("2024-03-31", "c1", 1.0), ("2024-03-31", "c2", 2.0),
             ("2024-03-31", "c9", 9.0)],
            ["snap_date", "cust_id", "total_aum"],
        )

    def test_membership_from_population_not_feature_table(self, spark):
        from recsys_tfb.pipelines.inference.nodes_spark import build_scoring_dataset
        out = build_scoring_dataset(self._pop(spark), self._features(spark), self._params())
        custs = {r["cust_id"] for r in out.select("cust_id").distinct().collect()}
        assert custs == {"c1", "c2", "c3"}          # c9 不在母體 → 不出現

    def test_missing_feature_member_kept_and_flagged(self, spark):
        from recsys_tfb.pipelines.inference.nodes_spark import build_scoring_dataset
        out = build_scoring_dataset(self._pop(spark), self._features(spark), self._params())
        flags = {r["cust_id"]: r["feature_present"]
                 for r in out.select("cust_id", "feature_present").distinct().collect()}
        assert flags == {"c1": True, "c2": True, "c3": False}

    def test_row_count_is_members_times_products(self, spark):
        from recsys_tfb.pipelines.inference.nodes_spark import build_scoring_dataset
        out = build_scoring_dataset(self._pop(spark), self._features(spark), self._params())
        assert out.count() == 3 * 2                  # 3 members × 2 products

    def test_missing_snap_date_raises(self, spark):
        from recsys_tfb.pipelines.inference.nodes_spark import build_scoring_dataset
        params = self._params()
        params["inference"]["snap_dates"] = ["2024-03-31", "2024-04-30"]
        with pytest.raises(ValueError, match="inference_population missing inference.snap_dates"):
            build_scoring_dataset(self._pop(spark), self._features(spark), params)
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `SPARK_LOCAL_IP=127.0.0.1 PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_pipelines/test_inference/test_nodes_spark.py::TestBuildScoringDatasetPopulation -q`
Expected: FAIL（`build_scoring_dataset` 仍是舊簽名 `(feature_table, parameters)` → `TypeError`）。

- [ ] **Step 3: 重寫 `build_scoring_dataset`**

`src/recsys_tfb/pipelines/inference/nodes_spark.py`，整段替換 `build_scoring_dataset`（保留檔頭 import：`F`, `pd`, `log_step`, `get_schema`）：

```python
def build_scoring_dataset(
    inference_population: DataFrame,
    feature_table: DataFrame,
    parameters: dict,
) -> DataFrame:
    """以 inference_population 為母體建立評分資料；feature_table 僅作 enrichment。

    母體 grain (time, entity) 由 source_etl 保證唯一，故不需 dropDuplicates。
    缺特徵的母體成員保留，以 feature_present=false 標記（in-memory + log，不下推）。
    """
    schema = get_schema(parameters)
    time_col = schema["time"]
    entity_cols = schema["entity"]
    item_col = schema["item"]
    join_key = [time_col] + entity_cols

    snap_dates = [
        pd.Timestamp(value).date()
        for value in parameters["inference"]["snap_dates"]
    ]
    products = parameters["inference"]["products"]
    spark = feature_table.sparkSession

    with log_step(logger, "read_population"):
        customers = (
            inference_population
            .filter(F.col(time_col).cast("date").isin(snap_dates))
            .select(*join_key)
        )
        available_dates = {
            pd.Timestamp(row[time_col]).date()
            for row in customers.select(time_col).distinct().collect()
            if row[time_col] is not None
        }
        missing_dates = sorted(set(snap_dates) - available_dates)
        if missing_dates:
            raise ValueError(
                "inference_population missing inference.snap_dates: "
                f"{[value.isoformat() for value in missing_dates]}"
            )

    with log_step(logger, "feature_coverage_report"):
        # 用窄投影（join_key + 指標）算覆蓋，避免在 wide feature 上做聚合
        ft_keys = (
            feature_table.select(*join_key).distinct()
            .withColumn("_ft_present", F.lit(True))
        )
        presence = customers.join(ft_keys, on=join_key, how="left")
        coverage = (
            presence.groupBy(time_col)
            .agg(
                F.count(F.lit(1)).alias("members"),
                F.sum(
                    F.when(F.col("_ft_present").isNull(), F.lit(1)).otherwise(F.lit(0))
                ).alias("members_missing_features"),
            )
            .collect()
        )
        for row in coverage:
            logger.info(
                "feature coverage %s=%s: members=%d missing_features=%d",
                time_col, row[time_col], row["members"],
                row["members_missing_features"],
            )

    with log_step(logger, "cross_join"):
        products_df = spark.createDataFrame([(p,) for p in products], [item_col])
        scoring = customers.crossJoin(products_df)

    with log_step(logger, "merge_features"):
        ft = feature_table.withColumn("_ft_present", F.lit(True))
        scoring = scoring.join(ft, on=join_key, how="left")
        scoring = scoring.withColumn(
            "feature_present", F.col("_ft_present").isNotNull()
        ).drop("_ft_present")

    logger.info(
        "Built scoring dataset for %d products x %d snap_dates",
        len(products),
        len(snap_dates),
    )
    return scoring
```

> 效能註記：母體與 feature_table 各掃描兩次（覆蓋報表用窄投影、scoring 用全欄）。母體小、窄投影掃描便宜；若 profiling 顯示重複掃描有感，再對 `customers` 加 `.cache()`。`feature_present` 僅存在於回傳的 in-memory `scoring`，不寫入任何 Hive 輸出表。

- [ ] **Step 4: 改 pipeline 接線**

`src/recsys_tfb/pipelines/inference/pipeline.py`，將 `build_scoring_dataset` node 的 inputs 改為：

```python
            Node(
                build_scoring_dataset,
                inputs=["inference_population", "feature_table", "parameters"],
                outputs="scoring_dataset",
            ),
```

- [ ] **Step 5: 跑新測試確認通過**

Run: `SPARK_LOCAL_IP=127.0.0.1 PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_pipelines/test_inference/test_nodes_spark.py::TestBuildScoringDatasetPopulation -q`
Expected: PASS（4 passed）。

- [ ] **Step 6: 更新/移除舊 `build_scoring_dataset` 測試**

舊測試 class（`TestBuildScoringDataset`）以舊簽名 `build_scoring_dataset(feature_table, parameters)` 呼叫，需更新為新簽名（補一個 `inference_population` 引數，內容為 feature_table 的 distinct `(snap_date, cust_id)`），或以新 class 取代。逐一檢視 `tests/test_pipelines/test_inference/test_nodes_spark.py` 內呼叫 `build_scoring_dataset(` 的測試並修正簽名。

- [ ] **Step 7: 跑整個 inference nodes 測試檔確認綠燈**

Run: `SPARK_LOCAL_IP=127.0.0.1 PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_pipelines/test_inference/test_nodes_spark.py -q`
Expected: PASS（0 failed）。

- [ ] **Step 8: Commit**

```bash
git add src/recsys_tfb/pipelines/inference/nodes_spark.py \
        src/recsys_tfb/pipelines/inference/pipeline.py \
        tests/test_pipelines/test_inference/test_nodes_spark.py
git commit -m "feat(inference): build_scoring_dataset 以 inference_population 為母體 + feature_present 註記"
```

---

## Task 4: 文件（隨程式落地）

**Files:** `README.md`、`docs/pipelines/inference.md`、`docs/pipelines/evaluation.md`

> 內容依 spec「文件範圍」節。重點：母體 `inference_population`（對應 `sample_pool`）、membership vs enrichment 分界、`feature_present`（in-memory + log，**不下推**）、移除 `dropDuplicates`、evaluation `segment_sources` 可指向 `inference_population`（跟著 eval 模式母體走）。

- [ ] **Step 1: `docs/pipelines/inference.md`**
  - §1 主要輸入加 `inference_population`；`(time,entity,item)` 欄位表**不**加 `feature_present`（不在輸出表）；改為在 §5.1 說明它是 in-memory 註記 + log。
  - §2 執行前準備 #4/#5：母體就緒查 `inference_population`、grain 由 ETL 保證。
  - 新增 §3.5「推論母體（`inference_population`）」：grain / ETL `primary_key`+`quality_checks` / 業務邏輯 / 分群欄。
  - §5.1 改寫：母體/特徵分離、移除 `dropDuplicates`、缺特徵保留 + `feature_present`（in-memory）+ log 回報。
  - §8 錯誤：`feature_table missing` → `inference_population missing inference.snap_dates`。
  - §9 限制 + §10 連結同步。

- [ ] **Step 2: `docs/pipelines/evaluation.md`**
  - §3.2 分群評估加「情境→預測來源→建議 segment source」表（monitoring→`inference_population`、post-training→`sample_pool`），說明對齊實際母體、零程式改動。

- [ ] **Step 3: `README.md`**
  - §0 三張建模來源表後加 `inference_population` 母體註記（membership vs enrichment）；推論輸出**不**加 `feature_present`（in-memory + log，非輸出欄）。
  - §2 source ETL 概述加 `inference_population_etl`；§2 evaluation 分群列加情境提示；§3 建表補 `parameters_inference_population_etl.yaml`。

- [ ] **Step 4: Commit**

```bash
git add README.md docs/pipelines/inference.md docs/pipelines/evaluation.md
git commit -m "docs: inference_population 母體 + segment 來源對齊（隨實作落地）"
```

---

## Task 5: 本機端到端驗證

- [ ] **Step 1: pre-flight + 資料隔離閘**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/inference-population-source && pwd
readlink .venv && /Users/curtislu/projects/recsys_tfb/.venv/bin/python -V
export SPARK_CONF_DIR=$PWD/conf/spark-local
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python scripts/local_spark_setup.py --check-isolation
```
Expected: 隔離檢查通過（全在 worktree 內）。

- [ ] **Step 2: 跑 inference pipeline（讀 `inference_population` 母體）**

先確認本機已有可用 `best` 模型（否則先跑 dataset+training，或用既有版本）。設定 `parameters_inference.yaml` 的 `snap_dates` 後：

```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m recsys_tfb inference --env local
```
Expected: log 出現 `feature coverage snap_date=...: members=... missing_features=...`；pipeline 成功發布 `ranked_predictions`。

- [ ] **Step 3: 抽查輸出表不含 `feature_present`**

```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python - <<'PY'
import os; os.environ.setdefault("SPARK_CONF_DIR", os.getcwd()+"/conf/spark-local")
from recsys_tfb.utils.spark import get_or_create_spark_session
s = get_or_create_spark_session({})
print(s.table("ml_recsys.ranked_predictions").columns)
PY
```
Expected: 欄位**不含** `feature_present`（確認未下推）。

- [ ] **Step 4: 全量相關測試逐檔分開跑**

```bash
export SPARK_LOCAL_IP=127.0.0.1
PY=/Users/curtislu/projects/recsys_tfb/.venv/bin/python
for f in tests/test_pipelines/test_inference/test_nodes_spark.py \
         tests/test_pipelines/test_inference/test_validation.py \
         tests/test_pipelines/test_source_etl/test_inference_population_config.py \
         tests/scripts/test_generate_synthetic_data.py; do
  echo "== $f =="; PYTHONPATH=src $PY -m pytest "$f" -q | tail -3
done
```
Expected: 全部 passed。

- [ ] **Step 5: （收尾）確認與 #84 的 rebase 已處理**

確認 `build_scoring_dataset` 為本計畫版本、`predict_scores` 為 #84 的 `feature_names` 版本，且 `git log` 線性。

---

## 自我檢查（writing-plans self-review）

- **Spec coverage**：母體表(Task1)、唯一性 ETL 保證(Task1)、build_scoring_dataset 換母體+移除 dropDuplicates(Task3)、feature_present in-memory+log+不下推(Task3 + 驗證 Task5/Step3)、segment 來源對齊(Task4)、文件範圍(Task4)、合成資料/本機(Task2)。✅
- **Placeholder scan**：各步驟皆含實際 SQL/YAML/Python/指令，無 TBD。✅
- **Type/簽名一致**：`build_scoring_dataset(inference_population, feature_table, parameters)` 於 nodes_spark / pipeline / 測試三處一致；catalog 名 `inference_population` 於 catalog/pipeline/local_spark_setup/SQL `${target_db}.inference_population` 一致；ETL stage 名 `inference_population_etl` 於 CLI 命令 / YAML 頂層 key / `_run_etl` lookup 一致。✅
- **不在範圍**：不改 `consistency.py`、不下推 `feature_present`、不改 `predict_scores`/`rank_predictions`/`validate_predictions` 核心邏輯。✅
