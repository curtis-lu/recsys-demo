# Dataset snap_date 記憶體疑慮（方向 A）Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 `apply_preprocessor_to_features` 的 per-column unknown `.count()`（N 次全表掃描）收斂成單次 aggregation，並文件化「dataset pipeline 已依 snap_date 物理分區、月份數不是記憶體瓶頸」與真正的 Spark 記憶體旋鈕。

**Architecture:** 不重構 DAG。一個行為不變的內部優化（單次 agg 取代 N 次 count）＋ 兩處文件（`parameters.yaml` spark 區塊註解、`docs/pipelines/dataset.md` 新章節）。設計依據見 `docs/superpowers/specs/2026-06-07-dataset-snap-date-memory-design.md`。

**Tech Stack:** PySpark 3.3.2、pytest 7.3.1（`spark` fixture，`caplog`）。

**與 spec 的一處細化**：spec 提的測試檔是 `tests/test_preprocessing/test_spark.py`，但 `apply_preprocessor_to_features` 的完整 fixture（feature_table / parameters）已存在於 `tests/test_pipelines/test_dataset/test_nodes_spark.py` 且該檔已 import 此函式，故測試放這裡（DRY，沿用既有 harness）。

---

## 環境前置（執行任何測試前，一次即可）

worktree 的 `.venv` 是指向 main 唯一真實 venv 的 symlink（`.venv` 不進版控，`git worktree add` 不會建）。先確認/補上，並 pre-flight：

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/dataset-snap-date-memory
[ -L .venv ] || ln -s /Users/curtislu/projects/recsys_tfb/.venv .venv
readlink .venv && /Users/curtislu/projects/recsys_tfb/.venv/bin/python -V   # 期望 Python 3.10.9
```

本計畫的測試用 in-memory `spark` fixture + `createDataFrame`（不讀 Hive 來源表、不寫 artifact），**不需** `data/` 子目錄 symlink。測試一律：絕對 venv python + `PYTHONPATH=<wt>/src`，並從 worktree root 執行。Spark 測試有 cold start，若單次 >2 分鐘可改背景執行。

---

## File Structure

- `src/recsys_tfb/preprocessing/_spark.py` — 修改 `apply_preprocessor_to_features` 內 `encode_categoricals` 區塊（Task 1）。
- `tests/test_pipelines/test_dataset/test_nodes_spark.py` — 新增 unknown-warning characterization 測試（Task 1）。
- `conf/base/parameters.yaml` — `spark:` 區塊補註解指引（Task 2）。
- `docs/pipelines/dataset.md` — 新增「## 規模與記憶體」章節（Task 2）。

不動：DAG/catalog/`nodes_spark.py` 包裝層/sampling/版本雜湊輸入。

---

## Task 1: 單次 aggregation 取代 per-column unknown `.count()`

這是**行為不變的重構**：先寫一個鎖住現有行為的 characterization 測試（在現行 N-count 實作下就應通過，證明它確實打到 encode-unknown 路徑），再改實作，最後確認測試仍綠。

**Files:**
- Test: `tests/test_pipelines/test_dataset/test_nodes_spark.py`（在檔案末端新增一個 class）
- Modify: `src/recsys_tfb/preprocessing/_spark.py:359-369`（`apply_preprocessor_to_features` 內）

- [ ] **Step 1: 在 `test_nodes_spark.py` 末端新增 characterization 測試**

把下列 class 追加到檔案最後（檔頭已 import `pandas as pd`、`apply_preprocessor_to_features`；`logging` 用方法內 local import，不動檔頭）：

```python
class TestApplyPreprocessorUnknownWarning:
    """apply_preprocessor_to_features 對每個含 unknown(-1) 的編碼欄各發一次 WARNING。
    鎖住此可觀察行為，確保 multi-count -> single-aggregation 的重構等價。"""

    def test_warns_per_column_with_unknown_categoricals(self, spark, caplog):
        import logging

        # feature_table 帶一個「非 identity」的類別特徵欄，資料含 fit 時沒見過的值
        # -> 編碼為 -1。identity_columns = [snap_date, cust_id, prod_name]，故
        # channel_preference 會進 encode_cols。
        ft = spark.createDataFrame(
            pd.DataFrame(
                {
                    "snap_date": pd.to_datetime(["2024-01-31"] * 3),
                    "cust_id": ["C001", "C002", "C003"],
                    "total_aum": [100.0, 200.0, 300.0],
                    "channel_preference": ["digital", "branch", "unseen_channel"],
                }
            )
        )
        preprocessor = {
            "feature_columns": ["channel_preference", "total_aum"],
            "categorical_columns": ["channel_preference"],
            # "unseen_channel" 刻意不在 mapping -> 編碼為 -1
            "category_mappings": {"channel_preference": ["digital", "branch"]},
            "drop_columns": [],
        }
        parameters = {
            "schema": {"categorical_values": {"prod_name": ["exchange_fx"]}},
            "dataset": {
                "train_snap_dates": ["2024-01-31"],
                "calibration_snap_dates": [],
                "val_snap_dates": [],
                "test_snap_dates": [],
            },
        }

        with caplog.at_level(logging.WARNING):
            result = apply_preprocessor_to_features(ft, preprocessor, parameters)
            pdf = result.orderBy("cust_id").toPandas()

        # digital->0, branch->1, unseen_channel->-1
        assert pdf["channel_preference"].tolist() == [0, 1, -1]
        warnings = [r.message for r in caplog.records if r.levelno == logging.WARNING]
        assert any(
            "1 unknowns in column 'channel_preference'" in m for m in warnings
        ), warnings
```

- [ ] **Step 2: 在現行（未改）實作下跑測試，確認 PASS（characterization 成立）**

Run:
```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/dataset-snap-date-memory
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/dataset-snap-date-memory/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  "tests/test_pipelines/test_dataset/test_nodes_spark.py::TestApplyPreprocessorUnknownWarning" -q
```
Expected: `1 passed`（證明測試確實走到 encode-unknown 警告路徑；現行 N-count 實作就會發出該 WARNING）。

- [ ] **Step 3: 重構實作為單次 aggregation**

在 `src/recsys_tfb/preprocessing/_spark.py` 的 `apply_preprocessor_to_features`，把這段（約 359-369 行）：

```python
    with log_step(logger, "encode_categoricals"):
        encode_cols = [c for c in categorical_cols if c in result.columns and c not in identity_cols]
        if encode_cols:
            result = _encode_categoricals(result, encode_cols, category_mappings)
            for col in encode_cols:
                n_unknown = result.filter(F.col(col) == -1).count()
                if n_unknown > 0:
                    logger.warning(
                        "apply_preprocessor_to_features: %d unknowns in column '%s'",
                        n_unknown, col,
                    )
```

改為：

```python
    with log_step(logger, "encode_categoricals"):
        encode_cols = [c for c in categorical_cols if c in result.columns and c not in identity_cols]
        if encode_cols:
            result = _encode_categoricals(result, encode_cols, category_mappings)
            # Single pass: one aggregation returns the unknown (-1) count for
            # every encoded column at once. The previous per-column .count()
            # re-scanned the full multi-month feature_table once per categorical
            # (N actions); this collapses it to a single scan.
            unknown_counts = result.agg(*[
                F.sum(F.when(F.col(c) == -1, 1).otherwise(0)).alias(c)
                for c in encode_cols
            ]).collect()[0]
            for col in encode_cols:
                n_unknown = unknown_counts[col] or 0
                if n_unknown > 0:
                    logger.warning(
                        "apply_preprocessor_to_features: %d unknowns in column '%s'",
                        n_unknown, col,
                    )
```

行為不變（同樣對有 unknown 的欄發相同 WARNING、數字相同），只是 N 次 action → 1 次掃描。`or 0` 防 `F.sum` 在無列時回 null。

- [ ] **Step 4: 重跑同一測試，確認仍 PASS（等價性）**

Run:
```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/dataset-snap-date-memory/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  "tests/test_pipelines/test_dataset/test_nodes_spark.py::TestApplyPreprocessorUnknownWarning" -q
```
Expected: `1 passed`。

- [ ] **Step 5: 跑同檔既有 apply_preprocessor 相關測試，確認無回歸**

Run（整個 test_nodes_spark.py，涵蓋 apply/build/select 路徑；Spark 測試，可背景）：
```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/dataset-snap-date-memory/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_pipelines/test_dataset/test_nodes_spark.py -q
```
Expected: 全數 passed（含新測試）。

- [ ] **Step 6: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/dataset-snap-date-memory
git add tests/test_pipelines/test_dataset/test_nodes_spark.py src/recsys_tfb/preprocessing/_spark.py
git commit -m "perf(dataset): collapse per-column unknown count() into one aggregation in apply_preprocessor_to_features"
```

---

## Task 2: 文件化 Spark 記憶體旋鈕與 snap_date 物理分區

純文件 / 註解，無程式邏輯，無測試。

**Files:**
- Modify: `conf/base/parameters.yaml`（`spark:` 區塊，約 11-21 行）
- Modify: `docs/pipelines/dataset.md`（在「## 重跑語意」與「## 接下來」之間插入新章節）

- [ ] **Step 1: `parameters.yaml` 的 `spark:` 區塊補註解指引**

把：

```yaml
  # spark.executor.memory: 4g
  # spark.executor.cores: 2
```

改為：

```yaml
  # spark.executor.memory: 4g
  # spark.executor.cores: 2
  #
  # --- 大規模（~10M entity）記憶體調參 ---
  # dataset pipeline 的 peak memory 取決於「單一 shuffle partition 大小」與 join
  # shuffle 量，而非框出幾個月（pipeline 已依 snap_date 物理分區，詳見
  # docs/pipelines/dataset.md §規模與記憶體）。主要旋鈕：
  #   shuffle.partitions 目標每分區 ~128–256MB → partitions ≈ shuffle_input_bytes / 128MB
  #   AQE（Spark 3.3 預設 on）勿關
  # dev/測試資料小用預設即可；生產取消下面兩行註解並依實測量調整：
  # spark.sql.shuffle.partitions: 400
  # spark.sql.adaptive.enabled: true
```

（全為註解，不改變任何生效值；`shuffle.partitions` 是 runtime SQL config，未來取消註解時會經 `utils/spark.py` 的 `builder.config` 套到 active session。）

- [ ] **Step 2: `docs/pipelines/dataset.md` 新增「## 規模與記憶體」章節**

把：

```markdown
- **怎麼指定要用哪個版本**：`--base-dataset-version` / `--train-variant`（預設取最新）。各層的版本對齊由框架自動處理（manifest ＋ `latest` symlink）。

## 接下來
```

改為：

```markdown
- **怎麼指定要用哪個版本**：`--base-dataset-version` / `--train-variant`（預設取最新）。各層的版本對齊由框架自動處理（manifest ＋ `latest` symlink）。

## 規模與記憶體

「12 個月一次抓」在這套 Spark backend 下**不等於把 12 個月載進記憶體**——pipeline 已在物理層按 `snap_date` 分區，四層保護：

1. **全程 lazy + spill**：`apply_preprocessor_to_features` / `build_model_input` 都是 transformation，executor 記憶體不足時 spill 到磁碟，不在 driver 累積。
2. **中間產物落 Hive、非 cache 於記憶體**：`preprocessed_feature_table` 是 `HiveTableDataset`（`partition_cols: [snap_date]`），編碼後寫回 Hive，下游各 split 從 Hive 讀；全程無 `.cache()` / `.persist()`。
3. **輸出依 snap_date dynamic partition 寫出**：每個 `*_model_input` 寫入即逐分區落地。
4. **join key 含 `time`**：`base_key = [snap_date] + entity`，故 keys⋈label⋈feature **永不跨 snap_date**，Spark 可逐分區處理。

因此決定 peak memory 的是**單一 shuffle partition 大小**與 join shuffle 量，不是月份數。真正的旋鈕在 `conf/base/parameters.yaml` 的 `spark:` 區塊：`spark.sql.shuffle.partitions`（每分區 ~128–256MB）、AQE（Spark 3.3 預設 on，勿關）、executor memory。

> 若日後在生產 10M 規模**實測**撞到單 job 記憶體天花板，逃生口是外部 per-snap_date 編排（先一次 fit 並持久化 preprocessor，再以凍結 preprocessor 逐月跑 apply+build；category mapping 必須一次 fit 完所有 train 月份，不可每月各自 fit）。非預設、目前不實作。

## 接下來
```

- [ ] **Step 3: 抽查渲染/格式**

Run（確認新章節存在、`spark:` 註解格式正確且不破壞 YAML）：
```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/dataset-snap-date-memory
grep -n "## 規模與記憶體" docs/pipelines/dataset.md
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -c "import yaml; yaml.safe_load(open('conf/base/parameters.yaml')); print('parameters.yaml OK')"
```
Expected: grep 命中一行；印出 `parameters.yaml OK`。

- [ ] **Step 4: Commit**

```bash
git add conf/base/parameters.yaml docs/pipelines/dataset.md
git commit -m "docs(dataset): snap_date partitioning is not a memory bottleneck + shuffle.partitions sizing guidance"
```

---

## Self-Review（plan 對 spec）

- **spec 交付物 1（單次 agg 小修）** → Task 1（Step 3 實作 + Step 1/2/4 TDD characterization + Step 5 回歸）。✓
- **spec 交付物 2（parameters.yaml 註解 + dataset.md 章節）** → Task 2 Step 1 / Step 2。✓
- **spec「行為不變、N→1」** → Task 1 同一測試在改前改後都綠（Step 2 vs Step 4）。✓
- **spec YAGNI（不重構 DAG、不硬編生產值、B 只留文件）** → Task 2 只加註解 + 一段逃生口文字，無程式/DAG 改動。✓
- **Placeholder 掃描**：無 TBD/TODO；每個程式步驟都附完整碼與確切指令。✓
- **型別/命名一致**：`unknown_counts` Row、`F.sum(F.when(...).otherwise(0))`、`category_mappings` key 命名與 `_encode_categoricals` 一致；測試用的 `identity_columns = [snap_date, cust_id, prod_name]` 與 `core/schema.get_schema` 推導一致。✓

## 收尾

兩個 commit 完成後，功能完整且測試綠。後續整合（merge/PR）走 finishing-a-development-branch；本計畫不含 merge 步驟。
