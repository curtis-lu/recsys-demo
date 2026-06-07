# `--source-check` preflight gate ＋ output checks fail-fast 設計

- 日期：2026-06-07
- 狀態：設計核准，待 spec 複審
- 分支：`feat/source-check-subcommand`

## 背景與現況

「source_etl pipeline」實際上是三個 CLI 子命令 `feature_etl` / `label_etl` /
`sample_pool_etl`，都走 `__main__.py` 的 `_run_etl()` → `SQLRunner.run()`
（`src/recsys_tfb/pipelines/source_etl/sql_runner.py`）。

`source_checks`（上游表新鮮度／schema 檢查）定義在各
`conf/base/parameters_{feature,label,sample_pool}_etl.yaml` 的 stage 區塊，目前
**內嵌**在 ETL 執行流程裡：`SQLRunner.run()` 對每個 snap_date 先跑
`_run_source_checks()` 再跑 tables。

現行失敗語意（見 `docs/pipelines/source_etl.md` §重跑語意）：

| 失敗類型 | 現行行為 | 對外 exit code |
|---|---|---|
| `source_checks`（上游） | 跳過該 snap_date、**繼續下一個日期** | 0（不丟例外） |
| output `quality_checks`（輸出） | `break` 該 snap_date 剩餘表、**繼續下一個日期** | **0**（silent，bug） |
| SQL / Spark 執行錯誤 | raise `SourceETLError`，**中止整個 run** | 1 |

兩個問題：(1) source_check 與 output check 失敗都「跳過繼續」，不符合「失敗應中
斷」；(2) output check 失敗時 `run()` 不丟例外，CLI 最後仍印
`completed successfully` 並 **exit 0** —— 品質檢查失敗卻對外回報成功。

> 前提：`INSERT OVERWRITE` 無 transaction、不會 rollback。一個 snap_date 的
> tables 逐張覆寫，所以不論採哪種失敗語意，失敗那天「已寫的表保留、後面的表沒
> 寫」的**部分寫入狀態都會留下**；差別只在失敗後要不要繼續做別的日期。重跑靠既
> 有的 `--restart-from <table>`（跳過該表之前的表）續跑。

## 目標

1. 把 source_check 抽成各 ETL 指令上的 `--source-check` 旗標（preflight 模式）：
   只跑該 stage 的 source_checks、**不寫表**、跑遍全部日期×全部檢查（collect-all）、
   有任一失敗即 `exit 1` 並印出可操作的失敗報告。
2. 統一失敗語意為「任一失敗即停下來讓你修」：
   - 正常 ETL run **不再跑 source_checks**。
   - output `quality_checks` 失敗改 **fail-fast 中止整個 run** ＋ 誠實的 `exit 1`
     （修掉 silent exit-0）。
   - SQL / Spark 執行錯誤維持中止。
3. 提供好的修復／重跑介面：console 結構化報告（含 expected vs actual、依檢查類型
   的修復提示、重跑指令）＋ 沿用 `etl_audit_log` 稽核。
4. 更新 README 與 `docs/`，補一份 `source_checks` 設定參考。

## 非目標

- 不改三個 ETL 的執行順序、`depends_on` 語意、SQL render / CTAS / schema
  evolution 等既有機制。
- 不新增頂層 `source_check` 指令（採旗標形式 `feature_etl --source-check`）。
- 不改 output `quality_checks` 的檢查內容（min_row_count / max_duplicate_key_ratio
  / max_null_ratio / schema contract），只改失敗後的控制流。
- 不為 `--source-check` 另寫報告檔（JSON/Markdown）；console ＋ audit log 即可。

## 設計決策（已於釐清階段定案）

| # | 決策 |
|---|---|
| D1 | `--source-check` 是 `feature_etl`/`label_etl`/`sample_pool_etl` 三指令各自的布林旗標，scope 為該 stage。 |
| D2 | 正常 ETL run（不帶旗標）**不再跑 source_checks**。檢查只在 `--source-check` 模式進行。 |
| D3 | output `quality_checks` 失敗 → fail-fast 中止整個 run ＋ `exit 1`。 |
| D4 | `--source-check` 唯讀：collect-all 跑遍全部日期×檢查，有任一失敗才 `exit 1`。 |
| D5 | 失敗介面 = console 結構化報告 ＋ `etl_audit_log` 稽核（不另寫報告檔）。 |
| D6 | `--source-check` ＋ `--restart-from` 同時給 → 報錯（restart 對純檢查無意義）。 |
| D7 | 產報告靠 enrich `CheckResult`（加欄位），不另開 report 結構。 |

## 詳細設計

### 1. CLI（`src/recsys_tfb/__main__.py`）

三個 ETL 指令各加一個旗標：

```python
source_check: bool = typer.Option(
    False, "--source-check",
    help="只跑該 stage 的上游 source_checks（preflight），不執行 ETL／不寫表；"
         "全部跑完後有任一失敗即以非零碼結束。",
),
```

`_run_etl()` 多收一個 `source_check_only: bool` 參數，分支如下：

- **`--source-check`（preflight）**
  - 若同時給 `--restart-from` → `logger.error` 後 `raise typer.Exit(code=1)`（D6）。
  - **強制起 Spark、忽略 `dry_run`**：source_check 唯讀、不寫表，必須實際查 Hive，
    不受 `dry_run` / `--env local` 預設影響（現行 dry-run 不起 Spark、跳過
    source_checks）。
  - 解析 `target_dates`（同正常流程：`--target-dates` 優先，否則讀 config
    `target_dates`；空則報錯 `exit 1`）。
  - 呼叫 `runner.run_source_checks(target_dates=date_list, run_id=run_context.run_id)`。
  - 攔 `SourceCheckError`：以 `logger.error("%s", exc)` 印出乾淨報告（**不丟
    stacktrace**）、`raise typer.Exit(code=1)` —— 仿 `_load_config_and_setup` 對
    `ConfigConsistencyError` 的處理。
  - 全通過則 `logger.info` 印 summary（"Source check passed: M/M checks"）。
- **正常 run（不帶旗標）**：與現行相同，呼叫
  `runner.run(target_dates, restart_from, run_id)`，但因 D2，`run()` 內已不再跑
  source_checks。

三個 typer 指令把新參數轉進 `_run_etl(..., source_check_only=source_check)`。

### 2. `SQLRunner` 重構（`sql_runner.py`）

**2a. 移除正常 run 的 source-check gate**

刪掉 `run()` 內現有區塊（約 `sql_runner.py:118-122`）：

```python
# 移除：
if not self._dry_run and self._source_checks:
    if not self._run_source_checks(spark, snap_date, run_id, audit):
        snap_status = "skipped_source_check"
        continue
```

`_run_source_checks()`（現 private、回傳 bool）由下方 2b 的新方法取代。

**2b. 新增 `run_source_checks()`（preflight 入口）**

```python
def run_source_checks(
    self,
    target_dates: list[str],
    run_id: str | None = None,
) -> None:
    """Preflight：對全部 target_dates 跑 source_checks，collect-all。

    全部跑完後若有任一失敗，raise SourceCheckError（攜帶結構化失敗清單）；
    每筆失敗寫一筆 etl_audit_log（table_name="__source_check__"）。不寫任何
    輸出表。
    """
    if run_id is None:
        run_id = generate_run_id()
    if not self._source_checks:
        logger.warning("No source_checks configured; nothing to check.")
        return

    spark, audit = self._initialize_context()   # 見 2d：source-check 模式不可 dry-run
    checker = SourceChecker(spark)

    all_results: list[CheckResult] = []          # CheckResult 已 enrich（見 3）
    for snap_date in target_dates:
        results = checker.run_all(self._source_checks, snap_date)
        all_results.extend(results)

    failed = [r for r in all_results if not r.passed]
    if failed:
        for r in failed:
            if audit:
                audit.write_record(AuditRecord(
                    run_id=run_id, snap_date=r.snap_date,
                    table_name="__source_check__", status="failed",
                    error_message=r.message,
                ))
        raise SourceCheckError(all_results, self._stage)   # 報告在例外內格式化（見 4）
    logger.info("Source check passed: %d/%d checks", len(all_results), len(all_results))
```

`SourceChecker.run_all()` 改為回傳 enrich 過的 `CheckResult`（帶
`table`/`check`/`snap_date`/`expected`/`actual`），並把 `snap_date` 一路帶入（見 3）。

**2c. output checks 改 fail-fast**

`_run_output_checks()` 失敗時不再回傳 `-1`，改 raise：

```python
if failed:
    if audit:
        audit.write_record(AuditRecord(... status="failed" ...))
    raise OutputCheckError(self._stage, table.name, snap_date, failed)  # 訊息含 --restart-from 指引
```

`_process_single_table()` 末段不再判斷 `row_count >= 0`；改為直接回傳（成功路徑寫
success audit）。`run()` 的迴圈移除「`if not success: snap_status="failed"; break`」，
讓 `OutputCheckError`（`SourceETLError` 子類）往上拋。`run()` 既有
`except SourceETLError:` 已會設 `snap_status="failed"` 並 `raise`，行為自然變成
**中止整個 run**。後續 snap_date 不再執行。

**2d. dry-run 與 source-check 的關係**

`_initialize_context()` 在 `self._dry_run` 時回傳 `(None, None)`。`--source-check`
模式必須查 Hive，故 preflight 路徑在 CLI 層即以一個「強制非 dry-run」的 runner 執行：
`_run_etl(source_check_only=True)` 建 `SQLRunner(..., dry_run=False)`（無視 config
的 dry_run），確保 `run_source_checks()` 內 `_initialize_context()` 真的起 Spark。

**2e. 建構子加 `stage`**

`SQLRunner.__init__` 新增 `stage: str` 參數並存為 `self._stage`（如
`"feature_etl"`），供報告印出正確的重跑指令。`_run_etl` 的兩條路徑都把它已持有的
`stage` 傳入：`SQLRunner(config=..., sql_dir=..., dry_run=..., rendered_sql_dir=...,
stage=stage)`。

### 3. enrich `CheckResult`（`checks.py`）

```python
@dataclass
class CheckResult:
    passed: bool
    message: str
    metric_value: float | int | None = None
    # 新增（皆有預設，向後相容）
    table: str = ""
    check: str = ""           # "partition_exists" | "row_count" | "schema_drift"
    snap_date: str = ""
    expected: str = ""        # 人讀字串，如 ">= 1000000" / "snap_date=2025-01-31"
    actual: str = ""          # 人讀字串，如 "523" / "not found"
```

`SourceChecker` 三個 check 方法填上 `table`/`check`/`expected`/`actual`；
`SourceChecker.run_all(checks, snap_date)` 把 `snap_date` 寫進每筆結果。
`OutputChecker` 的結果也順手填 `table`/`check`/`snap_date`（供 `OutputCheckError`
報告），但**不改其檢查邏輯**。既有只看 `passed`/`message`/`metric_value` 的呼叫端不受影響。

### 4. 例外型別與報告（`sql_runner.py`）

```python
class SourceETLError(Exception): ...            # 既有

class SourceCheckError(SourceETLError):
    """Preflight source_checks 失敗：攜帶全部結果，str() 即完整報告。"""
    def __init__(self, results: list[CheckResult], stage: str):
        self.results = results
        self.stage = stage                      # 印重跑指令用，如 "feature_etl"
        super().__init__(self._format(results, stage))

    @staticmethod
    def _format(results, stage) -> str: ...     # 見下方報告格式

class OutputCheckError(SourceETLError):
    """單一輸出表的 quality_checks 失敗（fail-fast）。"""
    def __init__(self, stage: str, table: str, snap_date: str, failed: list[CheckResult]):
        ...                                     # 訊息含 `<stage> --restart-from <table>` 指引
```

**`SourceCheckError` 報告格式（console）**

```
Source check FAILED: 2 of 5 checks failed

  [FAIL] feature_store.feat_aum / partition_exists @ 2025-01-31
         expected partition snap_date=2025-01-31, got: not found
         → 上游分區尚未產出。確認上游已寫入該日：SHOW PARTITIONS feature_store.feat_aum
  [FAIL] feature_store.feat_aum / row_count @ 2025-02-28
         expected >= 1000000, got 523
         → 上游資料量不足／該日載入不完整。確認上游 ETL 已完成。

修復上游後重跑（僅失敗日期）：
  python -m recsys_tfb feature_etl --source-check --target-dates 2025-01-31,2025-02-28
```

依檢查類型的修復提示（固定對照，不臆測）：

| check | 提示 |
|---|---|
| `partition_exists` | 上游分區尚未產出。確認上游已寫入該日：`SHOW PARTITIONS <table>` |
| `row_count` | 上游資料量不足／該日載入不完整。確認上游 ETL 已完成。 |
| `schema_drift` | 上游 schema 與 `expected_columns` 不符。對齊上游欄位或更新設定。 |

「僅失敗日期」＝ 從 `failed` 結果取 distinct `snap_date` 排序後 join。stage 名由
runner 持有（新增 `self._stage` 建構參數，供報告印正確指令）。

**`OutputCheckError` 報告格式（console）**

```
Output quality check FAILED: feature_table @ 2025-01-31
  [FAIL] feature_table / max_duplicate_key_ratio @ 2025-01-31
         expected <= 0.0, got 0.0123
ETL 已中止。修復後可從該表續跑（跳過先前已寫的表）：
  python -m recsys_tfb feature_etl --target-dates 2025-01-31 --restart-from feature_table
```

### 5. 文件

- **`docs/pipelines/source_etl.md`**
  - §關鍵設定：在 `source_checks` 一行下補一張**設定參考表**與範例 YAML：

    | 欄位 | 型別 | 預設 | 意義 |
    |---|---|---|---|
    | `<table_name>`（key） | str | — | 上游表 FQN，如 `feature_store.feat_aum` |
    | `partition_key` | str | （必填） | 分區欄名，檢查 `<partition_key>=<snap_date>` 是否存在 |
    | `min_row_count` | int | `0` | 該分區最少列數；`0` 表示不檢查列數 |
    | `expected_columns` | map{col: type} | `{}` | 期望欄位→型別；缺欄或型別不符即失敗；空表示不檢查 |
    | `allow_new_columns` | bool | `true` | `false` 時，上游多出 `expected_columns` 以外的欄位即失敗 |

    範例：
    ```yaml
    feature_etl:
      source_checks:
        feature_store.feat_aum:
          partition_key: snap_date
          min_row_count: 1000000
          expected_columns:
            cust_id: string
            aum_bal: decimal(18,2)
          allow_new_columns: true
    ```
  - §重跑語意：改寫三種失敗後果表（source_check → `--source-check` preflight
    gate、collect-all、exit 1；output quality_checks → fail-fast 中止整個 run、
    exit 1；SQL/Spark → 中止）。
  - §指令：補 `--source-check` 用法與「先 preflight 再正式跑」建議工作流：
    ```bash
    python -m recsys_tfb feature_etl --source-check --target-dates 2025-01-31  # 先驗上游
    python -m recsys_tfb feature_etl              --target-dates 2025-01-31  # 通過再寫表
    ```
- **`README.md`**：source_etl 段落補一句 `--source-check` preflight 與兩步驟工作流。
- **`docs/change-guide.md`**：若其 source_etl／check 條目觸及失敗行為，對齊新語意。

### 6. 受影響檔案總覽

| 檔案 | 動作 |
|---|---|
| `src/recsys_tfb/__main__.py` | 三 ETL 指令加 `--source-check`；`_run_etl` 加 `source_check_only` 分支與 `SourceCheckError` 攔截 |
| `src/recsys_tfb/pipelines/source_etl/sql_runner.py` | 移除 run() gate；加 `run_source_checks()`；output check 改 raise；加 `SourceCheckError`/`OutputCheckError`；建構子加 `stage` 名 |
| `src/recsys_tfb/pipelines/source_etl/checks.py` | enrich `CheckResult`；三個 source check 與 output check 填新欄位；`run_all` 帶 `snap_date` |
| `docs/pipelines/source_etl.md` | source_checks 設定參考表、重跑語意、指令工作流 |
| `README.md` | source_etl 段落補 `--source-check` |
| `docs/change-guide.md` | 對齊（如有觸及） |
| `tests/test_pipelines/test_source_etl/test_sql_runner.py` | 新增 source-check／fail-fast 測試；調整既有 |
| `tests/test_pipelines/test_source_etl/test_checks.py` | enrich 後欄位斷言 |
| `tests/test_cli/`（或既有 CLI 測試位置） | `--source-check` typer 整合測試 |

### 7. 測試計畫

- **`run_source_checks` collect-all**
  - 多日 × 多 check，部分失敗 → raise `SourceCheckError`，且 `exc.results` 含**全
    部**結果、`failed` 含全部失敗（不在第一個失敗就停）。
  - 全通過 → 不 raise。
  - 無 `source_checks` 設定 → 不 raise、log warning。
- **output check fail-fast**
  - 某表 quality_check 失敗 → raise `OutputCheckError`；後續表／後續 snap_date **不
    執行**（以 spy／mock 驗證 `spark.sql` 呼叫次數或表處理數）。
- **enrich `CheckResult`**
  - 三個 source check 與 output check 回傳的 `table`/`check`/`snap_date`/`expected`/
    `actual` 正確；既有 `message`/`metric_value` 斷言仍通過。
- **報告格式**
  - `str(SourceCheckError(...))` 含每筆 `[FAIL] table / check @ date`、expected/actual、
    對應修復提示、重跑指令（含正確 stage 名與僅失敗日期）。
  - `str(OutputCheckError(...))` 含 `--restart-from <table>` 指引。
- **CLI（typer `CliRunner`）**
  - `feature_etl --source-check`：檢查失敗 → `exit_code == 1`、**不寫表**（mock
    SQLRunner.run 未被呼叫）；通過 → `exit_code == 0`。
  - `--source-check` ＋ `--restart-from` → `exit_code == 1`（報錯）。
- **既有測試對齊**：`test_sql_runner` 中假設「source_check 內嵌於 run()」「output
  check continue-next-date」的測試改寫為新語意；`test_source_checks_parsed` 維持。

### 8. 邊界與相容性

- inference 尚未在公司環境部署、source_etl 改動無 backward-compat 包袱（見專案記
  憶），可乾淨切換語意。
- 本機 dev-cluster 用合成資料時跳過 source_etl，故 `--source-check` 主要用於生產；
  但旗標在任一 `--env` 都可運作（只要 Hive 來源表存在）。
- `parameters_*_etl.yaml` 目前 `source_checks: {}`（空）：`--source-check` 對空設定
  log warning 後 `exit 0`，不誤判失敗。

## 待辦（轉 writing-plans 後展開為 TDD 步驟）

1. enrich `CheckResult` ＋ checks 填欄位（先測後做）。
2. `SourceCheckError` / `OutputCheckError` ＋ 報告格式。
3. `SQLRunner.run_source_checks()` ＋ 移除 run() gate ＋ stage 名建構參數。
4. output check 改 fail-fast。
5. CLI `--source-check` 旗標 ＋ `_run_etl` 分支 ＋ 例外攔截。
6. 文件（source_etl.md / README / change-guide）。
7. 全套測試（含既有對齊）。
