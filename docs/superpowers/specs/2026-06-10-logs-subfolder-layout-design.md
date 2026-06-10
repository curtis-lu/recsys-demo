# logs/ 分子資料夾(pipeline / 月份 兩層)— 設計

日期:2026-06-10
分支:`feat/logs-subfolder-layout`

## 問題

`setup_logging()` 目前把每次 pipeline 執行的 log 以扁平方式寫在 `logs/` 根目錄:
`logs/{pipeline}_{run_id}.jsonl`(`run_id = YYYYMMDD_HHMMSS_{6hex}`)。隨執行次數累積,單一目錄檔案數無限成長、越來越難找。

## 目標

把 log 依 **pipeline / 月份** 兩層分資料夾,讓任一目錄的檔案量長期有界,且仍能「只看某 pipeline 的所有跑次」。同時提供一次性 migration 把現有扁平舊檔歸位。

最終結構:

```
logs/
  dataset/
    2026-06/
      dataset_20260610_185220_abc123.jsonl
      dataset_20260611_090112_def456.jsonl
    2026-05/
      ...
  training/
    2026-06/
      training_20260610_...jsonl
  inference/
  evaluation/
  source_etl/
```

## 非目標(YAGNI)

- 不加 log retention / 自動清舊。
- 不改 config schema(`logging.file.path` 維持 `logs/` 當根目錄)。
- 不動下游讀 log 的工具(目前沒有元件以固定路徑讀單一 log 檔)。
- 不改 JSON 內容格式、不改 console handler。

## 設計

### 1. 核心改動 — `setup_logging()`(`src/recsys_tfb/core/logging.py`)

現況(約 line 146–155):

```python
log_dir = Path(file_path)                       # logs/
log_dir.mkdir(parents=True, exist_ok=True)
filename = f"{context.pipeline}_{context.run_id}.jsonl"
file_handler = logging.FileHandler(log_dir / filename, encoding="utf-8")
```

改為:

```python
month = _month_from_run_id(context.run_id)      # "2026-06"
pipeline_dir = context.pipeline or "_unknown"
log_dir = Path(file_path) / pipeline_dir / month
log_dir.mkdir(parents=True, exist_ok=True)
filename = f"{context.pipeline}_{context.run_id}.jsonl"   # 不變
file_handler = logging.FileHandler(log_dir / filename, encoding="utf-8")
```

新增模組私有 helper:

```python
import re

_RUN_ID_DATE_RE = re.compile(r"^(\d{4})(\d{2})\d{2}_")

def _month_from_run_id(run_id: str) -> str:
    """Return 'YYYY-MM' from a run_id starting with YYYYMMDD_, else current month."""
    m = _RUN_ID_DATE_RE.match(run_id or "")
    if m:
        return f"{m.group(1)}-{m.group(2)}"
    return datetime.now(timezone.utc).strftime("%Y-%m")
```

要點:
- 月份優先從 `run_id` 前 8 碼推導,使資料夾月份與檔名時間戳一致;run_id 非標準格式(自訂)時 fallback 當下月份。
- `context.pipeline` 在真實 CLI 路徑(`__main__.py:85` `RunContext(pipeline=pipeline, env=env)`)永遠非空;`or "_unknown"` 純防呆(預設 `RunContext()` 的 `pipeline=""`)。
- **檔名格式完全不變**,所以月份資料夾內仍可 `dataset_*` glob;唯一改變是落地路徑多兩層。
- `file_enabled=False` 路徑、console handler、`JsonFormatter`/`ConsoleFormatter` 皆不動。

### 2. 一次性 migration script — `scripts/migrate_logs_layout.py`(Typer CLI)

依專案慣例(dev/CLI 工具放 `scripts/` 單檔 + `tests/scripts/`,以 `from scripts.X import ...` 測):

- 只掃 `logs/` **頂層** `*.jsonl`(`Path(logs_dir).glob("*.jsonl")`,不遞迴),因此已分好的子資料夾不受影響 → 可重跑(idempotent)。
- 以 regex 解析舊檔名:`^(?P<pipeline>.+)_(?P<date>\d{8})_\d{6}_[0-9a-f]{6}\.jsonl$`。
  `pipeline` 用貪婪 `.+`,讓 `source_etl` 這類含底線的 pipeline 名也能正確切出(`_\d{8}_\d{6}_[0-9a-f]{6}` 錨在尾端)。
- month = `date[:4]-date[4:6]`;目標 = `logs/<pipeline>/<YYYY-MM>/<原檔名>`。
- **預設 dry-run**:列印每個檔的搬移計畫與彙總(共 N 檔、M 不符規則),不動檔案。加 `--apply` 才真的 `Path.rename` 搬移。
- 不符 regex 的檔案:保留原地、列入「skipped(unmatched)」清單。
- 目標已存在同名檔(理論上不會,run_id 唯一):跳過 + 警告,不覆蓋。
- 選項:`--logs-dir`(預設 `logs/`)、`--apply/--dry-run`(預設 dry-run)。
- 邏輯拆成可測純函式:例如 `plan_moves(logs_dir) -> (moves: list[(src, dst)], skipped: list[(path, reason)])`,CLI 層只負責印出與(在 `--apply` 時)執行搬移。

### 3. 測試

**更新** `tests/test_core/test_logging.py::test_creates_handlers`(現斷言 `tmp_path/logs/dataset_20260322_120000_aabbcc.jsonl`):
- 改斷言新路徑 `tmp_path/logs/dataset/2026-03/dataset_20260322_120000_aabbcc.jsonl`。
- 視需要補一個 case:run_id 非標準格式時 fallback 到當月資料夾。

**新增** `tests/scripts/test_migrate_logs_layout.py`(以 `tmp_path` 建假 log 檔,純檔案系統、無 Spark):
- 正常搬移:`dataset_20260322_120000_aabbcc.jsonl` → `dataset/2026-03/`。
- 含底線 pipeline:`source_etl_20260401_010101_ffeedd.jsonl` → `source_etl/2026-04/`。
- 不符規則檔(如 `random.log`、`notes.txt`、`foo.jsonl`)保留原地、列入 skipped。
- dry-run 預設不動任何檔(`plan_moves` 回傳計畫,但檔案系統不變)。
- `--apply`(或直接呼叫搬移函式)後檔案到位、來源消失。
- idempotent:對已分好結構的目錄重跑,頂層無 `*.jsonl`,計畫為空、無變動。

## 影響面

- 改動檔:`src/recsys_tfb/core/logging.py`(新增 helper + 改路徑組裝)、新增 `scripts/migrate_logs_layout.py`。
- 測試:更新 `tests/test_core/test_logging.py`、新增 `tests/scripts/test_migrate_logs_layout.py`。
- config / 下游讀取:無變動。
- 行為相容性:新跑次落點改變;舊跑次靠 migration script 歸位。無對外契約破壞(inference 尚未部署、無人以固定 log 路徑讀取)。
