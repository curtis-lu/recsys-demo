# CLAUDE.md

Claude Code 在此 repo 的最小規範。原則：本檔只放「每個 session 都需要的不變量、指令塊、路由」；細節一律在指向的檔案裡，**不在此複述**（發現重複或矛盾時的處理，載入 `maintain-agent-rules` skill）。

## 這個專案是什麼

通用的**排序（learning-to-rank）批次建模框架**：對每個 query group（`time` × `entity`）把候選 `item` 依模型分數排名。欄位角色由 `conf/base/parameters.yaml` 的 `schema` 區塊配置；來源表（`feature_table`/`sample_pool`/`label_table`）由使用者自定義。**商業銀行產品推薦只是示例 instantiation，不是框架限定應用**——寫文件時保持抽象框架定位。

框架是 **Kedro 風格的手刻實作**（`src/recsys_tfb/core/` 自製 DataCatalog/Node/Pipeline/Runner，`src/recsys_tfb/io/*` 仿 kedro.io；**無 kedro 套件依賴**，Ploomber 僅作排程）。規模與細節見 README.md 與 docs/。

## 不變量（違反即錯，無例外）

- **生產限制：No UDFs in Spark、no network、no additional packages**。CPU-only。
- Tech stack 釘版本：Python 3.10.9（系統 python3=3.12 **不可用**）、PySpark 3.3.2、LightGBM 4.6.0、pandas 1.5.3、MLflow 3.1.0、Optuna 4.5.0（完整清單見 pyproject.toml）。
- CLI 格式：`python -m recsys_tfb <pipeline> [--options]`——**無 `run` 子指令、無 `--pipeline` flag**。
- `.venv` 永不進版控（歷史事故見 docs/operations/known-pitfalls.md §1）。
- model promote 需使用者人工觸發，Claude 不得自行操作。
- 功能開發從規劃到實作全程在獨立 `.worktrees/<name>` + `feat/` branch 進行。

## 路由表（做 X 之前先讀 Y）

| 任務 | 先讀 |
|---|---|
| 架構 / 重構 / 探索 codebase | `graphify-out/GRAPH_REPORT.md`（**強制**，不得用 Explore agent 替代這一步；有 `graphify-out/wiki/index.md` 就導航 wiki 而非讀原始檔） |
| 動 `core/` 框架抽象；新增或修改 pipeline node／catalog 條目 | `docs/agents/architecture-constraints.md`（框架事實＋10 條可機械檢查的約束＋例外登記。例外登記要加一筆必須先問使用者；稽核測試＝`tests/test_core/test_architecture_constraints.py`） |
| 寫或改一個 node 的**內容**（邊界畫哪、body 長什麼樣、機制放哪個檔、命名） | `docs/agents/pipeline-node-design.md`（跨 pipeline 的判準，13 條裡**只有 2 條有部分機械檢查**——上一列那份擋得住位置，這份擋的是內容。界外違例登記在其〈已登記的例外〉，要加一筆先問使用者） |
| 重整一整條 pipeline（不是改單一 node）：排順序、切 PR、選驗證手段、收尾清掃 | `docs/agents/pipeline-refactor-process.md`（**流程**判準，上一列那份是形狀判準。dataset／inference 兩輪 18 個 PR 歸納，每條附得出證據的 PR 或 commit） |
| worktree / venv 任何操作 | `docs/operations/dev-setup/worktree-venv-setup.md` ＋ 下方 pre-flight |
| 本機跑 Spark pipeline | `docs/operations/dev-setup/local-spark-setup.md`（或 local-spark skill）＋ 下方指令塊 |
| pipeline 部分執行（模式 vs 切片、加月份的 auto-included、接續前提） | `docs/operations/user-guides/pipeline-slicing.md`；旗標本身的用法在該 pipeline 文件 §4 |
| 踩到怪錯誤（ELOOP / 改了沒生效 / 組合跑才 fail） | `docs/operations/known-pitfalls.md` |
| 覺得某處「該修但沒修」、或準備順手做掉一個延後項 | `docs/agents/deliberate-non-goals.md`（刻意不做的事＋理由。**票號與進度不在裡面**，查 `gh`） |
| HPO 中斷接續 | `docs/pipelines/training.md` §4.7（操作）＋ §7.3（恢復語意與 `_hpo/` 清理） |
| 想多評估一個月份（加 `test_snap_dates`） | `docs/operations/user-guides/adding-an-eval-month.md`（不翻版本、不重訓；四步驟＋驗收） |
| 抽樣權重設定 | `docs/operations/user-guides/sampling-overrides-editor.md`（部分 config 靠 `scripts/sampling_overrides_editor.py`、`scripts/suggest_categorical_cols.py` 推導，非手填） |
| 做／改診斷報表的呈現層（表格、圖、定義、編排） | `docs/agents/diagnosis-report-presentation.md`（每段資訊扣回目的、地基量開頭講一次、選最能凸顯重點的媒介、判斷留給讀者） |
| 派 subagent / 選模型 / 驗收 | 判準已常駐於 `~/.claude/rules/00-core.md`；寫派工 prompt 前載入 `dispatch-subagents` skill |

## Worktree 鐵則（細節與事故記錄：known-pitfalls.md §3）

1. 唯一真實 venv＝`/Users/curtislu/projects/recsys_tfb/.venv`；各 worktree 的 `.venv` 是指向它的 symlink，不建獨立 venv。
2. **(R1)** Edit/Write worktree 檔案的絕對路徑必含 `.worktrees/<name>`——改錯邊的徵兆是「輸出跟 baseline 完全相同」。
3. **(R2)** `cd` 在 Bash 呼叫之間會持續：每個指令以 `cd <worktree-root> && ...` 開頭，或全用絕對路徑。
4. **(R3)** 每個 worktree 用自己的真 `data/` 樹，不 symlink 到 main。
5. 跑測試/CLI 一律：`PYTHONPATH=<wt>/src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest|recsys_tfb ...`（裸跑會抓到 main 的 src，靜默測錯 code）。
6. 跨 worktree git 一律 `git -C <abs-worktree>`。

**每次在 worktree 動 python 前先跑 pre-flight**（Spark cold start 2–4 分鐘，失敗才發現的成本很高；任一失敗先修再繼續）：

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/<name> && pwd          # 在 worktree root
readlink .venv && /Users/curtislu/projects/recsys_tfb/.venv/bin/python -V   # venv 對齊（Python 3.10.9）
export SPARK_CONF_DIR=$PWD/conf/spark-local                              # --check-isolation 讀此變數，未設會 FAIL: SPARK_CONF_DIR=''
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python scripts/local_spark_setup.py --check-isolation  # data/ 隔離閘
grep -n "你這次改的鍵:" conf/base/parameters_*.yaml                      # 換成實際改的鍵名，確認印出的是 worktree 的新值
# （注意：鍵多為巢狀縮排，grep 不要加 ^ 行首錨定——舊版此行用 ^(objective|metric|snap_date) 永遠零命中，等於沒檢查）
```

## 本機 Spark（無 Docker：local[*] + 內嵌 Derby + 本機 warehouse；完整步驟見 local-spark-setup.md）

```bash
cd <repo-or-worktree-root>
export SPARK_CONF_DIR=$PWD/conf/spark-local
PYTHONPATH=src .venv/bin/python scripts/local_spark_setup.py            # 首次 / --reset
PYTHONPATH=src .venv/bin/python -m recsys_tfb <pipeline> --env local    # 所有 pipeline 同一條路
```

- 端到端 smoke：`bash scripts/local_e2e.sh`；互動查表：`bash scripts/local_spark_shell.sh`（pyspark）／`… sql`。
- `--env local` 是唯一本機環境識別符。

## 測試（跑快，不是少跑）

- 單次改動只跑相關測試檔——理由是聚焦，**不是耗時**：全量 `tests/test_evaluation`（302 個測試）實測 **54 秒**（2026-08-08）。
- **寫耗時要連量測環境一起寫。** 本檔曾長期寫「~33 分鐘」——那是 Docker 時代量的，2026-06-09 換成 `local[*]` 之後就失效了，但沒人回頭重量，還被 10 份以上的 plan／spec 轉引當作跳過測試的理由。原委與規則：known-pitfalls.md §4。
- 可能 >2 分鐘的指令一律 background 執行（曾因 foreground 重跑全量空轉整晚）。
- 驗證優先 `git diff <base>..<head>`（SHA-based、秒級）＋針對性 grep；不重跑 subagent 已驗過的。
- **main 上有既知 failing/互擾測試，改動前先建 baseline**——清單見 known-pitfalls.md §5。
- 加速手段 backlog 與量測原則：known-pitfalls.md §4。

## Config consistency gate

不變量（Layer-1 的 A 系列 ＋ 資料閘的 B 系列）的**唯一真實來源＝`src/recsys_tfb/core/consistency.py`**，各代號意義見該檔模組 docstring 的 Invariant legend。**新增一致性不變量必須在該模組加 predicate，不得在各 pipeline ad-hoc 散落**。Layer-1 `validate_config_consistency` 在 CLI entry 執行、collect-all 一次 raise；Layer-2 `validate_data_consistency`（`pipelines/dataset/nodes.py`）是 dataset pipeline 第一個 side-effect 節點。改動這一帶之前先讀該模組 docstring，不要依賴本段的摘要。

## graphify

- 架構/重構/探索任務：先讀 `graphify-out/GRAPH_REPORT.md`（見路由表，強制）。
- 本 session 改過 code 檔案後，跑 `.venv/bin/python -c "from graphify.watch import _rebuild_code; from pathlib import Path; _rebuild_code(Path('.'))"` 讓圖保持最新（graphify 裝在 .venv 的 dev extras；裸 `python3` 在非 pyenv shell 會是 3.12 且無此套件）。
- 歷史上 hook 曾擋 checkout，**已修**（61ee9ac）；殘留通用規則見 known-pitfalls.md §2。

## Agent skills

供 `mattpocock-skills` 系列（`/to-tickets`／`/triage`／`/implement` 等）讀取的 per-repo 設定；細節在指向的檔案，本段只路由。

**產出路徑（硬規則）**：本系列 skill 的產物只寫進 `docs/adr/`、`docs/agents/`，路徑未指定的長文產出（如 `/research`）寫進 `docs/notes/`。**`docs/superpowers/` 是 superpowers skill 寫死的專屬存檔區（`plans/`／`specs/`），本系列一律不得寫入**——兩套 skill 的命名慣例相反（前者用工具名 namespace、後者用產物類型），混寫會讓目錄名再也對不上內容。

### Issue tracker

GitHub Issues（`curtis-lu/recsys-demo`），用 `gh` CLI 操作。見 `docs/agents/issue-tracker.md`。

### Triage labels

五個角色沿用預設字串（`wontfix` 已存在，其餘四個首次 `/triage` 時建立）。見 `docs/agents/triage-labels.md`。

### Domain docs

Single-context，**刻意不設 `CONTEXT.md`**——詞彙定義一律在程式碼旁的 docstring（`core/versioning.py`＝版本 ID、`core/consistency.py`＝不變量代號），`docs/` 只做白話解釋。見 `docs/agents/domain.md`。
