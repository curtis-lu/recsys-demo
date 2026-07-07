# Known pitfalls（已踩過的坑與既有問題）

> 從 CLAUDE.md 抽出的完整細節（2026-07-04）。CLAUDE.md 只留鐵則，事故的來龍去脈在這裡。
> 新增條目的格式與時機見 `~/.claude/rules/40-maintenance-protocol.md`。

## 1. `.venv` self-symlink ELOOP（已修，規則仍有效）

`.venv` 曾被誤 `git add` 進版控（`0cf79db`），其 symlink 目標指向自己 → 之後任何 checkout / `git worktree add` 都重建這個迴圈，全 `python`/`pytest` 報 `too many levels of symbolic links`。已修（`4e5af3c`：`git rm --cached .venv`、`.gitignore` 同時擋 `.venv` 與 `.venv/`、釘 `.python-version=3.10.9`）。

**規則：`.venv` 永不進版控**。`git ls-files | grep -x .venv` 一旦出現被追蹤就停下、`git rm --cached .venv`、commit 並**進 main**（否則各分支/worktree 一直繼承）。已追蹤的檔案無視 `.gitignore`。

## 2. graphify hook 擋 git checkout/merge（**已修 2026-06，殘留規則仍有效**）

歷史問題：graphify 的 post-checkout/post-commit hook 會把當時 **tracked** 的 `graphify-out/GRAPH_REPORT.md` 改髒，使隨後的 `git checkout` / `git merge --ff-only` 被「local changes would be overwritten」擋住；若指令用 `&&`+`set -e`+`>/dev/null` 串接，因 `set -e` 的 AND-list 例外會**靜默失敗、HEAD 沒動**而你以為成功了。

**已修**：`61ee9ac` untrack 了 GRAPH_REPORT.md（2026-07-04 以 `git ls-files graphify-out/` 為空驗證），hook 不再擋 checkout。

**仍有效的通用規則**：git 串接指令不要吞 stdout/exit code；切分支/合併後用 `git rev-parse HEAD` 確認真的動了。「指令看起來成功」不等於「狀態真的變了」。

## 3. Worktree 三件路徑踩坑 R1/R2/R3（2026-05-24 單日浪費 ≥3 次 Spark cold start）

- **(R1) 絕對路徑要含 `.worktrees/<name>`**：用 main repo 的絕對路徑 `Edit`/`Write` worktree 的 config —— 改錯邊，worktree 那份沒動、pipeline 讀的還是舊的。**徵兆：訓練出來的 `model_version` / best params 跟 baseline 完全相同。**
- **(R2) `cd` 在 Bash tool 呼叫之間會持續**（system prompt 明文；但 skill 執行後 cwd 可能 reset，是兩回事）。`cd <wt>/data && ln -s ...` 後沒回 worktree root，下一個 training 指令 `Path.cwd()/"data"` 看到雙重 `data/data/dataset` → FileNotFoundError after Spark started。**規則：Bash 指令以 `cd <worktree-root> && ...` 開頭、或全用絕對路徑。**
- **(R3) Worktree `data/` 隔離**：每個 worktree 用**自己的真 `data/` 樹**，**不 symlink 到 main**（`cache.root` 已相對化＝`data/recsys_cache`、warehouse/metastore 也相對）。首次進 worktree 跑 `PYTHONPATH=src .venv/bin/python scripts/local_spark_setup.py` 重建本機資料；隔離驗證用 `scripts/local_spark_setup.py --check-isolation`。

完整 SOP：`docs/operations/worktree-venv-setup.md`。

## 4. 測試效能：待評估的加速手段 backlog

整包 `tests/test_evaluation` 約 33 分鐘，主因是大量 Spark 測試逐一執行 Spark action（conftest `spark` fixture：`spark.master=local[1]`、`shuffle.partitions=1`；fixture 是 function-scoped，但 `get_or_create_spark_session` 會重用仍存活的 session，並非每測試都重啟）。**正確方向是把測試跑快，不是略過測試。** 採用任何手段前先實測（`pytest --durations=20` 找最慢的），勿臆測：

- conftest `spark` fixture 改 `local[*]` 或調並行度是否有感（小資料下未必，需量測）。
- 全程重用單一 SparkSession（注意 `tune_hyperparameters` 會 `.stop()`，見 conftest 註解需妥善處理）。
- pytest 程序級並行（xdist 類）的取捨：多 JVM 可能反而更慢。
- 減少測試中非必要的 count/collect，用更小固定資料。

## 5. main 上既有的測試問題（不是你造成的，勿浪費時間歸因給自己的改動）

- `TestPrepareTrainInputsWeight` 兩個測試在 main 本來就 failing（非快取 footgun、非 two-stage 造成），待獨立修。
- core+cli+io+pipelines **組合跑**時有 2 個 Spark 整合測試互相干擾 fail；**單獨跑皆過**。看到只在組合跑才出現的 fail，先單獨重跑確認。

改動前先在 main/基準點跑一次相關測試建立 baseline，才能區分「本來就壞」與「被我改壞」。

## 6. 環境 quirk 速記

- PySpark 3.3.2 `tableExists("db.t")` 兩段式寫法永遠回 False（實證於 PR#74），要用 `tableExists("t", "db")` 或先 `USE db`。
- local[*] 下 stderr 的 `RpcEndpointNotFoundException: CoarseGrainedScheduler` 是 by-design 噪音，不是錯誤。
- catalog deep-merge 對 type-discriminator 有 bug：workaround＝base 檔完整定義該 entry，不要依賴 env overlay 局部覆蓋 type。
- 【2026-07-07】取 model_version 不要用 `ls -t data/models`：目錄 mtime 不隨「內容檔被覆寫」更新（重訓寫回既有 mv 目錄時，該目錄不會浮到最上面），且 `data/models/` 混有測試殘留目錄（e2e_test_mv、mvx…）。徵兆＝抓到的 mv 與 config 語意矛盾（還原 config 後「新 mv」竟等於注入版）。正解：從 training log 的 `Wrote manifest: .../data/models/<mv>/manifest.json` 行取，或 `python -c "import json; print(json.load(open('data/models/<候選>/manifest.json'))['model_version'])"` 核對。驗證方式：取到 mv 後 grep training log 確認同一 run 寫的就是它。
