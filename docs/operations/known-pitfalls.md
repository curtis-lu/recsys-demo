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
- 【2026-07-08】`test_pipelines/test_inference/test_pipeline.py::TestInferencePipeline::test_pipeline_inputs` 在 main 本來就 failing（單獨跑也紅、確定性）：PR#85 給 inference pipeline 加了 `inference_population` input，該 exact-set 斷言未同步。待獨立修（一行 additive）。

改動前先在 main/基準點跑一次相關測試建立 baseline，才能區分「本來就壞」與「被我改壞」。

## 5b. 弄壞驗證（break-it check）在未提交檔案上的還原坑（2026-07-08）

- **症狀**：對「尚未 commit 的改動」做弄壞驗證後，用 `git checkout -- <file>` 還原——把整份未提交改動連同弄壞的那行一起洗掉；若接著用 `&&` 串 commit，pytest 經 pipe（`| tail`）exit code 被吃掉，紅燈照樣 commit 出一個壞 commit。
- **根因**：`git checkout --` 的還原目標是 HEAD，不是「弄壞前的工作樹狀態」；pipe 尾端指令的 exit code 掩蓋 pytest 失敗。
- **規則**：弄壞驗證一律「Edit 弄壞 → 跑測試 → Edit 改回原字串」，**禁止 `git checkout -- <file>`**（除非該檔在 HEAD 已是想要的狀態）；弄壞驗證與 commit 不串在同一條指令，commit 前獨立跑一次測試看到綠燈原文。
- **驗證方式**：還原後 `git diff --stat <file>` 應顯示**預期中的未提交改動仍在**（而不是空）；空 diff＝洗掉了。

## 6. 環境 quirk 速記

- PySpark 3.3.2 `tableExists("db.t")` 兩段式寫法永遠回 False（實證於 PR#74），要用 `tableExists("t", "db")` 或先 `USE db`。
- local[*] 下 stderr 的 `RpcEndpointNotFoundException: CoarseGrainedScheduler` 是 by-design 噪音，不是錯誤。
- catalog deep-merge 對 type-discriminator 有 bug：workaround＝base 檔完整定義該 entry，不要依賴 env overlay 局部覆蓋 type。
- 【2026-07-08】本機跑 evaluation 必帶 `--post-training`：default 模式讀 inference 產物 `ml_recsys.ranked_predictions`，本機（只跑過 training）沒有這張表。徵兆＝第一個 node `prepare_eval_data` 秒炸 `Table or view not found: ml_recsys.ranked_predictions`——這不是 Spark/catalog 壞掉，是模式選錯。正解：`python -m recsys_tfb evaluation --env local --model-version <mv> --post-training`（讀 `training_eval_predictions`）。
- 【2026-07-07】取 model_version 不要用 `ls -t data/models`：目錄 mtime 不隨「內容檔被覆寫」更新（重訓寫回既有 mv 目錄時，該目錄不會浮到最上面），且 `data/models/` 混有測試殘留目錄（e2e_test_mv、mvx…）。徵兆＝抓到的 mv 與 config 語意矛盾（還原 config 後「新 mv」竟等於注入版）。正解：從 training log 的 `Wrote manifest: .../data/models/<mv>/manifest.json` 行取，或 `python -c "import json; print(json.load(open('data/models/<候選>/manifest.json'))['model_version'])"` 核對。驗證方式：取到 mv 後 grep training log 確認同一 run 寫的就是它。

## 7. macOS 換網路後 Spark 起不來：hostname 解析到過期 IP（2026-07-07）

- **症狀（第一分鐘認出它）**：所有需要 Spark 的測試在 fixture setup 階段**秒炸**（整批 ERROR 而非 FAIL，總時長短到不可能有 JVM 起來過，例：226 passed + 39 errors in 12.9s）；stack 尾是 netty 的 `sun.nio.ch.Net.bind0 ... AbstractBootstrap` bind 失敗。CLI pipeline 同樣在啟動即炸。
- **根因**：macOS 的 hostname（`Mac`）在換 Wi-Fi/VPN 後仍解析到舊 DHCP 位址（實例：解析到 192.168.50.12，實際介面是 192.168.50.218）。Spark driver 預設綁 hostname 解析出的 IP → 綁不上。**錯誤看起來在測試/pipeline（下游），原因在網路環境（上游）**——R 系列鐵則同款形態。
- **規則**：本機一律 loopback——已固定在兩處：`tests/conftest.py` 的 `os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")`、`conf/spark-local/spark-env.sh` 的 `export SPARK_LOCAL_IP=127.0.0.1`（spark-submit 自動 source）。不要改設 `spark.driver.host`（spark-defaults.conf 註記過的 RpcEndpointNotFoundException 陷阱）。
- **驗證方式**：`python3 -c "import socket; h=socket.gethostname(); print(socket.gethostbyname(h))"` 對照 `ifconfig | grep "inet "`——兩邊不含同一 IP 即中招；修復後單跑任一 Spark 測試應 pass（本次實證 1 passed in 3.81s）。

## 8. 字串特徵欄靜默 → object 矩陣 OOM（已加 B6 閘，2026-07-11）

- **症狀（第一分鐘認出它）**：training `prepare_lgb_train_inputs` 在 `_pdf_to_X` 的 `to_numpy` 步被 `Killed`（OOM）；或（B6 上線後）在讀 parquet 前秒級 `DataConsistencyError: ... un-encoded non-numeric type(s)`。本機合成資料永不重現（合成 feature_table 無此欄）。
- **根因**：生產 `feature_table` 有字串欄，未宣告 `categorical_columns`、也未 `drop_columns` → `_compute_feature_columns` 收它為特徵 → `_encode_categoricals` 不編它 → `X_df.values` 塌縮成 object 矩陣（每格 ~34 B vs float64 8 B，公司規模 22→96 GiB）。錯誤在 training（下游），根因在 dataset schema 設定（上游）——R 系列同款形態。
- **規則**：字串特徵欄必須 declare categorical 或 drop。此不變量的唯一真實來源＝`core/consistency.py::nonnumeric_feature_errors`（B6，含 `spark_dtype_is_numeric` 分類器），掛在兩處：dataset 閘 `validate_data_consistency`（防復發）＋ `io/extract.py` 讀取 backstop（救舊 cache）。改 config 會 bump `base_dataset_version`、需重建 dataset。詳見 `docs/operations/training-oom-object-matrix.md`。
- **驗證方式**：`python -c "import pyarrow.parquet as pq, pyarrow as pa; s=pq.read_schema('<train_model_input.parquet>'); print([f.name for f in s if pa.types.is_string(f.type)])"` 對照 `preprocessor.json` 的 `feature_columns`／`categorical_columns`；差集非空即中招。

## 9. 本機跑 evaluation 必須兩個旗標，少一個就跑不動（2026-07-19）

- **症狀（第一分鐘認出它）**：不帶 `--post-training` → 卡在讀不到 `ranked_predictions`（inference 產物）；不帶 `--model-version` → `FileNotFoundError: No 'best' symlink found in .../data/models`。
- **根因**：兩個獨立原因。(a) evaluation 預設模式讀 inference 產出的 `ranked_predictions`，而本機 inference 撞既有 issue #63（`scripts/local_e2e.sh:6-9` 明寫本機 e2e 只收斂到 training）；`--post-training` 改讀 training 自己產出的 `training_eval_predictions`（分歧點在 `pipelines/evaluation/pipeline.py` 的三元式）。(b) 不指定 model_version 會解析 `data/models/best` symlink，而那要 promote 才有——**promote 是使用者保留的人工步驟，Claude 不得自行執行**。
- **規則**：本機一律 `python -m recsys_tfb evaluation --env local --post-training --model-version <mv>`，`<mv>` 用 training 那步印出的值。**不要為了讓它跑起來而去 promote。**
- **驗證方式**：先跑 `dataset` → `training`（training log 尾端會印 model_version 與 manifest 路徑），再帶入。完整建置鏈見 `docs/superpowers/plans/diag-redesign/00-shared-context.md` 的環境前置段。

## 10. 用 grep report.html 當驗收會假陽性：內嵌 plotly.js 含大量常見英文字（2026-07-19）

- **症狀（第一分鐘認出它）**：明明原始碼已清乾淨，`grep -c "<某個字>" report.html` 仍有命中，看起來像刪除不完整。實例：清掉 quadrant 診斷後，report.html 仍 grep 到 2 個 `quadrant`。
- **根因**：`generate_html_report`（`evaluation/report.py:85,118`）把整份 plotly.js **內嵌**進 HTML（約 3.5MB）。plotly 內部有四叉樹實作 `En.prototype.quadrant`，以及大量其他常見識別字。**命中的是第三方 minified JS，不是本專案的字串。**
- **規則**：驗證「某個功能的字樣是否清乾淨」一律 **grep 產生 HTML 的原始碼**（`src/recsys_tfb/evaluation/`、`src/recsys_tfb/pipelines/evaluation/`），不要 grep 產物 HTML。
- **驗證方式**：命中時用 `grep -o ".\{100\}<字>.\{100\}" report.html` 看上下文——落在 minified JS 裡（無空白、大量單字母變數）即為假陽性。決定性檢查是 `grep -rn "<字>" src/recsys_tfb/evaluation/` 為零。

## 11. pandas `groupby` 預設 `dropna=True` 讓 NULL 群整組消失（診斷層尤其危險，2026-07-20）

- **症狀（第一分鐘認出它）**：某個彙總表／矩陣少了一整個群，而其他數字（總量、極值、逐列計算的結果）顯示那個群明明存在。實例：`config_shift` 的 `query_offset_spread.max = 4.605`，但 `offset_matrix` 裡沒有任何一個 group 的 spread 解釋得了這個值；`notes` 也是空的。
- **根因**：`DataFrame.groupby()` 的 `dropna` 預設是 `True`——**分組鍵含 NaN 的列被整批丟棄，不報錯、不警告**。而 Spark 端的整數欄只要有任一 NULL，`toPandas()` 之後必然是 `float64` 帶 `NaN`（同 §8 的 dtype 家族）。用 `drop_duplicates().sort_values()` 手動迭代則會保留 NaN，所以**把手動迭代重構成 groupby 是典型的引入點**——這正是本次的實際來源（重構前後測試全綠）。
- **為什麼在診斷層特別貴**：診斷的輸出常被讀成「我量過了，沒事」。一個群靜默消失會讓「沒量到」與「量到零」在報表上長得一模一樣，讀者據此排除掉真正的原因——比不提供這項診斷更糟。
- **規則**：**彙總診斷數字的 `groupby` 一律顯式寫 `dropna=False`**，NaN 群給明確標籤（如 `"<NULL>"`），並讓輸出帶一則「有 N 個 NULL 群」的觀測。要丟棄 NaN 也可以，但必須是寫出來的決定，不是撿到的預設。
- **驗證方式**：`grep -rn "groupby(" src/recsys_tfb/diagnosis/` 拿到**候選清單**（不是通過／失敗閘——實測 10 個命中只有 1 個相關，當閘用會被當噪音略過）。逐一問一個問題：**這個分組鍵可能是 NaN 嗎？** 來自使用者資料的欄（context 欄、item、entity）可能；自己造的（`factorize` 碼、自己組的字串標籤）不可能。只有前者需要 `dropna=False`。
  針對性測試：造一份分組鍵半數為 `float64` NaN 的 fixture，斷言該群出現在輸出裡；mutation 靶＝拿掉 `dropna=False`，測試必須轉紅。
  **已知待查（2026-07-20 掃到、未處理）**：`diagnosis/metric/sample.py:281` 的 `groupby(item_col)` 分組鍵來自使用者資料，item 為 NULL 時該筆會從 `per_item_sampled` 計數靜默消失。屬 Plan 0 已 merge 的程式碼，尚未評估 item 欄是否有上游 not-null 保證。

## 12. Node inputs 與函式簽章是**位置**綁定：少一個輸入就整排平移（2026-07-20 公司環境實例；`generate_report` 已於同日修復）

- **症狀（第一分鐘認出它）**：某個 node 收到「另一個 node 的產物」。徵兆是型別完全不合的錯誤出現在看似無關的函式裡，例如 `build_offset_sweep_section` 爆 `TypeError: list indices must be integers or slices, not dict`——因為 `offset_sweep` 參數收到的是 `config_shift` 的 dict（它的 `per_item` 是 list of dicts，而 offset_sweep 的是 dict）。
- **根因（通用，仍然存在）**：`core/runner.py` 是 `result = node.func(*inputs)`，**位置展開**。`node.inputs` 少了一個元素，其後每個參數都往前移一格。**運氣好才會爆**——兩邊型別相容時不會有任何錯誤，報表會靜靜把 A 診斷的數字印在 B 診斷的標題底下。這個機制沒有修、也修不掉：只要 node 呼叫維持位置傳參，任何具名參數 ≥2 個的 node 都吃這條規則。
- **`generate_report` 這個實例已修復（2026-07-20，Plan 1.5）**：修復前它是「7 個具名參數 ＋ `*args` varargs」——registry 診斷用 `*(f"evaluation_{name}" for name in DIAGNOSES)` 展開接在尾端，中間漏一個完全不會報錯，只會平移。修法是**把簽章改成剛好 8 個必填、無預設值、無 varargs 的參數**（`evaluation_metrics, parameters, baseline_metrics, metric_ci, offset_sweep, pair_ledger, report_aggregates, diagnosis_pages`），對應 `pipeline.py` 裡剛好 8 個元素的 `inputs` 列表。**varargs 消失才是真正的修復**：少一個輸入現在會直接 `TypeError: missing 1 required positional argument`，在 node 呼叫當下就炸，不會等到報表算出一堆型別兜得上但語意錯的數字。純粹「參數個數對得上」不算修好——8 對 8 只是把賭注從「兩邊型別碰巧相容」降到「兩邊個數碰巧相同」，varargs 沒了才是把「漏一個不報錯」這個洞本身補起來。
- **規則**：改動任何 node 的 `inputs` 或其函式簽章之後，跑下面的並排比對。**新增診斷時尤其必跑**（Plan 2-5 每加一項診斷，`generate_report` 的簽章與 `inputs` 都不會變，因為診斷產物已經改走 `render_diagnosis_pages` 而非直接進 `generate_report`——但其他仍是「具名參數＋位置傳參」形狀的 node 一樣要跑這個檢查）。
- **已知未修的同形狀 node（2026-07-20 盤點，範圍外，不在本次修復範圍）**：
  - `log_experiment`（training）：`inputs` 10 個元素、函式 10 個具名參數（8 個必填 ＋ `quadrant_profiles`／`cases_manifest` 兩個有 `None` 預設值）。目前順序對得上，但沒有 varargs 這種「明顯錯誤形態」保護——中間插一個新輸入而忘記同步簽章順序，一樣會靜默位移。
  - `select_shap_population`（training，`diagnosis/model/population_spark.py`）：`inputs` 4 個元素、函式 4 個參數（3 個必填 ＋ `predict_manifest=None`）。同上，形狀小但機制相同。
- **驗證方式**：
  ```bash
  python -c "
  import inspect
  from recsys_tfb.diagnosis.metric.contract import DIAGNOSES
  from recsys_tfb.pipelines.evaluation.nodes_spark import generate_report
  from recsys_tfb.pipelines.evaluation.pipeline import create_pipeline
  sig = list(inspect.signature(generate_report).parameters)
  node = next(n for n in create_pipeline().nodes if n.name == 'generate_report')
  for i in range(max(len(sig), len(node.inputs))):
      s = sig[i] if i < len(sig) else '—'; n = node.inputs[i] if i < len(node.inputs) else '—'
      print('%-2d %-24s %s' % (i+1, s, n))
  "
  ```
  （`create_pipeline()` 不帶參數——舊版寫 `create_pipeline({})`，`{}` 是 falsy，剛好被當成 `post_training=False` 的預設值用，能跑只是巧合，不要照抄。）第 4 位應為 `metric_ci` ↔ `evaluation_metric_ci`、第 5 位 `offset_sweep` ↔ `evaluation_offset_sweep`。repo 內另有 `test_inputs_positionally_match_signature` 守這件事，但**它守不了手動同步的環境**（見 §13）。

## 13. 隔離環境（不能連 git）手動同步後，必須逐檔核對 hash（2026-07-20）

- **症狀（第一分鐘認出它）**：traceback 的行號與你本機的檔案完全吻合，但行為對不上任何一個你認得的版本；或 config 讀到的值與你剛改的不同。典型組合是 **`src/` 是新的、`conf/` 是舊的**——因為兩者是分開拷貝的。
- **根因**：公司環境無法 `git fetch`，程式碼靠手動拷貝進去。拷貝沒有原子性也沒有完整性檢查，漏一個檔或漏一個目錄不會有任何徵兆，而 git 那套「branch 名稱正確 ＝ 內容正確」的直覺在這裡不成立（shell prompt 顯示的 branch 名可能只是個空殼）。
- **規則**：同步之後**先核對 hash 再跑任何東西**。產出清單的方式（在能連 git 的那端）：
  ```bash
  git diff --name-only <對方目前的 base>..HEAD -- src/ conf/ | while read f; do
    printf "%s  %s\n" "$(shasum -a 1 "$f" | cut -c1-12)" "$f"; done
  ```
  刪除的檔案會印不出 hash——那些是**要刪**的，不是漏拷的。對方用 `sha1sum` 逐檔比對。
- **驗證方式**：hash 全對之後，再跑 §12 的位置綁定比對。兩關都過才開始跑 pipeline——否則失敗會出現在 pipeline 尾端，前面所有昂貴計算全部白做。
