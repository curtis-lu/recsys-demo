# Plan 5：改名、ScopeNote 驗收與文件（診斷重構 6/6）

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把第二套也叫「診斷」的東西改名成 overview、派 fresh reader 驗收五份 ScopeNote、重寫文件。

**Architecture:** 純收尾：`evaluation/diagnostics_spark.py` → `overview_spark.py`（行為不動）、外部眼睛驗收 ScopeNote 是否敷衍、刪除 754 行失效判讀手冊、重寫方法論框架文件、寫 quickstart。

**Tech Stack:** Python 3.10.9、PySpark 3.3.2（僅抽樣階段）、pandas 1.5.3、numpy、plotly、Optuna 4.5.0、pytest。

**前置：** Plan 4 已完成並 merge（五項診斷到齊）。

**共用脈絡（開工前必讀，本檔不複述）：** `docs/superpowers/plans/diag-redesign/00-shared-context.md`
——五項診斷的邏輯架構與閱讀順序、檔案結構、持久化邊界（§2.7）、共同統計限制（§3.6）、診斷契約（§4）。

---

## 三條鐵則（每份計畫都重貼，不得省）

這次重構的驗收標準跟一般功能不同，**寫錯方向比寫錯程式更貴**。三條鐵則：

1. **不下結論。** 不得產生 severity、verdict、建議動作、「應該／不足／異常」這類字眼。診斷輸出的是數字、分布、對照點、範圍說明。判斷留給讀者。
2. **不設門檻。** 不得用 config 門檻把連續量切成離散類別（舊 `quadrant.auc_threshold` 就是被這條判死的）。顏色只編碼資料本身的大小或正負，不編碼好壞。
3. **每個數字自帶說明。** 每項診斷必須宣告 `ScopeNote`，寫出它量的是什麼、算在哪批列上、**不能推論什麼**。`blind_to` 為空即契約違反，有測試擋。

**為什麼**：使用者的原話是「我沒有要把人類的思考與判斷外包給你，我要你做的是忠實呈現數據，但是用一個清楚好懂的邏輯架構來幫助人類判斷，而不是直接給結論」。既有的 `triage.py` 正是被否決的那種東西——它已經實作了「per-item 判定＋槓桿建議」，所以它必須死，不是因為寫得不好。

---


---

## 環境前置

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-redesign && pwd
readlink .venv && /Users/curtislu/projects/recsys_tfb/.venv/bin/python -V   # 應為 Python 3.10.9
export SPARK_CONF_DIR=$PWD/conf/spark-local
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python scripts/local_spark_setup.py --check-isolation
```

**測試一律用絕對 venv python ＋ `PYTHONPATH=src`**，裸跑會抓到 main 的 src 而靜默測錯 code：

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-redesign
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest <path> -v
```

**先建立 baseline**（main 上有既知 failing／互擾測試，清單見 `docs/operations/known-pitfalls.md` §5）：

```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_diagnosis tests/test_pipelines/test_evaluation tests/test_core/test_consistency.py \
  -q 2>&1 | tail -20 > /tmp/baseline.txt
```

### 本機 real-run（第一次要先建環境）

worktree 的 `data/` 樹是空的（鐵則 R3：每個 worktree 用自己的真 `data/`，不 symlink 到 main）。第一次要建完整鏈：

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-redesign
export SPARK_CONF_DIR=$PWD/conf/spark-local
V=/Users/curtislu/projects/recsys_tfb/.venv/bin/python
PYTHONPATH=src $V scripts/local_spark_setup.py --reset
PYTHONPATH=src $V -m recsys_tfb dataset  --env local
PYTHONPATH=src $V -m recsys_tfb training --env local      # 印出 model_version，記下來
PYTHONPATH=src $V -m recsys_tfb evaluation --env local --post-training --model-version <mv>
```

**兩個旗標都是必要的，少一個就跑不動**（2026-07-19 實測）：

- `--post-training`：預設模式讀 inference 產出的 `ranked_predictions`，而本機 inference 撞既有 issue #63（`scripts/local_e2e.sh:6-9` 明寫本機 e2e 只收斂到 training）。加了它改讀 training 自己產出的 `training_eval_predictions`（見 `pipelines/evaluation/pipeline.py:74` 的三元式）。
- `--model-version <mv>`：不指定會解析成 `data/models/best` symlink，而那個 symlink 要 promote 才有。**promote 是使用者保留的人工步驟，實作者不得自行執行**（CLAUDE.md 不變量）。用上一步 training 印出的 model_version 代入。


---


---

## Phase 7：改名與 ScopeNote 驗收

### Task 7.1: 改名第二套「診斷」

**Files:**
- Rename: `src/recsys_tfb/evaluation/diagnostics_spark.py` → `overview_spark.py`
- Rename: `tests/test_evaluation/test_diagnostics_spark.py` → `test_overview_spark.py`
- Modify: `report_builder.py`、`nodes_spark.py`、`parameters_evaluation.yaml`

- [ ] **Step 1: 記錄改名前的報表輸出當 baseline**

Run:
```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-redesign
md5 data/evaluation/*/*/report.html > /tmp/report_before.md5 || \
  md5sum data/evaluation/*/*/report.html > /tmp/report_before.md5
```

- [ ] **Step 2: 執行改名**

```bash
git mv src/recsys_tfb/evaluation/diagnostics_spark.py \
       src/recsys_tfb/evaluation/overview_spark.py
git mv tests/test_evaluation/test_diagnostics_spark.py \
       tests/test_evaluation/test_overview_spark.py
```

改名對照（逐一執行，每項改完跑一次相關測試）：

| 現在 | 改成 |
|---|---|
| `report_builder.build_diagnostics_section`（`:794-807`） | `build_overview_section` |
| `generate_report` 的 `diagnostics_frames` 參數 | `overview_frames` |
| config `evaluation.report.sections.diagnostics`（`:59`） | `evaluation.report.sections.overview` |
| config `evaluation.report.diagnostics.include_distributions`（`:70`） | `evaluation.report.overview.include_distributions` |
| config `evaluation.report.diagnostics.include_calibration`（`:71`） | `evaluation.report.overview.include_calibration` |
| config `evaluation.report.diagnostics.n_calibration_bins`（`:72`） | `evaluation.report.overview.n_calibration_bins` |

在 `overview_spark.py` 的 module docstring 開頭加一段：

```
本模組是「分布概覽」——描述性的資料檢視（分數直方圖、箱型圖、名次熱圖、
校準曲線）。它與 ``recsys_tfb.diagnosis`` 套件**沒有關係**：那邊是因果歸因
（回答「為什麼」），這邊是描述（回答「長什麼樣」）。歷史上兩者都叫
「診斷」、報表上又相鄰，讀者分不出在看哪一套，2026-07-19 改名分開。
```

- [ ] **Step 3: 跑測試**

Run:
```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_evaluation tests/test_pipelines/test_evaluation \
  tests/test_core/test_consistency.py -q 2>&1 | tail -10
```
Expected: 全綠。若 `consistency.py` 有 predicate 驗那三個 config 鍵，鍵名同步改，否則 Layer-1 會對已不存在的鍵 raise。

- [ ] **Step 4: real-run 確認報表內容不變**

Run 一次 evaluation，比對 `report.html` 除了區塊標題文字之外內容相同。

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "refactor(eval): diagnostics→overview 改名（兩套同名『診斷』分開，行為不動）"
```

### Task 7.3: `ScopeNote` 的 fresh reader 驗收

> **這個 Task 存在的理由**：`ScopeNote` 最可能的退化方式是變成儀式——作者填一段能通過契約的字、沒人看、於是我們只是把「沒人讀的 96K 手冊」搬成「沒人讀的欄位」。契約保證「有說明」，保證不了「說明是對的、是有用的」。那只能靠外部眼睛。

- [ ] **Step 1: 派 fresh-context subagent**

派一個**沒有本次對話脈絡**的 subagent，只給它五個診斷頁面的 HTML（或 `render_diagnosis.py` 的輸出目錄），**不給任何設計文件、不給本計畫、不給作者結論**。要求它回答：

1. 每一項在量什麼？用你自己的話講。
2. 看完之後，你**不會**誤以為它能告訴你什麼？
3. 哪一段的「看不見什麼」你覺得是敷衍的、或是看不懂的？
4. 有沒有哪個數字你不知道該拿它跟什麼比？

**驗收條件**：至少指出 3 處敷衍、看不懂、或缺對照點的地方，附頁面與段落。找不到 3 處就逐項列出它檢查過哪些面向，證明是查過而不是沒查。

- [ ] **Step 2: 依回饋修 `ScopeNote`**

被指出敷衍的段落重寫。**這一步不得跳過**——「reviewer 沒說什麼大問題」不是通過，是審查失效的訊號。

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "docs(diagnosis): 依 fresh reader 回饋修訂 ScopeNote"
```

---

## Phase 8：文件

### Task 8.1: 刪除失效的判讀手冊

**Files:**
- Delete: `docs/pipelines/evaluation-diagnosis.md`（754 行）

- [ ] **Step 1: 確認引用點**

Run:
```bash
grep -rn "evaluation-diagnosis.md" --include="*.md" --include="*.py" --include="*.yaml" . \
  | grep -v "docs/superpowers/plans/"
```
Expected: 列出所有引用。`docs/superpowers/plans/` 底下的是歷史紀錄，**不改**。

- [ ] **Step 2: 刪除並修引用**

```bash
git rm docs/pipelines/evaluation-diagnosis.md
```

把上一步找到的非歷史引用改指向新的 quickstart 或框架文件。**已知的存活 signpost（2026-07-19 實查，執行時要重新 grep 確認）**：

- `src/recsys_tfb/evaluation/report_builder.py:505`（offset_sweep 區塊的判讀指路）
- ~~`src/recsys_tfb/evaluation/report_builder.py:582`（pair_ledger 區塊的判讀指路）~~ —— **已於 Plan 3 Task 5.4 隨 `build_pair_ledger_section` 整段刪除**（2026-07-20 追記）。留在這裡是為了讓「它去哪了」有答案，不是待辦。
- `conf/base/parameters_training.yaml:162`（`diagnostics.gain_ledger.enabled` 的註解，指向該檔 §12）

第一處所屬的區塊在 Plan 4 會被新診斷取代，屆時可能已自然消失——**執行本 task 時重新 grep，不要照抄這份清單**（上面那條刪除線就是照抄會踩到的實例）。

> `src/recsys_tfb/diagnosis/metric/__init__.py` 原本也有一處，已於 2026-07-19 的 docstring 修正中移除。
>
> **`CLAUDE.md` 沒有引用這份文件**（已 grep 確認零命中），所以路由表那一列不必改——見 Task 8.4。

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "docs: 刪除 evaluation-diagnosis.md（判讀內容已隨數字進報表）"
```

### Task 8.2: 重寫 `ranking-diagnosis-framework.md`

**Files:**
- Rewrite: `docs/ranking-diagnosis-framework.md`

- [ ] **Step 1: 讀寫作規範**

必讀 `docs/handbooks/handbook-writing-guide.md`，並使用 `writing-technical-handbooks` skill（在 `~/.claude/skills/`，手動觸發）。風格要求：白話＋英文括注、禁直譯腔、貫穿數字範例、不洩漏開發脈絡（不寫「我們原本以為…後來發現」這種敘事）。全繁體中文。

- [ ] **Step 2: 寫作**

新框架文件只放**方法論**，判讀說明已經在報表裡，**不得複述**。目標長度 **150–250 行**（舊的 477 行）。章節：

1. **指標是什麼**：macro per-item mAP 的定義，以及它只由 query 內名次決定這個性質。
2. **為什麼是這五項、為什麼是這個順序**：§1 那張表的展開版。每項寫清楚「它回答什麼」「它排除什麼」「它看不見什麼」。
3. **五項共用同一份抽樣**：為什麼這件事重要（不同母體的數字並排解讀會錯）。
4. **這套診斷不做什麼**：不下結論、不設門檻、不給處方。說明為什麼——判斷是讀者的工作，系統只負責讓資料清楚。
5. **誠實條款**：within-item AUC 不是指標原生的量；per-item Δ_j 不可加；Gain 是訓練期的量不是評測期的貢獻；score_shift 的 Δ 有搜尋選擇偏誤。

- [ ] **Step 3: 派 fresh reader 驗收**

派一個沒有本次對話脈絡的 subagent 通讀，要求它回答：「照這份文件，你能不能講出五項各回答什麼、為什麼是這個順序？哪一段你讀不懂？」——**不給它任何本次對話的結論**。至少 3 個具體問題，找不到就列出檢查過的面向。

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "docs: 重寫排序診斷框架（方法論 150-250 行，判讀已進報表）"
```

### Task 8.3: 寫 quickstart

**Files:**
- Create: `docs/pipelines/evaluation-diagnosis-quickstart.md`

- [ ] **Step 1: 寫作**

只放**操作**，目標 **60–100 行**：
- 怎麼跑（`python -m recsys_tfb evaluation --env local`）
- 五個 `enabled` 開關在哪（逐字列出 config 鍵路徑，開檔核對不憑記憶）
- 產物在哪（§2.6 那張版面圖）
- 怎麼只重跑某一項（`--from-node diagnose_<name>`，附實測過的指令）
- 成本量級表（§Phase 6 Step 5 real-run 的實測秒數，**不是估計值**）

- [ ] **Step 2: 逐字核對 config 鍵**

Run:
```bash
grep -n "enabled" conf/base/parameters_evaluation.yaml
```
把輸出與文件中寫的鍵路徑逐字比對。**不得憑記憶寫鍵名。**

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "docs: evaluation 診斷 quickstart（操作與成本實測）"
```

### Task 8.4: 更新 CLAUDE.md 路由表與 graphify

- [ ] **Step 1: 更新路由表**

Modify `CLAUDE.md`：路由表**新增**一列指向新的框架文件與 quickstart。

> **不是改既有的列**：2026-07-19 實查 `grep -n "evaluation-diagnosis" CLAUDE.md` **零命中**——`CLAUDE.md` 從來沒有引用過那份 96K 判讀手冊。執行時先自己 grep 確認現況，若仍零命中就是純新增。

- [ ] **Step 2: 重建 graphify**

Run:
```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-redesign
.venv/bin/python -c "from graphify.watch import _rebuild_code; from pathlib import Path; _rebuild_code(Path('.'))"
```

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "docs: CLAUDE.md 路由表對齊新診斷文件＋graphify rebuild"
```

---

## 9. 全案驗收（整個重構六份計畫的總驗收）

- [ ] **全套測試綠**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-redesign
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_diagnosis tests/test_report tests/test_evaluation \
  tests/test_pipelines/test_evaluation tests/test_core/test_consistency.py \
  -q 2>&1 | tail -20
```
與 §6 建立的 `/tmp/baseline.txt` 比對，**不得有新增 fail**。

- [ ] **端到端 real-run 產物齊全**

五份診斷 HTML ＋ index ＋ 一份共用 js ＋ 五份 JSON，每份 HTML < 500KB。

- [ ] **禁判定字眼全域掃描**

```bash
grep -rn "建議\|應該\|異常\|不足\|verdict\|severity\|recommend" \
  src/recsys_tfb/diagnosis/metric/*/render.py
```
Expected: 零命中（`ScopeNote` 的 `blind_to` 裡「不代表」這類否定句不算判定，但不得出現對讀者的指示句）。

- [ ] **邊界宣稱仍成立**

```bash
grep -rn "from recsys_tfb.pipelines\|import recsys_tfb.pipelines" src/recsys_tfb/diagnosis/
grep -rn "from recsys_tfb.evaluation.report_builder" src/recsys_tfb/diagnosis/
```
Expected: 兩者皆零命中。`diagnosis/` 只依賴 `core`／`evaluation.metrics`／`io`／`utils`／`report`。

- [ ] **報表層不認識個別診斷**

```bash
grep -n "config_shift\|item_ability\|model_capacity\|suppression\|score_shift" \
  src/recsys_tfb/evaluation/report_builder.py
```
Expected: 零命中（`assemble_diagnosis_pages` 是透過 `DIAGNOSES` registry 動態載入的）。

- [ ] **清掉參考腳本**

`scripts/*_diagnosis.py` 六份與 `tests/scripts/test_*_diagnosis.py` 兩份是本次的參考實作，功能已進 `src/`。刪除或保留由使用者決定——**這一步要問，不要自己刪**。

- [ ] **fresh-context 驗收**

派一個沒有本次對話脈絡的 subagent 審查 `git diff main..feat/diag-redesign`，只給它 §0 的三條鐵則與 §9 的驗收條件，**不給任何作者結論**。要求至少 3 個具體問題（附檔案:行號與失敗情境），找不到就逐項列出檢查過的面向。

### 9b. Plan 3 執行時發現、刻意延後到這裡的兩個落差

兩者都是 Plan 3 查證途中撞到的既有缺口，**當下沒順手修**（不屬於那個 task 的範圍，見 `~/.claude/rules/90-letter-to-future-sessions.md` 第 3 點：deferred 項目被延後都有原因）。收尾時一併處理：

- [ ] **`_common.py` 沒有自己的測試檔**

`query_key`／`sample_arrays`／`ci_for_corrected_minus_baseline`／`to_logit`／`apply_injection` 全靠消費者（`config_shift`／`item_ability`）間接覆蓋。Plan 3 Task 5.1 為了 `per_item_ap` **新建**了 `tests/test_diagnosis/test_metric/test_common.py`，但只放了那兩條。

**為什麼要補**：共用層沒有自己的測試，改壞它的紅燈只會出現在別人的測試檔裡，訊息也指不到根因——而這一層現在有五個消費者。至少補：`query_key` 的多欄併鍵與分隔字元、`sample_arrays` 在缺 `inclusion_weight` 欄時 `ht_weights is None` 而 `row_weights` 全 1（**這兩條路是位元等價的，所以只驗數值的測試守不住，要斷言 `is None`**）、`ci_for_corrected_minus_baseline` 的方向（見它自己的 docstring）。

- [ ] **`evaluation.diagnosis.item_ability.top_n` 沒有 consistency 驗證**

查證：`grep -rn "top_n" src/recsys_tfb/core/consistency.py` 零命中。Plan 3 給 `suppression.top_examples` 加了 A19 驗證（非負 int），`top_n` 沒有是不一致的。

補在 **A15**（`diagnosis_metric_param_errors`）而不是新代號——它與 `sample.max_queries` ≥ 1 之類的同屬「診斷參數域」，不值得為它開一個代號。legend 同步更新。

---

## 10. 整個重構刻意不做的事

- **不做 severity／verdict／建議動作。** 見 §0。
- **不做宣告式圖表規格語言。** `display` config 的範圍**只到表格**（欄位順序／欄名／格式／排序）。圖表留在程式碼裡——一套通用圖表規格總會有 20% 的圖擠不進去，最後變成規格＋例外，比直接寫程式更複雜。
- **不用測試去守「不畫紅綠燈」。** 那種測試（斷言某些函式名不存在）防不了它宣稱要防的事——直接在 `figures.py` 寫死色碼就繞過了——卻讓人以為防住了。改成註解＋code review。同理不做 `DiagnosisSpec` dataclass 與 `slug_for()`：兩者都是可由既有資訊推導的包裝，只增加要學的概念數。
- **不做 Spark 端的 AUC／壓制帳本。** 五項共用一份抽樣是一致性保證；效能先靠 sort-once 與向量化解決，不夠快再談，且要先有實測數字。
- **不做座標下降版的 score_shift。** Optuna 版有 L2 與曝光 guardrail，成本又與 item 數脫鉤。
- **不做校準相關的任何東西。** 本專案目標是排序不是校準。舊的 reconciliation 層已於 `48364d5` 整層刪除，不要復活它。
- **不改 `diagnosis/model/`（訓練側 SHAP）與 `diagnosis/hpo/`。** 它們與 `metric/` 零交叉 import，本次不動。注意 `hpo/paths.py:5` 依賴 `model/paths.py` 的 `diagnostics_dir`，動 `model/` 會波及 `hpo/`——本次兩者都不動。
- **不改 `docs/superpowers/plans/` 底下的歷史計畫檔。** 它們是紀錄，不是現況文件。

---

## 公司環境檢視點（本 Plan 的交付驗收）

1. **`report.html` 的區塊名稱**——「分布概覽」與「排序診斷」分開之後，是否還會搞混？
2. **重寫後的 `ranking-diagnosis-framework.md`**——只看這份文件，你能不能講得出五項各回答什麼、為什麼是這個順序？講不出來就是沒過。
3. **quickstart 的成本表**——五項的實測秒數是否與你在公司環境的體感一致。

**看完給回饋之後**：全案驗收清單在本檔末尾 §9。其中「刪掉 `scripts/` 那六個參考腳本」一項**必須問你**，不由執行者決定。
