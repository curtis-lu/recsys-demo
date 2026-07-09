# Phase 4b：壓制帳本（pair_ledger）— 實作計畫

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 框架診斷項目 7 的兩個互補傷害歸因工具：(a) **成對帳本**——對每個正例列列舉同 query 排其上方的**負例** item，記交換名次的指標敏感度 |ΔAP|（lambdarank 的 λ 梯度定義，只做會計不訓練）→「壓制者 × 受害者」矩陣＋傷害 × segment 分組；(b) **substitution ablation**——逐 item 把分數換成 base-rate logit 常數、重算參數化 macro mAP（O(M) 次）→ 每 item 個性化分數的淨貢獻／淨傷害。落 `diagnosis/pair_ledger.json`、報表新 section（plotly heatmap）與判讀手冊擴充。依 spec `docs/superpowers/specs/2026-07-06-diagnosis-pipeline-integration-design.md` §3 Phase 4（138–141 行）。**開工第一件事＝修 opus N1（折別穩定性）**——與本階段綁定因為修了 δ*/LOO 數字會變，手冊 §10.6 示例表與乾淨基準快照要在同一次真跑重生。

**Architecture:** 三段：(1) 前置修繕——`offset_sweep.py` 折切改 CRC32 hash 折別（列序無關）＋ logit/注入邏輯抽到新私有模組 `diagnosis/metric/_common.py`（pair_ledger 共用）＋ `sample.py` keep_cols 補 segment 欄；(2) 新診斷模組 `diagnosis/metric/pair_ledger.py`（`pair_ledger(sample_pdf, parameters) -> dict` 傘函數，內含 `substitution_ablation`）；(3) 評估 pipeline 薄節點 `compute_pair_ledger`＋catalog＋報表 `build_pair_ledger_section`（`go.Heatmap`）＋consistency **A19**。

**Tech Stack:** numpy／pandas（driver-side；只有節點入口抽樣走 Spark）、`evaluation/metrics.py` 參數化 numpy 家族、`zlib.crc32`（stdlib）、plotly `go.Heatmap`、pytest、本機 local Spark。

**Scope note:** 閘門**只跑 evaluation**（model_version 6059dcef，零重訓）。已知答案＝(a) 單元測試的手算 fixture（3 列 query 的 |ΔAP| 精確值、substitution 精確 mAP）、(b) 真跑注入 `debug_inject_offsets: {ccard_ins: 1.0}` → pair_ledger 的 ccard_ins 壓制次數暴增（spec 驗收 1 後半）＋ offset_sweep centered 位移 ≈ −1.0＋**metric_ci／reconciliation／quadrant 三份 JSON 位元不變**（負控制）、(c) 清除注入 → 五份 JSON 與注入前的新乾淨基準位元一致。**注意：state A（乾淨態）的 offset_sweep.json 相對現行基準「合法地變」**——N1 hash 折別改變折別組成，δ*/LOO 數字會動；判讀比對 centered 值量級即可，位元比對只在 A↔C 之間做。

---

## 執行者必讀（違反會靜默做錯）

1. **一切都在 worktree**：repo root＝`/Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework`，branch `feat/diag-framework`。每個 Bash 指令以 `cd <該路徑> && ...` 開頭；Edit/Write 絕對路徑必含 `.worktrees/diag-framework`。
2. **跑 python 一律**：`PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest|recsys_tfb ...`。
3. **可能超過 2 分鐘的指令（evaluation 真跑）一律背景執行**。evaluation CLI 必帶 `--model-version 6059dcef --post-training`（本機無 inference 產物，known-pitfalls §6；無 `best` symlink，promote 是使用者保留的人工步驟）。
4. **生產不變量**：no Spark UDF、no new packages；`diagnosis/*` 只 import `core / evaluation(僅 numpy 原語 metrics.py) / io / utils`＋pandas/numpy/標準庫；**diagnosis 不 import pyspark／plotly**（`utils/hashing.py` top-level import pyspark，所以 diagnosis **不得 import 它**——hash 常數在 `_common.py` 本地重申並註記對齊）。heatmap 在報表側建。
5. **本階段不動 dataset/training config**：閘門注入只動 `evaluation.diagnosis.debug_inject_offsets`（不影響 model_version），結束時還原。
6. 測試判準＝與 baseline 一致（known-pitfalls §5）；`SPARK_LOCAL_IP=127.0.0.1` 已釘進 conftest，不需手動設。
7. 欄名一律經 `get_schema(parameters)` 取；segment 欄名經 `evaluation.segment_columns` 取，勿硬編 `cust_segment_typ`。

## 設計定案（所有 task 共用語意，不要各自發明）

- **N1 修法＝query key 的 CRC32 hash 折別（計畫階段拍板）**：舊法（`RandomState` permutation over `ngroup()` 位置碼）依 `toPandas()` 列序（`sample.py:115` 無 orderBy），公司叢集不同平行度重跑可能翻折別。新法：對 query key 字串算 `zlib.crc32(f"{key}|offset_sweep_fold|{seed}") % 100_000`，`bucket < round(holdout_fraction*100_000)` → holdout。同 query 所有列 key 相同 → 折別列序無關。datetime64 欄先 `strftime("%Y-%m-%d %H:%M:%S")` 正規化（對齊 `utils/hashing.py` 的 Spark 端慣例），其他欄 `astype(str)`（null → 字串 `"nan"`/`"None"`，確定性自成一組——沿 461b59d 的 null 鍵語意）。**近似比例**（binomial，非精確 round(n×fraction)）——診斷用途可接受，手冊已知限制註記。**保 fit 非空**：全部 query 落 holdout 時，把 bucket 最小的 query（可能複數，同 bucket 全移）移入 fit＋確定性；holdout 空的既有語意不變（折外 None＋notes）。`_HASH_BUCKETS=100_000` 本地常數＝`utils.hashing.HASH_BUCKETS`（不 import，見必讀 4）。
- **注入進 pair_ledger（計畫階段拍板：要）**：spec 驗收 1 後半「pair_ledger 應顯示該 item 的壓制次數暴增」只有注入生效才驗得到。抽共用私有模組 **`diagnosis/metric/_common.py`**：`diag_cfg / metric_params / to_logit / parse_injection / apply_injection`（自 offset_sweep 平移，行為不變）；offset_sweep 與 pair_ledger 都 import 它。`debug_inject_offsets` scope 語意更新為「**僅分流層家族**（offset_sweep＋pair_ledger 兩個 leaf 節點）」——config 註解、手冊同步改；metric_ci／reconciliation／quadrant 仍完全不受影響（閘門負控制不變）。注入 note 文案統一為 `debug_inject_offsets 生效（僅分流層節點；基準指標為注入後現狀）：{...}`（offset_sweep 舊文案「僅本節點」一併換掉——其 JSON 本來就因 N1 而變，不存在位元回歸包袱；有釘舊文案的既有測試改斷言＝預先授權）。
- **|ΔAP| 的精確定義（λ 會計；鍵名即契約）**：query 內以 `np.lexsort((-z, groups))` 排序（與 `positive_row_contributions` 完全同款，tie 處理一致）。對正例列 $i$（rank $b$）與其上方**負例**列 $j$（rank $a<b$；同 query 也是正例的 item 不記帳——Bob 效應，框架 Ch 1.4），交換兩列名次造成的**該 query AP 貢獻總和**變化：
  $\Delta\text{AP}_{ij} = c(a,\,P_a{+}1) - c(b,\,P_b) + \sum_{\text{正例 } r\in(a,b)} \mathbb{1}[r \le k]\cdot \tfrac{1}{r}$，
  其中 $c(r,p)= p/r$ 若 $r\le k$ 否則 $0$、$P_r$＝排序後前 $r$ 名的正例累計數、$k$＝`evaluation.metric.k`（null → 不截斷）。$k$=null 時恆 $>0$；$k$ 截斷下可為 0（兩列都在 $k$ 外）——**照記 pair_count，dap 記 0**（帳要誠實）。全額 |ΔAP| 記在 (壓制者=item(j), 受害者=item(i)) 這一格（Burges λ 慣例；中間正例的外溢貢獻不另拆——手冊已知限制註明「query-AP 粒度，非 macro 全式」）。手算錨（單元測試釘死）：query 排序 [B−, A+, C+] → ΔAP(B→A)=1/1−1/2=**0.5**、ΔAP(B→C)=1/1−2/3+1/2=**5/6**。
- **substitution ablation**：對每個 item $j$：`z_sub = z.copy(); z_sub[items==j] = logit(clip(base_rate_j, 1e-12, 1-1e-12))`，`base_rate_j = mean(label | item j)` **取自診斷抽樣**；重算 `compute_macro_per_item_map(groups, items, y, z_sub, **metric_params)`。`delta_vs_current = map_substituted − map_current`：**負值＝個性化分數有淨貢獻（換掉會變差），正值＝淨傷害**。只做 spec 明定的「換掉 j」方向；「只留 j」反向 v1 不做（YAGNI，手冊已知限制註明）。`map_current` 與所有替換都算在**注入後**的 z 上（與 offset_sweep 的 mAP(0) 語意一致：量的是現狀）。
- **全樣本、不切折（計畫階段拍板）**：pair_ledger 與 substitution 是描述性會計、無擬合搜尋，無過擬合疑慮——不切 holdout，全部抽樣 query 都記帳。
- **by_segment（傷害 × 分群）**：per 正例列傷害 = 該列所有受壓 pair 的 ΔAP 加總（victim 側 row total）。按 `evaluation.segment_columns` 每欄分組（值 `astype(str)`，NaN/None → `"null"`）：`{n_pos_rows, n_suppressed_pos_rows, dap_sum, dap_share}`。配置的 segment 欄不在抽樣中 → notes 註記＋該欄略過（不炸）。**邊界（spec 明文）**：segment 欄由 `prepare_eval_data` 的 `join_segment_sources` 早已併進 eval_predictions——diagnosis 只消費欄位，**不 import** `evaluation/segments.py`。前置：`sample.py` keep_cols 補 segment 欄（Task 2）。
- **函數形狀**：spec 具名的兩個函數都存在——`pair_ledger(sample_pdf, parameters) -> dict`（**傘函數**，內部呼叫 `substitution_ablation` 併入輸出，node 只呼叫它）與 `substitution_ablation(sample_pdf, parameters) -> dict`（獨立可測）。理由：合併邏輯（notes 去重）屬領域層，不放 node。
- **輸出 dict（JSON-ready，鍵名即契約，報表與文件都吃它）**：頂層 `enabled / score_col_used / metric_params(echo) / injected_offsets(echo) / n_queries / n_pos_rows / n_mis_ordered_pairs / matrix{supp:{vict:{pair_count,dap_sum}}} / by_suppressor{item:{pair_count,dap_sum,dap_share}} / by_victim{同}／map_current / substitution{item:{base_rate,base_logit,map_substituted,delta_vs_current}} / by_segment{col:{val:{n_pos_rows,n_suppressed_pos_rows,dap_sum,dap_share}}} / notes[]`；`dap_share`＝該格 dap_sum／全帳 dap_sum（全帳為 0 → None）。所有 dict 鍵排序輸出（JSON 確定性）。節點再補 `sample`（抽樣 metadata，沿 `compute_offset_sweep` 慣例）。空抽樣 → 計數 0／空 dict＋notes，不炸。
- **停用 stub／必要輸入**：沿 `compute_offset_sweep` 模式（`nodes_spark.py:361-400`）——`enabled: false` → `{"enabled": False}` stub；enabled 但 `eval_predictions is None` → `ValueError`。
- **報表**：`build_pair_ledger_section`——figure＝`go.Heatmap`（列＝壓制者、欄＝受害者、z＝dap_sum、colorscale `"Blues"`、hover 帶 pair_count）；tables＝壓制者邊際表（dap_sum 降冪）＋ substitution 表（delta_vs_current 降冪）＋ by_segment 表。`n_mis_ordered_pairs == 0` → 不畫圖只留表＋description 說明。glossary 補條目。
- **既有測試會被本計畫「合法」改到的**：(a) `test_offset_sweep.py` 折切相關——`test_holdout_split_counts`（精確 6/12）改為分割不變量＋新增列序 shuffle 不變性測試（N1 的核心回歸）；釘注入舊文案的斷言改新文案；(b) `tests/test_pipelines/test_evaluation/test_pipeline.py` 結構斷言——default/post_training **9→10** node、compare **12→13**、node 名清單加 `compute_pair_ledger`（`compute_offset_sweep` 之後、`generate_report` 之前）、outputs 加 `evaluation_pair_ledger`；(c) report sections／diagnosis config 鍵的 exact-set 斷言 additive 更新。以上**預先授權**；**任何非 additive 的既有測試改動 → 停下回報**。
- **shuffle 不變性測試的比對精度**：結構欄（items、delta_star——皆 grid 點、counts、notes）**精確相等**；mAP 類浮點欄 `assert_allclose(rtol=1e-12)`（`np.bincount` 累加順序可能差最後一 ulp）。
- **閘門的已知答案基準**：注入 item 沿 Phase 4a 用 `ccard_ins`、量 +1.0。判準：offset_sweep 的 **centered 位移** ∈ [−1.35, −0.65]（gauge 讀法，spec 修訂）；pair_ledger 的 `by_suppressor["ccard_ins"]`（pair_count 與 dap_sum）相對 state A **暴增**（state A 記錄實際值，state B 至少 5× 或從近零跳到顯著——實際倍率如實記錄）；三份非分流 JSON 位元不變。
- **效能**：pair 枚舉＝per query 一次 numpy 段運算（query 長度 ≤ item 數 M：本機 8、公司 22，per-query pair 矩陣 ≤22×22）——本機 654 query 毫秒級；公司 200k query 的 Python 迴圈估 **10–90 秒**。substitution＝(M+1) 次全量指標評估（lexsort 4.4M 列 ~1s）≈ **30 秒**。皆遠低於 offset_sweep；Task 8 實測本機、按列數外推寫進手冊。**不做進一步優化**（v1 取簡單正確）。
- **文件是一等交付物（spec §3 固定結構）**：Task 9 內建——手冊新增 §11 壓制帳本（含數感節＋真跑示例走讀）、§12 已知限制改號＋新增條目、**§10.6 示例數字因 N1 重生**、glossary。寫法鐵則（禁用開發詞彙、示例產物印進文件、讀者 agent 驗洩漏）不可省。

## 執行模式（controller 注意）

同 Phase 4a＋提速協議（HANDOFF 執行協議 9）：Task 1、8 controller 直跑；**Task 2 派 sonnet implementer A**（前置三合一）；**Task 3 派 sonnet implementer B**（pair_ledger 模組，最大件）；**Task 4–6 合批派 sonnet implementer C**（config/node/報表，同 setup、內部逐 task TDD＋各自 commit；C 依賴 B 的模組存在，不並行）；**Task 7 合併 reviewer（sonnet）背景執行**、與 Task 8 真跑並行，prompt 附 controller 綠燈證據＋明令只讀 diff、只跑新增/變更測試檔；Task 9 文件 writer 的素材包由 controller 從 Task 8 產物先備好；**Task 10 opus 總審背景執行**。所有 implementer prompt **直接內嵌該 task 全文＋執行者必讀＋設計定案**，計畫檔路徑只作查證。controller 在 Task 6 完成後跑一次 graphify rebuild（CLAUDE.md §graphify）。

---

### Task 1：pre-flight ＋ baseline（controller 直跑）

**Files:** 無程式碼變更；產出 `/tmp/phase4b_test_baseline.txt`、`/tmp/phase4b_json_before/`。

- [ ] **Step 1: pre-flight**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && pwd && readlink .venv && \
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -V && \
export SPARK_CONF_DIR=$PWD/conf/spark-local && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python scripts/local_spark_setup.py --check-isolation && \
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework status --short
```
Expected: worktree root、Python 3.10.9、isolation OK、working tree 乾淨（@4d00fb9 之後）。

- [ ] **Step 2: 相關測試 baseline（背景）**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_diagnosis/test_metric/ tests/test_core/test_consistency.py \
  tests/test_evaluation/test_report_builder.py \
  tests/test_pipelines/test_evaluation/ \
  -q 2>&1 | tail -5 | tee /tmp/phase4b_test_baseline.txt
```
Expected: 259 passed（f44d4f8 收尾數字），記下確切數字。

- [ ] **Step 3: 產物快照（負控制基準）**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
mkdir -p /tmp/phase4b_json_before && \
cp data/evaluation/6059dcef/20260131/diagnosis/*.json /tmp/phase4b_json_before/ && \
ls -la /tmp/phase4b_json_before/
```
Expected: metric_ci.json、reconciliation.json、quadrant_summary.json、offset_sweep.json 四份。

---

### Task 2：前置三合一——N1 hash 折別＋`_common.py` 共用 helper＋sample keep_cols（implementer A）

**Files:**
- Create: `src/recsys_tfb/diagnosis/metric/_common.py`
- Modify: `src/recsys_tfb/diagnosis/metric/offset_sweep.py`
- Modify: `src/recsys_tfb/diagnosis/metric/sample.py`（keep_cols）
- Test: `tests/test_diagnosis/test_metric/test_offset_sweep.py`、`tests/test_diagnosis/test_metric/test_sample.py`

三件互相糾纏（都動 offset_sweep 一帶）所以合一個 implementer，內部仍逐件 TDD＋各自 commit。

- [ ] **Step 1: 新增 `_common.py`（自 offset_sweep 平移，行為不變）**

完整內容：

```python
"""Metric-diagnosis 家族共用私有 helper（offset_sweep ＋ pair_ledger）.

抽出動機：``debug_inject_offsets`` 的注入語意是分流層閘門的已知答案來源，
必須在家族內完全一致，不允許兩份複製品各自漂移。scope＝僅分流層家族的
兩個 leaf 節點；metric_ci／reconciliation／quadrant 不受影響。

``_HASH_BUCKETS`` 與 ``utils.hashing.HASH_BUCKETS`` 同值（100_000）——
該模組 top-level import pyspark，diagnosis 依賴白名單禁止，故本地重申。
"""
from __future__ import annotations

import numpy as np
import pandas as pd

_CLIP_EPS = 1e-12
_HASH_BUCKETS = 100_000


def diag_cfg(parameters: dict) -> dict:
    return ((parameters.get("evaluation", {}) or {})
            .get("diagnosis", {}) or {})


def metric_params(parameters: dict) -> dict:
    m = ((parameters.get("evaluation", {}) or {}).get("metric", {}) or {})
    k = m.get("k")
    return {
        "k": int(k) if k is not None else None,
        "weight_alpha": float(m.get("weight_alpha", 0.0)),
        "min_positives": int(m.get("min_positives", 0)),
        "shrinkage_k": float(m.get("shrinkage_k", 0.0)),
    }


def to_logit(scores: np.ndarray) -> tuple[np.ndarray, list[str]]:
    s = np.asarray(scores, dtype=np.float64)
    if len(s) and (s.min() < 0.0 or s.max() > 1.0):
        return s.copy(), [
            "score 超出 (0,1)——略過 logit 變換，δ 單位為原始分數尺度"
        ]
    z = np.clip(s, _CLIP_EPS, 1.0 - _CLIP_EPS)
    return np.log(z / (1.0 - z)), []


def parse_injection(parameters: dict) -> dict:
    return {
        str(k): float(v)
        for k, v in (diag_cfg(parameters)
                     .get("debug_inject_offsets", {}) or {}).items()
    }


def apply_injection(
    z: np.ndarray, items: np.ndarray, inject: dict,
) -> tuple[np.ndarray, list[str]]:
    if not inject:
        return z, []
    notes = [
        f"debug_inject_offsets 生效（僅分流層節點；基準指標為注入後現狀）："
        f"{inject}"
    ]
    unknown = sorted(set(inject) - set(items.tolist()))
    if unknown:
        notes.append(f"注入鍵不在抽樣 item 中（無作用）：{unknown}")
    return z + pd.Series(items).map(inject).fillna(0.0).to_numpy(), notes
```

- [ ] **Step 2: offset_sweep.py 改用 `_common`**

刪除 `_diag_cfg`、`_metric_params`、`_logit_scores` 與 `_CLIP_EPS`；改：

```python
from recsys_tfb.diagnosis.metric._common import (
    apply_injection, diag_cfg, metric_params, parse_injection, to_logit,
)
```

`sweep()` 內對應替換：`diag = diag_cfg(parameters)`、`mp = metric_params(parameters)`、`inject = parse_injection(parameters)`、`z, z_notes = to_logit(...)`；注入段改：

```python
    z, inj_notes = apply_injection(z, items, inject)
    notes.extend(inj_notes)
```

（取代原本手寫的 `z = z + pd.Series(...)`＋兩段 notes；注入 note 文案隨之更新為 `_common` 版。）

- [ ] **Step 3: 跑既有 offset_sweep 測試，確認重構後行為不變**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_diagnosis/test_metric/test_offset_sweep.py -q 2>&1 | tail -3
```
Expected: 若 `test_injection_echoed_and_noted` 釘了舊文案「僅本節點」→ 改斷新文案子字串 `"僅分流層節點"`（預先授權）；其餘全綠。

- [ ] **Step 4: Commit（重構段）**

```bash
git add src/recsys_tfb/diagnosis/metric/_common.py src/recsys_tfb/diagnosis/metric/offset_sweep.py tests/test_diagnosis/test_metric/test_offset_sweep.py
git commit -m "refactor(diagnosis): 抽 _common.py 共用 logit/注入 helper（pair_ledger 前置）"
```

- [ ] **Step 5: 寫 N1 的失敗測試（列序 shuffle 不變性）**

加進 `tests/test_diagnosis/test_metric/test_offset_sweep.py` 的 `TestMechanics`：

```python
    def test_fold_assignment_invariant_to_row_order(self):
        # opus N1：折別不得依 toPandas() 列序（公司叢集不同平行度會翻折）。
        pdf = _interleaved_pdf()
        shuffled = pdf.sample(frac=1.0, random_state=7).reset_index(drop=True)
        a = sweep(pdf, _params())
        b = sweep(shuffled, _params())
        assert a["items"] == b["items"]
        assert a["delta_star"] == b["delta_star"]          # grid 點，精確
        assert a["n_queries_fit"] == b["n_queries_fit"]
        assert a["n_queries_holdout"] == b["n_queries_holdout"]
        for fold in ("map_fit", "map_holdout"):
            for kk in ("zero", "star"):
                va, vb = a[fold][kk], b[fold][kk]
                assert (va is None) == (vb is None)
                if va is not None:
                    np.testing.assert_allclose(va, vb, rtol=1e-12)
```

- [ ] **Step 6: 跑它確認 FAIL**

Run: `PYTHONPATH=src ... -m pytest tests/test_diagnosis/test_metric/test_offset_sweep.py::TestMechanics::test_fold_assignment_invariant_to_row_order -q`
Expected: FAIL（舊法 permutation over ngroup 位置碼，shuffle 後折別組成不同）。若意外 PASS（fixture 太對稱）→ 換 `random_state` 重驗一次，仍 PASS 才回報。

- [ ] **Step 7: 實作 hash 折別**

`offset_sweep.py`：`import zlib`（頂部標準庫區）；`_split_queries` 整個替換為：

```python
_FOLD_SITE = "offset_sweep_fold"


def _fold_split(
    sample_pdf: pd.DataFrame, query_cols: list, holdout_fraction: float,
    seed: int,
) -> tuple[np.ndarray, np.ndarray]:
    """query 層 hash 折別（列序無關；opus N1 修，2026-07-08）。

    對 query key 字串 CRC32 分桶：bucket < fraction*BUCKETS → holdout。
    同 query 各列 key 相同 → 折別與 toPandas() 列序無關。近似比例
    （非精確 round(n×fraction)）。fit 折空時把 bucket 最小的 query 移入
    fit（確定性），保 fit 非空；holdout 空由呼叫端既有路徑處理。
    """
    from recsys_tfb.diagnosis.metric._common import _HASH_BUCKETS

    parts = []
    for c in query_cols:
        s = sample_pdf[c]
        if pd.api.types.is_datetime64_any_dtype(s):
            parts.append(s.dt.strftime("%Y-%m-%d %H:%M:%S"))
        else:
            parts.append(s.astype(str))
    keys = parts[0] if len(parts) == 1 else parts[0].str.cat(parts[1:], sep="|")
    token = f"|{_FOLD_SITE}|{seed}"
    buckets = keys.map(
        lambda k_: zlib.crc32(f"{k_}{token}".encode()) % _HASH_BUCKETS
    ).to_numpy()
    threshold = int(round(holdout_fraction * _HASH_BUCKETS))
    hold_mask = buckets < threshold
    fit_mask = ~hold_mask
    if len(buckets) and not fit_mask.any():
        rescue = buckets == buckets.min()
        fit_mask, hold_mask = rescue, ~rescue
    return fit_mask, hold_mask
```

`sweep()` 內呼叫點改為（groups 仍留給指標用）：

```python
    fit_mask, hold_mask = _fold_split(
        sample_pdf, query_cols, holdout_fraction, seed
    )
```

（import 放頂部而非函數內亦可——與檔案既有慣例對齊。）

- [ ] **Step 8: 更新折切既有測試**

`test_holdout_split_counts` 改名＋改斷言（hash 折別無精確 round 保證）：

```python
    def test_fold_partition_invariants(self):
        out = sweep(_interleaved_pdf(), _params())
        assert out["n_queries_fit"] + out["n_queries_holdout"] == 12
        assert out["n_queries_fit"] >= 1        # fit 非空保證
        assert out["n_queries_holdout"] >= 1    # 本 fixture 實際不劣化（驗過再釘）
```

先印實際折數確認 fixture 在 hash 折別下兩折皆非空；若 holdout 為 0（hash 恰好全落 fit）→ 把 fixture `n_queries` 12 調成 16 或 20（`_interleaved_pdf` 參數化過），選一個兩折皆非空的值後**所有用到該 fixture 的測試同步用同一值**，並在 fixture docstring 註記原因。`test_single_query_leaves_holdout_empty_without_crash`、`test_null_query_key_rows_get_defined_fold_assignment` 預期不用改（rescue 保 fit 非空 → 單 query 必落 fit；null 鍵變 `"nan"` 字串桶，13 組計數不變）——跑過確認，有變化如實回報。

- [ ] **Step 9: 全檔測試綠＋mutation check**

```bash
PYTHONPATH=src ... -m pytest tests/test_diagnosis/test_metric/test_offset_sweep.py -q 2>&1 | tail -3
```
Expected: 全綠（含 Step 5 新測試）。mutation check：把 `_fold_split` 的 `token` 臨時改成不含 seed 的常數字串 → `test_metric_params_and_config_echoed` 等不動、但改 seed 相關測試（若有 determinism-across-seed 斷言）或 shuffle 測試仍綠是允許的；**必驗**：臨時把 `buckets` 換回 `np.arange(len(sample_pdf)) % 2`（列序依賴）→ `test_fold_assignment_invariant_to_row_order` 轉紅，改回。回報弄壞了哪行。

- [ ] **Step 10: Commit（N1 段）**

```bash
git add src/recsys_tfb/diagnosis/metric/offset_sweep.py tests/test_diagnosis/test_metric/test_offset_sweep.py
git commit -m "fix(diagnosis): offset_sweep 折別改 CRC32 hash（列序無關，opus N1）"
```

- [ ] **Step 11: sample.py keep_cols 補 segment 欄（TDD）**

先在 `tests/test_diagnosis/test_metric/test_sample.py` 加測試（沿該檔既有 Spark fixture 慣例改寫——fixture 名以實際檔案為準，**預先授權對齊**）：

```python
    def test_segment_columns_kept_when_configured_and_present(self, spark):
        # 沿本檔既有的 eval_predictions fixture，多帶一個 segment 欄
        sdf = <既有 fixture>.withColumn("seg_a", F.lit("x"))
        params = <既有 params>  # 加 evaluation.segment_columns = ["seg_a", "seg_missing"]
        pdf, _meta = draw_diagnosis_sample(sdf, params)
        assert "seg_a" in pdf.columns          # 配置且存在 → 帶回
        assert "seg_missing" not in pdf.columns  # 配置但不存在 → 靜默略過（沿 score_uncalibrated 慣例）
```

跑確認 FAIL（現行 keep_cols 不含 segment 欄）。實作：`sample.py` 的 keep_cols 段改為：

```python
    seg_cols = list((parameters.get("evaluation", {}) or {})
                    .get("segment_columns", []) or [])
    keep_cols = list(dict.fromkeys(
        c
        for c in [*query_cols, item_col, label_col, score_col,
                  "score_uncalibrated", *seg_cols]
        if c in eval_predictions.columns
    ))
```

並在模組 docstring 的欄位描述句補「＋配置的 `evaluation.segment_columns`（存在者）」。跑測試轉綠。

- [ ] **Step 12: Commit（keep_cols 段）＋回報**

```bash
git add src/recsys_tfb/diagnosis/metric/sample.py tests/test_diagnosis/test_metric/test_sample.py
git commit -m "feat(diagnosis): 診斷抽樣 keep_cols 補 segment 欄（pair_ledger by_segment 前置）"
```

回報格式：結論先行；三段 commit hash；驗收逐條附證據（測試輸出尾 3 行、mutation check 弄壞的行）；沒做到或不確定的事獨立一段。

---

### Task 3：`pair_ledger.py` 模組＋單元測試（implementer B）

**Files:**
- Create: `src/recsys_tfb/diagnosis/metric/pair_ledger.py`
- Test: `tests/test_diagnosis/test_metric/test_pair_ledger.py`

- [ ] **Step 1: 先寫已知答案測試（手算錨全部釘死）**

完整測試檔：

```python
"""pair_ledger 單元測試。

手算錨（設計定案）：query 排序 [B−, A+, C+]（rank 1..3）
- ΔAP(B→A)：a=1,b=2,P_a=0,P_b=1 → 1/1 − 1/2 = 0.5
- ΔAP(B→C)：a=1,b=3,P_a=0,P_b=2，中間正例 rank2 → 1/1 − 2/3 + 1/2 = 5/6
substitution 錨：B（全負，base_rate→clip→logit≈−27.6）沉底 →
q1 變 [A+, C+]、q2 變 [A+] → per-item A=1, C=1 → mAP=1.0。
"""
import numpy as np
import pandas as pd
import pytest

from recsys_tfb.diagnosis.metric.pair_ledger import (
    pair_ledger, substitution_ablation,
)


def _params(k=None, inject=None, segment_columns=None):
    return {
        "schema": {
            "columns": {
                "time": "snap_date",
                "entity": ["cust_id"],
                "item": "prod_name",
                "label": "label",
                "score": "score",
                "rank": "rank",
            }
        },
        "evaluation": {
            "metric": {"k": k},
            "segment_columns": segment_columns or [],
            "diagnosis": {
                "pair_ledger": {"enabled": True},
                "debug_inject_offsets": inject or {},
            },
        },
    }


def _sigmoid(x):
    return 1.0 / (1.0 + np.exp(-x))


def _ledger_pdf():
    """兩個 query 的手算 fixture（分數過 sigmoid 落 (0,1)，logit 還原名次）。

    q1（cust 1, seg X）：B− 0.9 > A+ 0.8 > C+ 0.7
    q2（cust 2, seg Y）：B− 0.9 > A+ 0.8 > C− 0.7
    已知答案：matrix B→A {2 pairs, 1.0}、B→C {1, 5/6}；n_mis=3；
    map_current = mean(A=(1/2+1/2)/2=0.5, C=2/3) = 7/12。
    """
    rows = [
        ("2026-01-31", 1, "B", 0, 0.9, "X"),
        ("2026-01-31", 1, "A", 1, 0.8, "X"),
        ("2026-01-31", 1, "C", 1, 0.7, "X"),
        ("2026-01-31", 2, "B", 0, 0.9, "Y"),
        ("2026-01-31", 2, "A", 1, 0.8, "Y"),
        ("2026-01-31", 2, "C", 0, 0.7, "Y"),
    ]
    return pd.DataFrame(
        rows,
        columns=["snap_date", "cust_id", "prod_name", "label", "score",
                 "seg"],
    )


class TestKnownAnswerMatrix:
    def test_pair_deltas_match_hand_computation(self):
        out = pair_ledger(_ledger_pdf(), _params())
        assert out["n_queries"] == 2
        assert out["n_pos_rows"] == 3
        assert out["n_mis_ordered_pairs"] == 3
        m = out["matrix"]
        assert m["B"]["A"]["pair_count"] == 2
        np.testing.assert_allclose(m["B"]["A"]["dap_sum"], 1.0)
        assert m["B"]["C"]["pair_count"] == 1
        np.testing.assert_allclose(m["B"]["C"]["dap_sum"], 5.0 / 6.0)
        assert list(m) == ["B"]  # 只有 B 當過壓制者

    def test_marginals_and_shares(self):
        out = pair_ledger(_ledger_pdf(), _params())
        sup = out["by_suppressor"]["B"]
        assert sup["pair_count"] == 3
        np.testing.assert_allclose(sup["dap_sum"], 11.0 / 6.0)
        np.testing.assert_allclose(sup["dap_share"], 1.0)
        vic = out["by_victim"]
        np.testing.assert_allclose(vic["A"]["dap_sum"], 1.0)
        np.testing.assert_allclose(vic["A"]["dap_share"], 6.0 / 11.0)
        np.testing.assert_allclose(vic["C"]["dap_share"], 5.0 / 11.0)

    def test_positive_item_above_is_not_a_suppressor(self):
        # q1 的 C+ 上方有 A+（正例）——不記帳（Bob 效應）。
        out = pair_ledger(_ledger_pdf(), _params())
        assert "A" not in out["by_suppressor"]

    def test_k_truncation_changes_ledger_currency(self):
        # k=1：每對 swap 都是「把正例抬進 top-1」→ ΔAP 恆 1.0。
        out = pair_ledger(_ledger_pdf(), _params(k=1))
        np.testing.assert_allclose(
            out["by_suppressor"]["B"]["dap_sum"], 3.0
        )


class TestSubstitution:
    def test_substituting_pure_negative_item_recovers_full_map(self):
        out = substitution_ablation(_ledger_pdf(), _params())
        np.testing.assert_allclose(out["map_current"], 7.0 / 12.0)
        sub_b = out["substitution"]["B"]
        assert sub_b["base_rate"] == 0.0
        np.testing.assert_allclose(sub_b["map_substituted"], 1.0)
        np.testing.assert_allclose(
            sub_b["delta_vs_current"], 5.0 / 12.0
        )  # 正值＝B 的個性化分數是淨傷害

    def test_umbrella_merges_substitution_block(self):
        out = pair_ledger(_ledger_pdf(), _params())
        assert "substitution" in out and "map_current" in out
        np.testing.assert_allclose(out["map_current"], 7.0 / 12.0)


class TestBySegment:
    def test_harm_grouped_by_segment(self):
        out = pair_ledger(
            _ledger_pdf(), _params(segment_columns=["seg"])
        )
        seg = out["by_segment"]["seg"]
        assert seg["X"]["n_pos_rows"] == 2
        assert seg["X"]["n_suppressed_pos_rows"] == 2
        np.testing.assert_allclose(seg["X"]["dap_sum"], 4.0 / 3.0)
        np.testing.assert_allclose(seg["X"]["dap_share"], 8.0 / 11.0)
        assert seg["Y"]["n_pos_rows"] == 1
        np.testing.assert_allclose(seg["Y"]["dap_sum"], 0.5)

    def test_missing_segment_column_noted_and_skipped(self):
        out = pair_ledger(
            _ledger_pdf(), _params(segment_columns=["nope"])
        )
        assert out["by_segment"] == {}
        assert any("nope" in n for n in out["notes"])


class TestInjection:
    def test_injection_creates_suppression_for_injected_item(self):
        base = pair_ledger(_ledger_pdf(), _params())
        assert "C" not in base["by_suppressor"]
        out = pair_ledger(_ledger_pdf(), _params(inject={"C": 5.0}))
        # C− 在 q2 被抬到頂 → 壓 A+；q1 的 C+ 抬頂不造成傷害。
        assert out["by_suppressor"]["C"]["pair_count"] == 1
        np.testing.assert_allclose(
            out["by_suppressor"]["C"]["dap_sum"], 2.0 / 3.0
        )
        assert out["injected_offsets"] == {"C": 5.0}
        assert any("debug_inject_offsets 生效" in n for n in out["notes"])

    def test_substitution_measures_post_injection_state(self):
        out = substitution_ablation(
            _ledger_pdf(), _params(inject={"C": 5.0})
        )
        # 注入後現狀：q1 [C+,B−,A+]→A=2/3、C=1；q2 [C−,B−,A+]→A=1/3
        # per-item A=(2/3+1/3)/2=1/2, C=1 → map_current=3/4
        np.testing.assert_allclose(out["map_current"], 0.75)


class TestMechanics:
    def test_deterministic_and_row_order_invariant(self):
        pdf = _ledger_pdf()
        shuffled = pdf.sample(frac=1.0, random_state=3).reset_index(drop=True)
        a = pair_ledger(pdf, _params(segment_columns=["seg"]))
        b = pair_ledger(shuffled, _params(segment_columns=["seg"]))
        assert a["matrix"] == b["matrix"]
        assert a["by_segment"] == b["by_segment"]
        np.testing.assert_allclose(a["map_current"], b["map_current"],
                                   rtol=1e-12)

    def test_empty_sample_returns_stub_shape(self):
        out = pair_ledger(_ledger_pdf().iloc[0:0], _params())
        assert out["n_queries"] == 0
        assert out["matrix"] == {} and out["substitution"] == {}
        assert out["map_current"] is None
        assert any("抽樣為空" in n for n in out["notes"])

    def test_score_outside_unit_interval_noted(self):
        pdf = _ledger_pdf()
        pdf["score"] = pdf["score"] * 10.0  # 超出 (0,1)
        out = pair_ledger(pdf, _params())
        assert any("略過 logit" in n for n in out["notes"])

    def test_metric_params_echoed(self):
        out = pair_ledger(_ledger_pdf(), _params(k=3))
        assert out["metric_params"]["k"] == 3
        assert out["score_col_used"] == "score"
```

（注意：fixture 的 score 是原始 (0,1) 分數，模組內 logit 變換單調、名次不變，手算錨直接成立。）

- [ ] **Step 2: 跑測試確認 FAIL（模組不存在）**

Run: `PYTHONPATH=src ... -m pytest tests/test_diagnosis/test_metric/test_pair_ledger.py -q`
Expected: collection error `No module named 'recsys_tfb.diagnosis.metric.pair_ledger'`。

- [ ] **Step 3: 實作模組**

完整內容：

```python
"""Pair ledger（壓制帳本，框架診斷項目 7；spec §3 Phase 4b）。

在診斷抽樣上（driver-side numpy）做兩件互補的傷害歸因會計：

- ``pair_ledger``（傘函數，node 只呼叫它）：對每個正例列，列舉同 query
  排其上方的**負例** item，記交換兩列名次會讓該 query 的 AP 貢獻總和變
  多少（|ΔAP|，lambdarank 的 λ 梯度定義——這裡只做會計、不訓練）→
  「壓制者 × 受害者」矩陣＋傷害 × segment 分組；並內含 substitution。
- ``substitution_ablation``：逐 item 把分數換成該 item base rate 的
  logit 常數、重算參數化 macro mAP（O(M) 次）→ 每 item 個性化分數的
  淨貢獻／淨傷害（delta_vs_current 負＝淨貢獻、正＝淨傷害）。

設計要點（計畫「設計定案」節的落地）：
- 排序與 ``positive_row_contributions`` 完全同款 lexsort；k 截斷語意
  跟 ``evaluation.metric.k`` 一致。
- |ΔAP| 是 query 層 AP 貢獻和的精確變化量，**不是** macro per-item mAP
  的全式變化（per-item 分母跨 query）——judgement 用相對量。
- 全樣本不切折：描述性會計、無擬合搜尋，無過擬合疑慮。
- 注入（debug_inject_offsets）語意與 offset_sweep 一致（_common 共用）：
  一切計算之前加到 logit 分數上，map_current＝注入後現狀。
- segment 欄由上游 join 進 eval_predictions，這裡只消費欄位（spec 明文
  邊界：不 import evaluation/segments.py）。
"""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from recsys_tfb.core.schema import get_schema
from recsys_tfb.diagnosis.metric._common import (
    _CLIP_EPS, apply_injection, metric_params, parse_injection, to_logit,
)
from recsys_tfb.evaluation.metrics import compute_macro_per_item_map

logger = logging.getLogger(__name__)


def _arrays(sample_pdf: pd.DataFrame, parameters: dict):
    """共用前處理：groups／items／y／z（含注入）＋notes。"""
    schema = get_schema(parameters)
    query_cols = [schema["time"], *schema["entity"]]
    notes: list[str] = []
    groups = (
        sample_pdf.groupby(query_cols, sort=False, dropna=False)
        .ngroup()
        .to_numpy()
    )
    items = sample_pdf[schema["item"]].astype(str).to_numpy()
    y = (sample_pdf[schema["label"]].to_numpy() == 1)
    z, z_notes = to_logit(sample_pdf[schema["score"]].to_numpy())
    notes.extend(z_notes)
    inject = parse_injection(parameters)
    z, inj_notes = apply_injection(z, items, inject)
    notes.extend(inj_notes)
    return groups, items, y, z, inject, schema, notes


def substitution_ablation(
    sample_pdf: pd.DataFrame, parameters: dict
) -> dict:
    mp = metric_params(parameters)
    out: dict = {"map_current": None, "substitution": {}, "notes": []}
    if len(sample_pdf) == 0:
        out["notes"].append("診斷抽樣為空——substitution ablation 未執行")
        return out
    groups, items, y, z, _inject, _schema, notes = _arrays(
        sample_pdf, parameters
    )
    out["notes"] = notes
    yf = y.astype(np.float64)
    map_current = float(
        compute_macro_per_item_map(groups, items, yf, z, **mp)
    )
    out["map_current"] = map_current
    for it in sorted(set(items.tolist())):
        mask = items == it
        base_rate = float(yf[mask].mean())
        p = min(max(base_rate, _CLIP_EPS), 1.0 - _CLIP_EPS)
        base_logit = float(np.log(p / (1.0 - p)))
        z_sub = z.copy()
        z_sub[mask] = base_logit
        m = float(compute_macro_per_item_map(groups, items, yf, z_sub, **mp))
        out["substitution"][it] = {
            "base_rate": base_rate,
            "base_logit": base_logit,
            "map_substituted": m,
            "delta_vs_current": m - map_current,
        }
    return out


def pair_ledger(sample_pdf: pd.DataFrame, parameters: dict) -> dict:
    schema = get_schema(parameters)
    mp = metric_params(parameters)
    k = mp["k"]
    seg_cols_cfg = list(
        (parameters.get("evaluation", {}) or {}).get("segment_columns", [])
        or []
    )
    notes: list[str] = []
    out: dict = {
        "enabled": True,
        "score_col_used": schema["score"],
        "metric_params": mp,
        "injected_offsets": {},
        "n_queries": 0,
        "n_pos_rows": 0,
        "n_mis_ordered_pairs": 0,
        "matrix": {},
        "by_suppressor": {},
        "by_victim": {},
        "map_current": None,
        "substitution": {},
        "by_segment": {},
        "notes": notes,
    }
    if len(sample_pdf) == 0:
        notes.append("診斷抽樣為空——pair ledger 未執行")
        return out

    groups, items, y, z, inject, _schema, arr_notes = _arrays(
        sample_pdf, parameters
    )
    notes.extend(arr_notes)
    out["injected_offsets"] = inject

    seg_cols = [c for c in seg_cols_cfg if c in sample_pdf.columns]
    missing = [c for c in seg_cols_cfg if c not in sample_pdf.columns]
    if missing:
        notes.append(f"segment 欄不在抽樣中，by_segment 略過：{missing}")

    # ---- pair 枚舉（與 positive_row_contributions 同款 lexsort）----
    sort_idx = np.lexsort((-z, groups))
    g_s = groups[sort_idx]
    y_s = y[sort_idx].astype(np.float64)
    it_s = items[sort_idx]
    boundaries = np.concatenate([
        [0], np.flatnonzero(np.diff(g_s)) + 1, [len(g_s)],
    ])

    pair_count: dict = {}
    dap_sum: dict = {}
    row_harm = np.zeros(len(sample_pdf), dtype=np.float64)
    row_suppressed = np.zeros(len(sample_pdf), dtype=bool)
    n_pairs = 0
    n_pos_rows = 0
    for qi in range(len(boundaries) - 1):
        s, e = boundaries[qi], boundaries[qi + 1]
        yq = y_s[s:e]
        n_pos_rows += int(yq.sum())
        if yq.sum() == 0:
            continue
        L = e - s
        ranks = np.arange(1, L + 1, dtype=np.float64)
        k_eff = float(k) if k is not None else float(L)
        cum = np.cumsum(yq)
        contrib = np.where(ranks <= k_eff, cum / ranks, 0.0)
        s_prefix = np.cumsum(
            np.where((yq == 1) & (ranks <= k_eff), 1.0 / ranks, 0.0)
        )
        pos_pos = np.flatnonzero(yq == 1)
        neg_pos = np.flatnonzero(yq == 0)
        for b in pos_pos:
            if b == 0:
                continue
            above = neg_pos[neg_pos < b]
            if len(above) == 0:
                continue
            a_rank = above + 1.0
            new_c = np.where(
                a_rank <= k_eff, (cum[above] + 1.0) / a_rank, 0.0
            )
            spill = s_prefix[b - 1] - s_prefix[above]
            dap = new_c - contrib[b] + spill
            victim = str(it_s[s + b])
            orig_row = sort_idx[s + b]
            row_harm[orig_row] += float(dap.sum())
            row_suppressed[orig_row] = True
            n_pairs += len(above)
            for j, d in zip(above, dap):
                key = (str(it_s[s + j]), victim)
                pair_count[key] = pair_count.get(key, 0) + 1
                dap_sum[key] = dap_sum.get(key, 0.0) + float(d)

    out["n_queries"] = int(len(boundaries) - 1)
    out["n_pos_rows"] = int(n_pos_rows)
    out["n_mis_ordered_pairs"] = int(n_pairs)

    total = float(sum(dap_sum.values()))
    matrix: dict = {}
    for (sup, vic), c in pair_count.items():
        matrix.setdefault(sup, {})[vic] = {
            "pair_count": c, "dap_sum": dap_sum[(sup, vic)],
        }
    out["matrix"] = {
        sup: dict(sorted(v.items())) for sup, v in sorted(matrix.items())
    }

    def _marginal(axis: int) -> dict:
        agg: dict = {}
        for key, c in pair_count.items():
            a = agg.setdefault(
                key[axis], {"pair_count": 0, "dap_sum": 0.0}
            )
            a["pair_count"] += c
            a["dap_sum"] += dap_sum[key]
        for a in agg.values():
            a["dap_share"] = (a["dap_sum"] / total) if total > 0 else None
        return {k_: agg[k_] for k_ in sorted(agg)}

    out["by_suppressor"] = _marginal(0)
    out["by_victim"] = _marginal(1)

    # ---- by_segment（傷害集中在誰身上）----
    pos_mask = y
    for c in seg_cols:
        raw = sample_pdf[c].to_numpy()
        vals = np.where(pd.isna(raw), "null", raw.astype(str))
        block: dict = {}
        for v in sorted(set(vals[pos_mask].tolist())):
            m = pos_mask & (vals == v)
            dsum = float(row_harm[m].sum())
            block[v] = {
                "n_pos_rows": int(m.sum()),
                "n_suppressed_pos_rows": int((m & row_suppressed).sum()),
                "dap_sum": dsum,
                "dap_share": (dsum / total) if total > 0 else None,
            }
        out["by_segment"][c] = block

    # ---- substitution（傘函數併入；notes 去重保序）----
    sub = substitution_ablation(sample_pdf, parameters)
    out["map_current"] = sub["map_current"]
    out["substitution"] = sub["substitution"]
    for n in sub["notes"]:
        if n not in notes:
            notes.append(n)

    logger.info(
        "pair ledger: %d queries, %d mis-ordered pairs, "
        "%d suppressors, map_current=%s",
        out["n_queries"], n_pairs, len(out["by_suppressor"]),
        out["map_current"],
    )
    return out
```

- [ ] **Step 4: 測試全綠**

Run: `PYTHONPATH=src ... -m pytest tests/test_diagnosis/test_metric/test_pair_ledger.py -q`
Expected: 全部 PASS。

- [ ] **Step 5: mutation check（證明測試真的覆蓋）**

臨時把 `spill = s_prefix[b - 1] - s_prefix[above]` 改成 `spill = 0.0` → `test_pair_deltas_match_hand_computation`（B→C 的 5/6 含 spill 1/2）轉紅；改回。再臨時把 `above = neg_pos[neg_pos < b]` 的 `neg_pos` 改成全列 → `test_positive_item_above_is_not_a_suppressor` 轉紅；改回。回報兩處。

- [ ] **Step 6: offset_sweep 測試回歸（確認 _common 共用未破壞）**

Run: `PYTHONPATH=src ... -m pytest tests/test_diagnosis/test_metric/ -q 2>&1 | tail -3`
Expected: 全綠。

- [ ] **Step 7: Commit**

```bash
git add src/recsys_tfb/diagnosis/metric/pair_ledger.py tests/test_diagnosis/test_metric/test_pair_ledger.py
git commit -m "feat(diagnosis): pair_ledger 壓制帳本＋substitution ablation＋by_segment（診斷項目 7）"
```

---

### Task 4：config ＋ consistency A19（implementer C，第一段）

**Files:**
- Modify: `conf/base/parameters_evaluation.yaml`（diagnosis 區塊＋report.sections）
- Modify: `src/recsys_tfb/core/consistency.py`
- Test: `tests/test_core/test_consistency.py`

- [ ] **Step 1: 先寫 A19 失敗測試**

沿 `test_consistency.py` 既有 A18 測試 class 的組織慣例，新增：

```python
class TestPairLedgerParams:  # A19
    def test_valid_config_passes(self):
        params = _valid_params()  # 沿本檔既有 helper
        params["evaluation"]["diagnosis"]["pair_ledger"] = {"enabled": True}
        assert pair_ledger_param_errors(params) == []

    def test_non_bool_enabled_rejected(self):
        params = _valid_params()
        params["evaluation"]["diagnosis"]["pair_ledger"] = {"enabled": "yes"}
        errs = pair_ledger_param_errors(params)
        assert len(errs) == 1
        assert "evaluation.diagnosis.pair_ledger.enabled" in errs[0]

    def test_missing_block_defaults_clean(self):
        params = _valid_params()
        params["evaluation"]["diagnosis"].pop("pair_ledger", None)
        assert pair_ledger_param_errors(params) == []
```

並在 collect-all 測試（若有 exact 清單）additive 加入 A19（預先授權）。跑確認 FAIL。

- [ ] **Step 2: 實作 A19**

`consistency.py`：`offset_sweep_param_errors` 之後新增：

```python
def pair_ledger_param_errors(parameters: dict) -> list[str]:
    """evaluation.diagnosis.pair_ledger domains (A19)."""
    errors: list[str] = []
    diag = ((parameters.get("evaluation", {}) or {})
            .get("diagnosis", {}) or {})
    cfg = diag.get("pair_ledger", {}) or {}
    en = cfg.get("enabled", True)
    if not isinstance(en, bool):
        errors.append(
            f"evaluation.diagnosis.pair_ledger.enabled={en!r} must be a "
            "bool"
        )
    return errors
```

註冊：`validate_config_consistency` 內 `errors.extend(offset_sweep_param_errors(parameters))` 之後加 `errors.extend(pair_ledger_param_errors(parameters))`。模組 docstring 的 Invariant legend 在 A18 之後加：

```
* A19 — ``evaluation.diagnosis.pair_ledger`` parameter domains:
  ``enabled`` must be a bool. Predicate: ``pair_ledger_param_errors``.
```

- [ ] **Step 3: config**

`conf/base/parameters_evaluation.yaml`——`debug_inject_offsets: {}` 之前（緊接 offset_sweep 區塊之後）插入：

```yaml
    # 壓制帳本（A19）：pair_ledger——對每個正例列列舉同 query 排其上方的
    # 負例 item，記交換名次的指標敏感度 |ΔAP|（λ 會計）→「壓制者×受害者」
    # 矩陣＋傷害×segment 分組（evaluation.segment_columns）；substitution
    # ablation 逐 item 換 base-rate logit 常數重算指標（O(M) 次）量淨貢
    # 獻/淨傷害。跑在同一份診斷抽樣上；描述性會計、全樣本不切折。
    pair_ledger:
      enabled: true
```

`debug_inject_offsets` 的註解「只影響 offset_sweep 節點」改為「只影響分流層節點（offset_sweep＋pair_ledger）」。`report.sections` 的 `offset_sweep: true` 之後加 `pair_ledger: true`。

- [ ] **Step 4: 測試綠＋commit**

```bash
PYTHONPATH=src ... -m pytest tests/test_core/test_consistency.py -q 2>&1 | tail -3
git add conf/base/parameters_evaluation.yaml src/recsys_tfb/core/consistency.py tests/test_core/test_consistency.py
git commit -m "feat(consistency): A19 pair_ledger 參數域＋config 區塊"
```

---

### Task 5：`compute_pair_ledger` 節點＋catalog＋pipeline 接線（implementer C，第二段）

**Files:**
- Modify: `src/recsys_tfb/pipelines/evaluation/nodes_spark.py`
- Modify: `src/recsys_tfb/pipelines/evaluation/pipeline.py`
- Modify: `conf/base/catalog.yaml`
- Test: `tests/test_pipelines/test_evaluation/test_pipeline.py`（結構斷言 additive 更新，預先授權）

- [ ] **Step 1: 先更新結構測試（9→10、12→13、名單、outputs）**

`test_pipeline.py`：node 數斷言 `== 9` → `== 10`（兩處）、`== 12` → `== 13`；node 名清單在 `compute_offset_sweep` 之後加 `"compute_pair_ledger"`；outputs 清單加 `"evaluation_pair_ledger"`。跑確認 FAIL（node 尚未加）。

- [ ] **Step 2: 節點（`compute_offset_sweep` 之後）**

```python
def compute_pair_ledger(
    eval_predictions: Optional[SparkDataFrame],
    parameters: dict,
) -> dict:
    """壓制帳本薄 node（spec §3 Phase 4b；框架診斷項目 7）。

    領域邏輯全在 ``diagnosis.metric.pair_ledger``（driver 端 numpy）。
    抽樣與其他診斷節點走同一套 ``draw_diagnosis_sample``（同 seed→內容
    相同；各自重抽、非共享快取，每次呼叫都是一趟 Spark 掃描）。
    停用時寫 stub。
    """
    eval_params = parameters.get("evaluation", {}) or {}
    cfg = ((eval_params.get("diagnosis", {}) or {})
           .get("pair_ledger", {}) or {})
    if not cfg.get("enabled", True):
        logger.info("pair ledger disabled — writing stub")
        return {"enabled": False}
    if eval_predictions is None:
        raise ValueError(
            "compute_pair_ledger: eval_predictions is required when "
            "evaluation.diagnosis.pair_ledger.enabled is true"
        )
    from recsys_tfb.diagnosis.metric.pair_ledger import pair_ledger
    from recsys_tfb.diagnosis.metric.sample import draw_diagnosis_sample

    sample_pdf, sample_meta = draw_diagnosis_sample(
        eval_predictions, parameters
    )
    out = pair_ledger(sample_pdf, parameters)
    out["sample"] = sample_meta
    logger.info(
        "pair ledger computed: %d mis-ordered pairs, %d suppressors, "
        "map_current=%s",
        out.get("n_mis_ordered_pairs", 0),
        len(out.get("by_suppressor", {})),
        out.get("map_current"),
    )
    return out
```

- [ ] **Step 3: pipeline 接線**

`pipeline.py`：`compute_offset_sweep` 的 Node 之後加：

```python
        Node(
            compute_pair_ledger,
            inputs=["eval_predictions", "parameters"],
            outputs="evaluation_pair_ledger",
        ),
```

（default 與 post_training 共用的 node 清單——與 `compute_offset_sweep` 同一處；import 區同步加名。）`generate_report` 的 inputs 清單加 `"evaluation_pair_ledger"`（尾端，與函數簽名順序一致——見 Task 6 Step 2）。

- [ ] **Step 4: catalog**

`conf/base/catalog.yaml`——`evaluation_offset_sweep` entry 之後：

```yaml
evaluation_pair_ledger:
  type: JSONDataset
  filepath: data/evaluation/${model_version}/${snap_date}/diagnosis/pair_ledger.json
```

- [ ] **Step 5: 測試綠＋commit**

```bash
PYTHONPATH=src ... -m pytest tests/test_pipelines/test_evaluation/ -q 2>&1 | tail -3
git add src/recsys_tfb/pipelines/evaluation/nodes_spark.py src/recsys_tfb/pipelines/evaluation/pipeline.py conf/base/catalog.yaml tests/test_pipelines/test_evaluation/test_pipeline.py
git commit -m "feat(evaluation): compute_pair_ledger 節點＋catalog 接線（10 節點）"
```

---

### Task 6：報表 section（heatmap）＋ assemble 接縫（implementer C，第三段）

**Files:**
- Modify: `src/recsys_tfb/evaluation/report_builder.py`
- Modify: `src/recsys_tfb/pipelines/evaluation/nodes_spark.py`（`generate_report` 尾參）
- Test: `tests/test_evaluation/test_report_builder.py`

- [ ] **Step 1: 先寫報表測試**

沿該檔 `build_offset_sweep_section` 測試的組織慣例，新增（fixture dict 直接用 Task 3 手算 fixture 的已知輸出形狀）：

```python
class TestPairLedgerSection:
    def _ledger(self):
        return {
            "enabled": True,
            "n_queries": 2, "n_pos_rows": 3, "n_mis_ordered_pairs": 3,
            "matrix": {"B": {"A": {"pair_count": 2, "dap_sum": 1.0},
                             "C": {"pair_count": 1, "dap_sum": 5 / 6}}},
            "by_suppressor": {"B": {"pair_count": 3, "dap_sum": 11 / 6,
                                    "dap_share": 1.0}},
            "by_victim": {"A": {"pair_count": 2, "dap_sum": 1.0,
                                "dap_share": 6 / 11},
                          "C": {"pair_count": 1, "dap_sum": 5 / 6,
                                "dap_share": 5 / 11}},
            "map_current": 7 / 12,
            "substitution": {"B": {"base_rate": 0.0, "base_logit": -27.6,
                                   "map_substituted": 1.0,
                                   "delta_vs_current": 5 / 12}},
            "by_segment": {"seg": {"X": {"n_pos_rows": 2,
                                         "n_suppressed_pos_rows": 2,
                                         "dap_sum": 4 / 3,
                                         "dap_share": 8 / 11}}},
            "notes": [],
        }

    def test_section_renders_heatmap_and_tables(self):
        sec = build_pair_ledger_section(self._ledger(), _params())
        assert sec is not None
        assert len(sec.figures) == 1
        assert len(sec.tables) == 3  # 壓制者邊際、substitution、by_segment

    def test_no_pairs_skips_figure_keeps_tables(self):
        ledger = self._ledger()
        ledger["n_mis_ordered_pairs"] = 0
        ledger["matrix"] = {}
        sec = build_pair_ledger_section(ledger, _params())
        assert sec is not None and sec.figures == []

    def test_disabled_or_section_off_returns_none(self):
        assert build_pair_ledger_section({"enabled": False}, _params()) is None
        assert build_pair_ledger_section(None, _params()) is None
        params_off = _params()
        params_off["evaluation"]["report"] = {
            "sections": {"pair_ledger": False}
        }
        assert build_pair_ledger_section(self._ledger(), params_off) is None

    def test_notes_appended_to_description(self):
        ledger = self._ledger()
        ledger["notes"] = ["某注意事項"]
        sec = build_pair_ledger_section(ledger, _params())
        assert "某注意事項" in sec.description
```

（`_params()` 沿該檔既有 helper；exact-set sections 斷言若有，additive 加 `pair_ledger`——預先授權。）跑確認 FAIL。

- [ ] **Step 2: 實作 section builder**

`report_builder.py`——`build_offset_sweep_section` 之後：

```python
def _pair_ledger_heatmap(ledger: dict) -> go.Figure | None:
    matrix = ledger.get("matrix") or {}
    if not matrix:
        return None
    suppressors = sorted(matrix)
    victims = sorted({v for row in matrix.values() for v in row})
    z = [[(matrix.get(s_, {}).get(v) or {}).get("dap_sum")
          for v in victims] for s_ in suppressors]
    counts = [[(matrix.get(s_, {}).get(v) or {}).get("pair_count", 0)
               for v in victims] for s_ in suppressors]
    fig = go.Figure(go.Heatmap(
        z=z, x=victims, y=suppressors, colorscale="Blues",
        customdata=counts,
        hovertemplate=("壓制者 %{y} → 受害者 %{x}<br>"
                       "|ΔAP| 總量 %{z:.4f}<br>"
                       "pair 數 %{customdata}<extra></extra>"),
        colorbar={"title": "|ΔAP| 總量"},
    ))
    fig.update_layout(
        title="壓制帳本：交換名次的指標敏感度 |ΔAP|（λ 會計）",
        xaxis_title="受害者（正例被壓的 item）",
        yaxis_title="壓制者（排上方的負例 item）",
    )
    return fig


def build_pair_ledger_section(
    ledger: dict | None, parameters: dict
) -> ReportSection | None:
    if not _section_on(parameters, "pair_ledger"):
        return None
    if not ledger or not ledger.get("enabled"):
        return None
    sup = ledger.get("by_suppressor", {}) or {}
    sup_tbl = pd.DataFrame(
        {c: [sup[it].get(c) for it in sup]
         for c in ["pair_count", "dap_sum", "dap_share"]},
        index=list(sup),
    ).sort_values("dap_sum", ascending=False) if sup else pd.DataFrame()
    subst = ledger.get("substitution", {}) or {}
    sub_tbl = pd.DataFrame(
        {c: [subst[it].get(c) for it in subst]
         for c in ["base_rate", "base_logit", "map_substituted",
                   "delta_vs_current"]},
        index=list(subst),
    ).sort_values("delta_vs_current", ascending=False) if subst else pd.DataFrame()
    seg_rows = []
    for col, block in (ledger.get("by_segment", {}) or {}).items():
        for val, st in block.items():
            seg_rows.append({"segment": f"{col}={val}", **st})
    seg_tbl = pd.DataFrame(seg_rows).set_index("segment") if seg_rows \
        else pd.DataFrame()
    fig = _pair_ledger_heatmap(ledger)
    desc = (
        "壓制帳本：誰的負例壓在誰的正例上方、交換名次會讓 query AP 變多少"
        "（|ΔAP|，λ 會計——記帳不訓練）。判讀順序：(1) 看壓制者邊際表，"
        "|ΔAP| 總量大的 item 是主要加害者，回象限表看它是否「水準偏高」；"
        "(2) substitution 表 delta_vs_current 為正＝把該 item 分數換成 "
        "base-rate 常數反而更好（個性化分數是淨傷害）、負＝淨貢獻；"
        "(3) by_segment 看傷害集中在哪群。完整判讀："
        "docs/pipelines/evaluation-diagnosis.md。"
    )
    if ledger.get("n_mis_ordered_pairs", 0) == 0:
        desc += "（本次抽樣無任何排錯 pair——矩陣為空，不畫圖。）"
    notes = ledger.get("notes") or []
    if notes:
        desc += "⚠ " + "／".join(notes)
    tables = [sup_tbl, sub_tbl, seg_tbl]
    table_titles = ["壓制者邊際（|ΔAP| 總量降冪）",
                    "Substitution ablation（淨傷害降冪）",
                    "傷害 × segment"]
    return ReportSection(
        title="壓制帳本 Pair ledger（誰壓了誰、代價多少）",
        description=desc,
        figures=[fig] if fig is not None else [],
        tables=tables,
        table_titles=table_titles,
    )
```

- [ ] **Step 3: assemble 接縫**

`assemble_report` 簽名尾端加 `pair_ledger: dict | None = None`；`candidates` 清單在 `build_offset_sweep_section(...)` 之後插 `build_pair_ledger_section(pair_ledger, parameters)`。`nodes_spark.py` 的 `generate_report` 簽名尾端加 `pair_ledger: Optional[dict] = None`、`assemble_report(...)` 呼叫尾端傳 `pair_ledger=pair_ledger`（inputs 已在 Task 5 Step 3 接上）。`build_glossary_section` 補三條（沿該函數既有格式）：`|ΔAP|`（交換一對名次讓該 query 的 AP 貢獻總和變多少；λ 會計，query-AP 粒度）、`壓制者／受害者`（同 query 排在正例上方的負例 item／被壓的正例 item）、`substitution ablation`（把某 item 分數換成 base-rate 常數重算指標；delta 正＝該 item 個性化分數是淨傷害）。

- [ ] **Step 4: 測試綠＋整包回歸＋commit**

```bash
PYTHONPATH=src ... -m pytest tests/test_evaluation/test_report_builder.py tests/test_pipelines/test_evaluation/ -q 2>&1 | tail -3
git add src/recsys_tfb/evaluation/report_builder.py src/recsys_tfb/pipelines/evaluation/nodes_spark.py tests/test_evaluation/test_report_builder.py
git commit -m "feat(report): pair_ledger section（heatmap＋三表）＋glossary"
```

回報格式（implementer C 三段共用）：結論先行；各段 commit hash；驗收逐條附證據；沒做到或不確定的事獨立一段。

---

### Task 7：合併審查（sonnet reviewer，**背景執行**，與 Task 8 並行）

**待審物**：`git diff <Task 1 時的 HEAD>..HEAD`（controller 填實際 SHA）＋新增測試檔。
**prompt 要點**（沿 30 §5 模板＋提速協議）：附 controller 綠燈證據（Task 2–6 各段測試輸出）；明令**只讀 diff＋只跑新增/變更的測試檔**（`test_offset_sweep.py`、`test_pair_ledger.py`、`test_sample.py`、`test_consistency.py`、`test_report_builder.py`、`test_pipeline.py`），不重跑全套；重點面向：(a) |ΔAP| 公式實作 vs 設計定案的手算錨；(b) hash 折別的確定性與 rescue 分支；(c) `_common.py` 平移是否真的行為不變（比對舊 code）；(d) 索引正確性（`sort_idx[s + b]` 映回原列、`s_prefix[b-1]` 邊界）；(e) 依賴白名單（diagnosis 無 pyspark/plotly import）。列出至少 3 個具體問題（附 檔案:行號 與失敗情境），找不到就逐項列出檢查過程；結尾 verdict PASS / PASS-with-nits / FAIL。

---

### Task 8：真跑閘門三狀態＋效能量測（controller 直跑；Task 7 審查在背景同時進行）

**Files:** 產出 `/tmp/phase4b_state_{A,B,C}/`；不改 code（除 config 注入的暫時編輯）。

- [ ] **Step 1: graphify rebuild（本階段 code 改動後）**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
.venv/bin/python -c "from graphify.watch import _rebuild_code; from pathlib import Path; _rebuild_code(Path('.'))"
```

- [ ] **Step 2: State A（乾淨態，新基準）**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
export SPARK_CONF_DIR=$PWD/conf/spark-local && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m recsys_tfb evaluation \
  --env local --model-version 6059dcef --post-training
```
（背景執行。）完成後：

```bash
mkdir -p /tmp/phase4b_state_A && \
cp data/evaluation/6059dcef/20260131/diagnosis/*.json /tmp/phase4b_state_A/ && \
for f in metric_ci reconciliation quadrant_summary; do \
  cmp /tmp/phase4b_json_before/$f.json /tmp/phase4b_state_A/$f.json && echo "$f BIT-IDENTICAL"; done
```
判準：
- **metric_ci／reconciliation／quadrant 三份與 Task 1 快照位元一致**（keep_cols 加欄的負控制）。若 metric_ci 有差 → 先懷疑 toPandas 列序改變牽動 bootstrap 位置碼（N1 同類議題），diff 內容、如實回報討論，**不硬套通過**。
- `offset_sweep.json` **合法地變**（N1 折別）：schema 鍵集不變；centered δ* 量級與舊值同向（fund_bond／fund_stock 正、exchange_usd 負——實際值記錄）。
- `pair_ledger.json` 新增：`n_mis_ordered_pairs > 0`、by_suppressor 非空、substitution 每 item 有值、by_segment 含 `cust_segment_typ`。記錄 top 壓制者與 ccard_ins 的 `pair_count`/`dap_sum`（state B 的比較基準）。
- 報表 `report.html` 出現壓制帳本 section（heatmap＋三表）。
- 效能：從 log 取 `compute_pair_ledger` 節點耗時，按列數外推公司規模（4.4M 列），寫給 Task 9 素材包。

- [ ] **Step 3: State B（注入）**

編輯 `conf/base/parameters_evaluation.yaml`：`debug_inject_offsets: {ccard_ins: 1.0}`。重跑同指令（背景）。完成後快照到 `/tmp/phase4b_state_B/`，判準：
- pair_ledger：`by_suppressor["ccard_ins"]` 的 pair_count／dap_sum 相對 state A **暴增**（記實際倍率）。
- offset_sweep：ccard_ins 的 **centered 位移**（state B centered − state A centered）∈ [−1.35, −0.65]。
- metric_ci／reconciliation／quadrant 與 state A **位元一致**（注入 scope 負控制）。

- [ ] **Step 4: State C（還原）**

還原 `debug_inject_offsets: {}`（`git diff` 確認 config 乾淨）。重跑（背景）。判準：**五份 JSON 全部與 state A 位元一致**（`cmp` 逐一）。

- [ ] **Step 5: 素材包（給 Task 9）**

把 state A 的 pair_ledger.json 關鍵表（by_suppressor 前幾名、substitution 全表、by_segment）、注入前後對照數字、效能數字、**N1 之後的 offset_sweep 新示例數字（§10.6 重生用）** 整理到 `/tmp/phase4b_doc_materials.md`。

---

### Task 9：判讀手冊擴充（writer＋讀者 agent；素材包由 controller 從 Task 8 備好）

**Files:**
- Modify: `docs/pipelines/evaluation-diagnosis.md`

**契約**（spec §3 固定結構＋寫法鐵則）：
- 新 **§11 壓制帳本（pair ledger）**，子節比照 §10 規格：11.1 在回答什麼（與象限層 `suppression_count` 的關係：那邊記**次數**、這邊記**指標幣值**——同一件事的兩種計價）；11.2 |ΔAP| 的定義與數感（**把設計定案的 3 列手算例完整走一遍**：[B−, A+, C+] → 0.5 與 5/6，含 k=1 時變 1.0 的對照）；11.3 矩陣與邊際表讀法；11.4 substitution ablation 讀法（delta 正負號語意、base rate 取自診斷抽樣、只做「換掉 j」方向的誠實限制）；11.5 by_segment 讀法（傷害集中在誰身上；cohort 自動偵測的降級版）；11.6 示例走讀（**真跑素材印進文件**）；11.7 與 offset sweep 的關係（分流閥給水準缺口總量、帳本給「誰壓誰」明細；兩者都吃注入）。
- **§10.6 示例表數字重生**（N1 折別改變 δ*/LOO——用 Task 8 state A 新數字整表替換；§10.4 若引用具體收復量一併對齊）。§10 內提及折切機制處補一句「折別＝query key 的 hash 分桶（列序無關），比例為近似值」。
- 原 §11 已知限制 → **改號 §12**（維持檔尾），新增條目：λ 會計是 query-AP 粒度非 macro 全式；substitution 只做「換掉 j」方向；base rate 取自診斷抽樣非全量；hash 折別比例是近似值；pair 枚舉／substitution 的公司規模成本估計（Task 8 實測外推）。
- glossary（手冊內與報表 glossary 一致）：|ΔAP|、壓制者/受害者、substitution ablation、（若 §10 區已有 δ* 條目則不重複）。
- roadmap／導言若列了各層清單，additive 加壓制帳本一行。
- **寫法鐵則**：手冊禁用開發詞彙（Phase、task、spec、N1…），交付前 grep；無直覺尺度建數感節；報表 description 只留短判讀順序＋指向手冊。
- **交付前派 fresh 讀者 agent**：驗證清單含「列出所有指涉你看不到的東西的詞」「照 §11 能否從 pair_ledger.json 自行算出一格 |ΔAP|」；讀者問題全修完才 commit。

```bash
git add docs/pipelines/evaluation-diagnosis.md
git commit -m "docs: 判讀手冊 §11 壓制帳本＋§10.6 示例數字重生（hash 折別）＋§12 已知限制改號"
```

---

### Task 10：opus 總審＋nit 修復（**背景執行**）

**待審物**：本階段全部 diff（`git diff <Task 1 SHA>..HEAD`）＋三狀態閘門證據（Task 8 判準逐條與 `/tmp/phase4b_state_*` 路徑）＋手冊 diff。
**重點**：閘門證據核驗（不重跑，驗 cmp 輸出與數字合理性）、跨檔一致性（輸出 dict 鍵名 vs 報表 vs 手冊 vs config 註解逐字對齊）、|ΔAP| 公式與手算錨的數學正確性、依賴白名單、spec §3 Phase 4 驗收條的對應。列至少 3 個具體問題或逐項列出檢查過程；verdict READY / READY-with-nits / NOT-READY。nit 屬低成本者當下修（20 §5），修完重跑受影響測試檔。

---

### Task 11：使用者閘門

彙報（檔案引用一律絕對路徑）：
- 三狀態閘門結果（A 新基準數字、B 注入暴增倍率與 centered 位移、C 位元復原證據）。
- N1 修復說明（折別為何變、§10.6 已重生、公司叢集重跑穩定性的預期改善）。
- 報表新 section 截圖或路徑＋手冊 §11 位置。
- 測試總數變化（baseline 259 → 新數字）。
- 效能實測與公司規模外推。
- 沒做的事（明說）：substitution 反向（只留 j）、cohort 自動偵測（v2）、λ 會計的 macro 全式化。
使用者檢視通過 → 更新 HANDOFF＋memory，進 Phase 5 規劃。**還原措施若使用者未過**：注入 config 已還原、全部改動在 feat/diag-framework 上，不影響 main。

---

## Self-review（writing-plans skill 要求，寫完計畫後 controller 自查）

- [x] Spec 覆蓋：spec §3 Phase 4 pair_ledger 段三個交付物（pair_ledger／substitution_ablation／by_segment）＋config＋catalog＋報表＋A19＋驗收 1 後半（壓制暴增）＝Task 3/4/5/6/8；spec 驗收 2 修訂版（位元復原）＝Task 8 State C。HANDOFF 開工提醒 8 條：N1（Task 2）、keep_cols（Task 2）、注入共用 helper 拍板（設計定案＋Task 2）、效能估算（設計定案＋Task 8）、文件 task（Task 9）、A19（Task 4）、閘門指令（Task 8）、遺留取捨 N2/N3 不在範圍（如實列於 Task 11「沒做的事」之外——它們已記錄非義務）。
- [x] Placeholder 掃描：無 TBD／「適當處理」；所有 code 步驟附完整程式碼；兩處「沿既有 fixture 慣例」（test_sample.py、test_consistency.py 的 helper 名）為預先授權的對齊，非 placeholder（Phase 4a 同款先例）。
- [x] 型別／鍵名一致性：`pair_ledger` 輸出鍵在 Task 3 模組、Task 6 報表測試 fixture、Task 6 builder、Task 9 手冊契約四處逐字一致（`pair_count/dap_sum/dap_share/base_rate/base_logit/map_substituted/delta_vs_current/n_mis_ordered_pairs`）；`_common.py` 五個函數名在 Task 2/3 一致；手算錨 0.5、5/6、7/12、5/12、2/3、8/11 經兩次獨立推導核過。
