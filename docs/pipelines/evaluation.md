# evaluation pipeline

> 把預測 ⋈ label 算排序指標、產報表。兩個情境（訓練後 / 上線監控）、三個模式（標準 / 比較 / 只比較）。
> DAG pipeline；節點接線見 [`../data-lineage.html`](../data-lineage.html)。

## 指令與選項

```bash
# 情境1：訓練後評估（讀 training_eval_predictions）
python -m recsys_tfb evaluation --env local --post-training

# 情境2：上線監控（讀 ranked_predictions，預設）
python -m recsys_tfb evaluation --env local

# 指定要評估哪一版（Model A；省略依解析規則取對應版本）
python -m recsys_tfb evaluation --model-version <model_version>

# 加比較（同時產標準報表與 report_comparison.html）
python -m recsys_tfb evaluation --post-training --compare <key>

# 只出比較報表（前提：該版已用標準/--compare 跑過、persist 過 enriched_eval_predictions）
python -m recsys_tfb evaluation --compare-only <key>

# 先看執行計畫不跑 / 列 node 名與接續成本
python -m recsys_tfb evaluation --from-node compute_metrics --dry-run
python -m recsys_tfb evaluation --list-nodes
```

> `<key>` 取自 `parameters_evaluation.yaml` 的 `compare_sources`；`--compare` / `--compare-only` 互斥（只能給一個）。`--from-node` / `--only-node` 互斥；切片機制 → [`../operations/pipeline-slicing.md`](../operations/pipeline-slicing.md)。

## 用途

`evaluation` 對「預測 ⋈ `label_table`」算 per query group 的排序指標（mAP / NDCG…），對照 popularity baseline，產 HTML 報表，並把 enrich 後的預測寫回 Hive 供後續比較。

## 兩個情境

評估**永遠**是 per query group 的排序指標（query group ＝ 同一個 (time, entity) 下所有候選 item，指標每組各算再平均，見 README §0），差別只在讀哪份預測：

| 情境 | 指令 | 讀什麼 | 何時用 |
|---|---|---|---|
| 訓練後評估 | `evaluation --post-training` | `training_eval_predictions`（test set，training 產） | 剛訓完看這版在 test 的排序表現 |
| 上線後監控 | `evaluation`（預設） | `ranked_predictions`（inference 發布的已驗證結果） | 模型上線後定期追蹤排名品質 |

> 兩情境都靠 `label_table` 提供 ground truth，所以要等**該 snap_date 的 label 認定窗過完、label 補齊**後才算得出指標（上線當下通常還沒有 label，需等觀察窗結束）。

## 三個模式（節點流程）

`prepare_eval_data` 先把預測 ⋈ `label_table`、加 rank 與 segment enrichment，得 `eval_predictions`（in-memory），其餘節點接在後面：

**標準（無 compare flag）**

| 節點 | 做什麼 | 產出 |
|---|---|---|
| `prepare_eval_data` | 預測 ⋈ label，再 enrich（把名次 rank 與分群欄 join 進每筆預測） | `eval_predictions` |
| `compute_metrics` | 排序指標（mAP / NDCG…） | （in-memory） |
| `compute_baseline_metrics` | popularity baseline（以整體最常被選的 item 當「不看個人特徵」的基準線）對照 | （in-memory） |
| `generate_report` | 產報表 | `evaluation_report`（report.html） |
| `persist_eval_predictions` | 寫回 Hive 供後續比較 | `enriched_eval_predictions` |

**`--compare X`**：在標準流程後加 `load_compare_predictions` → `restrict_to_common` → `generate_comparison_report`（同時產標準報表與比較報表 `report_comparison.html`）。

**`--compare-only X`**（前提：該 model_version 必須**已用標準或 `--compare` 模式跑過、persist 過 `enriched_eval_predictions`**）：短流程，不重算指標——`validate_enriched_eval_predictions_present`（讀回先前的 `enriched_eval_predictions`、檢查該 partition 存在）→ `load_compare_predictions` → `restrict_to_common` → `generate_comparison_report`，**只**產比較報表。

## 關鍵設定（`conf/base/parameters_evaluation.yaml`）

- **指標**：k 值、要算哪些指標。指標怎麼算、報表怎麼讀 → [`../metrics.html`](../metrics.html)。
- **分群報表** `segment_columns` ＋ `segment_sources`：每個要分群的欄都要有對應的 segment 來源，否則該段報表悄悄不出現（被一致性閘擋，README §4）。
- **多模型比較** `compare_sources`：把本模型與其他來源在共同客戶上對比。每個來源一個 `kind`：

  | kind | 指向 | 需要的子設定 |
  |---|---|---|
  | `model_version` | 同框架的另一個 model_version | `model_version` |
  | `external_hive` | 外部 Hive 表 | `table`、`columns` 映射、`prod_mapping`、`unmapped_policy` |

  `--compare X` 與 `--compare-only X` 的 `X` 必須是 `compare_sources` 裡的 key，且兩者互斥（只能給一個）。設定不合法會被一致性閘擋（README §4）。

## 重跑語意

- `--model-version`：評估哪一版（預設依解析規則取對應版本）。
- 標準模式每次重算指標並覆寫 `enriched_eval_predictions`（dynamic partition，按 `model_version` ＋ `snap_date`）。
- `--compare-only` **不**重算，直接讀回上次 persist 的 `enriched_eval_predictions`；若 `segment_sources` 在兩次 run 間改變，需先 drop 該表再重跑（schema evolution 是待辦）。

## 接下來

- 指標定義與報表分段 → [`../metrics.html`](../metrics.html)
- 預測從哪來（inference / training）→ [`../data-lineage.html`](../data-lineage.html)、[`training.md`](training.md)
- 比較 / 分群設定的錯誤訊息 → README §4
