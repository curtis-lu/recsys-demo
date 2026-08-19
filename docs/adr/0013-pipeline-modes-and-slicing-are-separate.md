---
status: accepted
date: 2026-08-13
---

# 「少跑一些節點」有兩個機制：模式決定形狀，切片決定接續點

[ADR-0012](0012-month-aware-slicing-not-per-artifact-skip.md) 決定用**切片**來做「加評估月份時只跑 test 鏈」，並把那支旗標設計成一個具名切片 preset：硬寫兩個節點名（終點 `filter_test_model_input` ＋ 資料閘），其餘由 DAG 反推。

那個設計實作完成、通過驗收、也通過兩軸審查之後**被推翻**。本 ADR 記錄推翻的決定與理由，以及它揭露的那個更一般的分野。

ADR-0012 的**收邊條件**部分（`_can_load` 對月份感知）不受影響，仍然成立——被推翻的只有「這支旗標用切片做」。

## 決定

**`--only-test-months` 是 `create_pipeline` 的模式參數，不是切片。**

它保留的節點就是一份明寫的清單（`ONLY_TEST_MONTHS_NODES`，`pipelines/dataset/pipeline.py`）：

```python
ONLY_TEST_MONTHS_NODES = (
    "validate_data_consistency",   # 零輸出的資料閘：切片永遠拉不回來，只有點名進得來
    "select_test_keys",
    "apply_preprocessor_to_features",
    "build_test_model_input",
    "filter_test_model_input",
)
```

於是「少跑一些節點」在本 repo 有兩個彼此正交的機制：

| | **模式** | **切片** |
|---|---|---|
| 誰決定節點集 | `create_pipeline(**kwargs)`——旗標決定要組出哪些 node | `--from-node` / `--only-node`——對已組好的 pipeline 取子集 |
| 語意 | 這次要跑**哪一條動線** | 這次要**從哪裡接續** |
| 缺料時 | 照常 `catalog.load()`，缺了就 raise | `can_load` 判斷後自動補跑上游 |
| 零輸出節點 | 在清單裡就會跑 | 一定被跳過 |
| 節點集怎麼來 | 明確列出，寫在 `create_pipeline` 旁邊 | 由起點 ＋ DAG 反推 |

兩者**可組合**：`--only-test-months --only-node X` ＝ 先組出短 pipeline，再對它切片。

## 為什麼推翻

### 一、repo 已經有同形前例，而 ADR-0012 沒有查

`pipelines/evaluation/pipeline.py` 的 `compare_only` 就是「CLI 旗標 → `create_pipeline` 早退 → 回傳短 pipeline」，服務的是同一種需求（一支旗標宣告「這次只跑這條動線」）。

本次之前，`create_pipeline` 的模式參數在本 repo 已經有五個（`evaluation` 三個：`post_training` / `compare_source` / `compare_only`；`dataset` 與 `training` 各一個 `enable_calibration`），而切片入口只有兩個、四個 pipeline 共用。ADR-0012 的 preset 設計等於為既有機制已覆蓋的需求發明第二套。

### 二、preset 要讀懂得先跑一段推理，而那段推理本身就是成本

preset 只寫兩個節點名這件事，需要一整段關於 producer map 的推理才讀得懂：擴張只沿 `node.outputs` 建的 producer map 走 → 零輸出的節點永遠不會被自動拉回 → 所以資料閘必須被顯式列入 → 所以常數裡是「一個終點 ＋ 一個例外」而不是「一組同類的節點」。

**那段推理是正確的**（ADR-0012 的「後果」第一條寫得沒錯），但它的存在本身就是成本：讀者看到的是一個看似同質、實則混了兩種機制的 tuple。模式分支不需要這段推理——清單就是清單，資料閘只是裡面的一個名字。

## 這推翻了 ADR-0012 的哪些具體條目

- **「考慮過但否決的選項 → preset 硬寫 test 鏈的節點清單」**。當時的否決理由是「日後鏈上多一個節點時會安靜地漏掉」。理由本身成立，但**代價被高估了**：清單寫在 `create_pipeline` 裡、距離節點定義二十行，而漏掉的那一刻有一條測試會紅（把清單比對「從終點推導出的集合」）。用一條測試換掉一整段機制與它的解釋成本，划算。
- **「後果 → 資料閘要顯式進 preset」**、**「後果 → 『test 鏈是哪些節點』成為單一常數」**。兩者都是 preset 機制的衍生需求，機制沒了就不存在。資料閘現在只是清單的第一個成員。
- **「決定」末句「內部等價於 `--only-node filter_test_model_input` 再加上資料閘」**。不再等價：模式不做上游擴張。
- **「命名」段落裡「`--only-*` 家族自帶『這是切片、與 `--from-node`／`--only-node` 互斥』的提示」**。旗標名沿用，但語意相反——它與切片**正交、可組合**。命名本身仍然成立（不用 `--incremental-*` 的理由沒變）。
- **「這同時關掉一個靜默缺陷」** 一節說 `--only-node filter_test_model_input` 是「本 ADR 的 preset 目標節點」。該缺陷與其修法（月份感知 `can_load`）**不受影響、仍然必要**，但它現在守的是**手動切片**這條次要動線，不再是加月份的建議動線。

## 後果

- **上游缺料從「自動補跑」變成「當場 raise」。** 模式不做擴張，所以 `base_dataset_version` 翻號、`preprocessor` 不存在時，`apply_preprocessor_to_features` 會在 `catalog.load()` 失敗。**接受，而且認為更好**：那一刻的正確答案就是「這次執行不是『只加評估月份』」，fail-loud 比默默重建一個只有 test 產物的半套版本目錄好。
- **月份感知的收邊條件不再服務主動線。** ADR-0012 修的靜默缺陷（切片停在一跳之前、什麼都沒寫卻宣稱做了）仍然真實，`--from-node` / `--only-node` 對 dataset 也仍是有文件、有契約的動線（契約清單＝`RESUME_CONTRACTS`，`tests/test_pipelines/test_resume_contracts.py`）——但它從「加月份的建議做法」降為「手動接續時的保護」。**這不構成撤回 ADR-0012 的理由**，只是重要性的重新定位。
- **清單需要一條防漂移測試**，這是選擇「列出」而非「推導」唯一的實質代價。測試把清單比對「從 `filter_test_model_input` 以月份感知 `can_load` 推導出的集合」＋ 資料閘（`tests/test_pipelines/test_dataset/test_pipeline.py` 的 `TestOnlyTestMonthsMode`）。
- **清單少一個名字必須 fail loud。** 節點改名後若默默從清單過濾中掉出去，模式就悄悄變成更短的 pipeline——而 dataset 寫不出任何東西仍然 exit 0，正是 ADR-0012 開頭那個失效模式。所以 `_keep_named` 對找不到的名字 `raise`，不是「過濾到什麼算什麼」。
- **模式分支不得重新宣告 `Node(...)`。** 稽核測試以 AST 掃 `pipelines/**/*.py` 的全部 `Node(` 呼叫（含 `if` 分支內），重寫一次資料閘會讓 A7 的零輸出登記從 1 筆變 2 筆。（此處的 A7 是 `docs/agents/architecture-constraints.md` 的**結構約束**編號，與 `core/consistency.py` 的 A7 不是同一套——該檔 §「兩個 A 系列不是同一套編號」有說明。）所以短 pipeline 由既有 node list **過濾**而來。這條約束不是外加的麻煩——它逼出來的形狀正好是唯一一份節點宣告。

### 順手修掉的一個獨立缺陷：`--rebuild-dates` 的警語條件

與模式無關，但同一輪一起改了：警語條件從「有沒有帶切片旗標」改成「**切片實際丟掉了節點**」（`kept < total`，`__main__.py`）。舊條件下，`--from-node <第一個節點>` 選中 15 of 15、一個都沒被排除，卻仍宣稱「未被選中的上游不會刷新」，而它建議的補救動作跑出來逐位元相同。

## 這件事的一般形狀，值得記下來

推翻的成本不高（實作兩天、丟掉一天），但**它本來可以是零**：`compare_only` 一直都在，`grep -rn "def create_pipeline" src/` 三秒就會看到那些模式參數。ADR-0012 花了大量篇幅論證「為什麼是執行層不是產物層」「為什麼是 `_can_load` 不是 `exists()`」——兩個都是精緻且正確的分層論證——卻沒有先問**「這個需求在這個 repo 裡有沒有既有的做法」**。

判準：設計一個新機制之前，先找同一個 repo 裡**形狀相同的既有需求**是怎麼解的。找到了就沿用，沒找到才設計。這比任何分層論證都先發生，而且便宜得多。
