---
status: accepted
date: 2026-09-16
---

# 特徵表可以多張、各自宣告 join 欄位；「往回算」留在使用者 SQL，框架不檢查偷看

> **更正（2026-09-22，ADR-0026）**：決定 2、3 沿用；下面兩處以 ADR-0026 為準。
> - 決定 1 不是「多張、每張宣告自己的 join 欄位」：特徵表照接的鍵分兩類——entity 層級（`feature_table`，以 base key 接，恰好一張）與候選層級（catalog 條目 `candidate_feature_table`，以 identity 接，最多一張，抽樣後才讀、不另存）。
> - 決定 4 的觸發條件是「宣告了候選層級特徵表」。

## 背景

設計第二種使用情境（線上廣告推薦的離線訓練與評估：一列是一次曝光，label 是有沒有點）時，特徵出現三種不同的一列粒度：

| 特徵 | 一列是 | 怎麼接到曝光列上 |
|---|---|---|
| 每日批次特徵 | 一個 entity 一天的快照 | 照曝光當下「拿得到的那一份」快照 |
| 即時特徵 | 一次曝光 | 照 identity（含 `event`，ADR-0021） |
| item 的屬性 | 一個 item，或拼成 item 的其中一個屬性（item 恆為一欄，多屬性在來源 SQL 拼起來——見 `CONTEXT.md`） | 照它自己的識別欄 |

現況只容得下第一種的一個特例：dataset pipeline 只收一張 `feature_table`，一律以 `time + entity` 等值 join（`pipelines/dataset/steps/model_input.py::join_features_missing_as_null`、`docs/pipelines/dataset.md` §5）；前處理在抽樣之前對整張表做（`apply_preprocessor_to_features`）。repo 裡沒有任何 as-of（照時間往回找最近一份）的 join。

另外有兩個時間條件，讓「照 `time` 等值 join」本身就不對：

- 每日快照要到某個時點才產出，一大早的曝光在線上只拿得到前一天之前的那份；照曝光的 `time` 等值接，會接到線上當時還拿不到的快照。
- 即時特徵要算到曝光那一刻為止，多算到之後的行為就是偷看答案。

## 決定

1. **特徵表可以多張，每張宣告自己的 join 欄位。** 沒宣告時等同現在的單一 `feature_table` 以 `time + entity` join，行為與版本號不變。
2. **「往回算」全部在使用者的來源 SQL 做。** SQL 在每一列候選上先算好 join 需要的欄（例如「該用哪一天的快照」），dataset 仍然只做等值 join。
3. **框架不檢查「特徵有沒有算到曝光之後」。** 改在 `docs/pipelines/source_etl.md` 寫清楚這是來源 SQL 的責任，並附範例與自我檢查清單。

   本決定**不廢止**既有的延後項 B2（`core/consistency.py` 的 Invariant legend：「label-window leakage columns reach features (specified but DEFERRED)」，在等排程）。兩者擋的不是同一件事：B2 擋「label 觀察窗算出來的欄位被當成特徵」，本決定講的是「特徵的時間對齊」。B2 的去留不在本輪範圍。
4. **離線推論遇到多張特徵表時，在 CLI 入口直接擋下。** `inference` pipeline 只會接一張 `feature_table`，其他張會被漏掉。

## 考慮過、沒選的做法

**合成一張「一次曝光一列」的寬表交給 dataset。** dataset 幾乎不用改，但每日特徵要抄進每一次曝光；而且前處理在抽樣之前做，連之後會被抽掉的大量負例也要處理。`inference` 刻意不把 entity 特徵展開成每個 item 一份，是同一個理由（`docs/pipelines/inference.md` §5.1）。

**把 as-of join 放進框架。** 使用者給快照表、行為紀錄與可用時間，框架負責往回找。「不偷看」可以由框架保證，但即時特徵的算法（過去 N 分鐘點了幾次、最後一次點了什麼……）每個部署都不同，框架只會長成一堆特例；而且要在不准 UDF 的限制下實作。

**加一條「算到幾點」的檢查。** 每張特徵表附「算到幾點」欄、每列候選附事件時間，框架比大小。但「算到幾點」是 SQL 自己填的，這條只擋得住忘了寫，擋不住寫錯。ADR-0017 決定二用過同一個判準：加一條擋不到目標情境的不變量，只會讓下一個人以為這件事已經有人守著。

## 後果

- 訓練與線上評分的特徵算法是兩份程式碼，「算得不一樣」的風險由部署承擔。training 的交接包（特徵順序、類別編號、特徵表清單與 join 欄位）**尚未實作**（見 `docs/notes/2026-09-16-event-support-plan.md` 的 P8）；就算做出來，它也只擋得住順序與編號對不上，擋不住算法寫錯。
- dataset 收多張特徵表要動的地方，至少包括：catalog 條目、前處理的 fit 與 apply、`build_*_model_input` 的 join、粒度閘 B10、feature_table 型別檢查、CLI 計算 `base_dataset_version` 時的 feature_table fingerprint、`core/consistency.py` 的 `DATASET_SOURCE_TABLES`。
