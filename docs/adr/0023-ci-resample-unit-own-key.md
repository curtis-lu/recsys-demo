---
status: accepted
date: 2026-09-16
---

# 信賴區間的重抽單位用獨立設定，不沿用 `val_sample_keys`

## 背景

evaluation 診斷的 cluster bootstrap 信賴區間，要把「彼此相關的列」當成一群一起重抽。現行程式拿 `schema.entity` 的**全部欄位**當一群（`diagnosis/metric/uncertainty.py`，`cluster_key` 那一行）；`diagnosis/metric/item_ability/_compute.py` 與 `diagnosis/metric/config_shift/_compute.py` 也用同樣的方式定義 cluster。但程式註解寫「cluster id（entity only）」、`conf/base/parameters_evaluation.yaml` 的註解寫「cluster＝cust_id」——原意是「以客戶為單位」。

`entity` 只有一欄時兩者等價，所以沒出過事。`entity` 多欄時就不等價了——多欄 entity 的部署是把「替誰排序」拆成兩個層級，例如 ADR-0021 那個第二種使用情境（線上廣告推薦的離線訓練與評估）用 `[客戶, 版位]`：同一位客戶在不同版位的曝光是兩個 entity 值，但顯然不是兩個互不相干的人。這時同一位客戶的不同版位被當成互不相干的群，重抽出來的區間偏窄，看起來比實際更有把握。最壞的情況是同一個人在 m 個層級上的行為完全一樣，標準誤被低估約 √m 倍。

這和 ADR-0016 是同一類問題：以 entity 為單位的操作，在多欄 entity 下單位悄悄變了，而且不會報錯。

## 決定

evaluation 新增一個設定鍵，宣告重抽單位：

- 合法值是 `schema.entity` 的非空子集，檢查方式照 ADR-0016 的不變量 A29（`core/consistency.py::entity_grouping_key_errors`）。
- 預設為完整 `schema.entity`。
- 上面三個用 cluster 的診斷都讀它。
- **鍵必須落在 `evaluation.diagnosis.*` 底下**，否則要同時登記進 `pipelines/evaluation/steps/config_fingerprint.py` 的 `COMPUTED_KEYS`。理由是 ADR-0020 決定二把設定指紋做成封閉列舉，而 `("evaluation.diagnosis", "draw_diagnosis_sample_node")` 已經在裡面：鍵放在別層又沒登記的話，改了它、已落地的區間指紋不動，`--only-node generate_report` 會沿用舊區間而且退出碼 0——正是 ADR-0020 要消滅的形態。鍵名本身在實作時定。

## 第四處 cluster：只計數，本輪不改

`diagnosis/metric/suppression/_compute.py:338` 也用同一個方式算 `clusters`，但它不是 bootstrap 的重抽單位，只用來在 `:709` 輸出 `n_entities`。本輪**不**改它，代價是：多欄 entity 的部署會看到一個 `n_entities`，它數的是 (客戶, 版位) 的組合數，和宣告的重抽單位不是同一個單位。實作這張票時要在壓制診斷的報表說明裡寫清楚這一點，否則兩份診斷的「有幾個 entity」會互相打架。

## 為什麼不直接沿用 `val_sample_keys`

語意很像，都是「以 entity 的哪幾欄為單位」。但 `val_sample_keys` 刻意算進 `base_dataset_version`（`core/versioning.py` 在 `TRAIN_SAMPLING_KEYS` 上方的註解）。沿用它的話，只想換信賴區間的算法，就得把整批 dataset 產物重算。

## 為什麼預設是完整 entity，而不是修成「第一欄」

- 預設成完整 entity，現有部署的行為與輸出都不變；示例部署的 `entity` 是單欄，不受影響。
- 預設成第一欄，等於把「第一欄剛好是客戶」這個示例部署的巧合寫成規格——ADR-0016 拒絕過同一個做法。
- 代價是：多欄 entity 的部署必須自己宣告，否則區間照舊偏窄。這一點要寫進設定註解與 `docs/pipelines/evaluation-diagnosis.md`。
- **所以多欄 entity 又沒宣告時要印一句警告**（log 與報表各一次）：「重抽單位＝完整 entity（N 欄），區間可能偏窄」。預設值保的是零遷移，但它保留的正是本 ADR 判定為錯的行為；而信賴區間輸出的是「有多少把握」，預設錯的方向是**高估把握**，不能無聲無息地送進結論。這一點與 ADR-0016 的同款預設不同——那裡的鍵影響抽樣，不影響結論的可信度。
