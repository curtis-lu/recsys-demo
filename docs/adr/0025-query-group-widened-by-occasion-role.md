---
status: accepted
date: 2026-09-20
---

# query group 由選用角色 `occasion` 加寬；無正例的組留多少由使用者決定；HPO 多一組精確的 average precision

ADR-0021 讓框架容得下「一列是一次曝光」的資料，但它只回答了「同一個 item 出現多次怎麼辦」（`event` 角色），沒回答更前面的問題：**這種資料的一個 query group 是什麼。** 本 ADR 補上這一題，並記下連帶的三個決定。完整的需求、user story 與測試接點在 spec #426；這裡只記「為什麼這樣選」。

## 背景：同一份曝光紀錄，有兩種合理的 query group

```
形狀一：一個 query group ＝ 一個 entity × 一個時段      形狀二：一個 query group ＝ 一次排序的場合
┌─ entity e1 × 第 3 週 ───────────┐                  ┌─ e1 × 第 3 週 × 場合 o1 ─┐
│ item A（曝光 x1）  label 1      │                  │ item A   label 1         │
│ item A（曝光 x2）  label 0      │ ← 同一 item 多列 │ item B   label 0         │
│ item B（曝光 x3）  label 0      │                  └──────────────────────────┘
└─────────────────────────────────┘                  ┌─ e1 × 第 3 週 × 場合 o2 ─┐
                                                     │ item A   label 0         │ ← 可能只有 1 列
                                                     └──────────────────────────┘
```

形狀一的組夠大，但把不同時刻的曝光放在一起比，線上其實不會這樣比。形狀二最接近線上真正在做的事（同一刻、一起被排的那一組候選），但組可能很小。哪一種對取決於部署，框架不替使用者決定。

ADR-0021 只容得下形狀一：query group 寫死是 `time` ＋ `entity`。

## 決定

### 1. 新增選用角色 `occasion`，它會進 query group；`event` 不進

- **`occasion`**：一次排序的場合，一欄或多欄。query group ＝ `time` ＋ `entity` ＋ `occasion`。
- **`event`**（ADR-0021）：同一個 query group、同一個 item 底下有多列時分辨每一列。**不進 query group。**
- identity ＝ `time` ＋ `entity` ＋ `occasion` ＋ `item` ＋ `event`，沒宣告的角色不算。**這個順序是規定，不是寫法**：決定性抽樣的分桶把 identity 各欄依序串起來再雜湊，順序一變，同一份資料就抽出不同的列。沒宣告新角色時順序與今天相同（`time`、`entity`、`item`），所以既有部署的抽樣結果不變；之後任何加寬 identity 的改動（例如 #394 的 item 多欄）都不得調動既有欄位的相對順序。
- 形狀一只宣告 `event`；形狀二只宣告 `occasion`；兩個都宣告也合法。
- 兩個角色都是宣告了才進 identity、才進版本雜湊（ADR-0021 決定 3 的做法，延伸到 `occasion`）。
- 離線推論忽略兩個角色、監控模式在 CLI 入口擋下（ADR-0021 決定 5，延伸到 `occasion`）。

### 2. query group、identity、base key 三組欄位都只從 `get_schema` 取

`get_schema` 已經算 `identity_columns`；再多算兩格：

- `query_group_columns` ＝ `time` ＋ `entity`（＋ `occasion`）。
- `base_key_columns` ＝ `time` ＋ `entity`，**永遠不含 `occasion`**——entity 層級的表（特徵表、分群來源）用它接到候選列上。名字沿用程式裡既有的說法（dataset 的 `base_key`、`require_base_key_columns`）。

為什麼要分兩格：程式裡手拼的 `[time] + entity` 有二十多處，意思至少兩種——「界定名次比較的範圍」與「以 entity 層級的表去接」。今天兩者同值所以看不出來；`occasion` 讓第一種變寬、第二種不能變（特徵表裡沒有場合的欄位）。先收成有名字的欄位（純搬移、行為不變），之後加寬 query group 或 identity 都只改 `get_schema` 一處。

整條離線推論 pipeline（含它的排名 partition 與每組候選數的檢查）用的是 base key。這是一個決定，不是推導出來的：批次評分的當下沒有任何「場合」存在——候選是框架替每個 entity 乘上全部 item 產生的，不是哪一次請求擺出來的——所以離線推論排名的範圍是「這個 entity 在這個時段」。它的排名 partition 表面上符合「界定名次比較的範圍」，歸類時最容易被歸錯。

反過來，下面這些**一定要跟著 query group 走**，歸錯了不會報錯、只會安靜地給出錯的數字：比較報表（`--compare`）對齊兩邊 query group 並數「共同的 query group 數」的地方、以 query 為單位的診斷抽樣、診斷母體的分組。它們不受「監控模式在入口擋下」保護。

### 3. 沒有正例的 query group 留多少，由三個鍵決定

`dataset.train_zero_positive_group_ratio`（預設 1.0）、`dataset.val_zero_positive_group_ratio`（預設 0.0）、`dataset.test_zero_positive_group_ratio`（預設 0.0）。規則只有一條：有正例的 query group 全留；沒有正例的整組決定去留，留下比例 r。val／test 的 r > 0 時多一欄權重（1 或 1／r）。預設值就是今天的行為——既有「val／test 丟掉無正例的組」那一步是 r ＝ 0 的特例，兩者合成同一步。

為什麼需要：

- 把每一列當成二元預測的指標（決定 4、ADR-0024 的家族）對正例佔比很敏感。算在丟過的表上會系統性偏高；形狀二下組只有一列時，val 剩下的全是正例，指標恆為滿分。全部留下又放不下，所以留一個比例、用權重補回去。
- `lambdarank` 在沒有正例的組上梯度恆為零，training 本來就替它丟；提前到 dataset 丟，訓練表小很多，而且這一步不會拆散任何一組（逐列抽樣會把小組抽成只剩正例）。
- `binary` 與 `rank_xendcg` 會從無正例的組學到東西（後者 repo 有實測，在全是負例的組上照樣長樹），所以「丟掉也不損失」**只對 `lambdarank` 成立**。對這兩個目標，train 的 r < 1 是一種**整組的負例降採樣**，訓練母體的正例佔比會上升——性質與既有的逐列抽樣（`sample_ratio_overrides` 壓低負例）相同。**刻意不設「目標不是 `lambdarank` 就擋下 r < 1」的入口檢查**：降採樣負例是這類資料放得進表的必要手段，框架今天也不擋逐列的那一種；train 端不帶權重，因為排序只看分數高低。文件要把這個性質寫明。

**步驟的先後與適用範圍**（實作票不得自行決定）：

- train 的順序是：逐列抽樣（在 `sample_pool` 的鍵上、label 接上之前）→ train／train_dev 切分 → 接 label 與特徵 → 本步驟。所以本步驟看到的是**逐列抽樣之後還在的列**：「有正例的組全留」的意思是本步驟不再丟它們的任何一列，不是「逐列抽樣不會動它們」。某一組的正例若已被逐列抽樣抽掉，它在本步驟就是無正例的組。要讓小的 query group 保持完整，`sample_ratio` 要設 1（不逐列抽）。
- r ＝ 1.0 時本步驟不丟任何一組；r ＝ 0 時與今天 val／test 的過濾逐列相同。
- train 的鍵同時套用在 train 與 train_dev，兩邊各自對自己的組判定、用同一個 r。train／train_dev 是以 entity 互斥切開的，一個 query group 不會跨兩邊。train_dev 是 early stopping 的驗證集，它的母體因此跟著變——與既有的逐列抽樣性質相同（train_dev 本來就是從抽樣後的鍵切出來的）；`lambdarank` 下 training 本來就替兩邊丟掉無正例的組，所以 r ＝ 0 不改變它的早停母體。
- val／test 沒有逐列抽樣（val 只有整個 entity 的抽樣），所以上面的先後問題只存在於 train。

**補記（2026-09-21，#429 實作時由使用者拍板）：train 這一步做在 keys 上、建表之前。** 上面的先後寫的是「接 label 與特徵 → 本步驟」；實作改成「本步驟自己接一次 `label_table`（只接 identity 與 label）判斷去留、只丟 keys 的列 → 再照常建表」。判斷用的仍是 `label_table` 的 label，看到的仍是逐列抽樣與切分之後的列，所以留下哪些組、每組哪些列，與寫在建表之後逐列相同；差別只在特徵 join 的前後。理由是粒度閘 B10：它要求 train／train_dev 的 model_input 列數**等於**建它的 keys，建表之後才丟組，兩邊必然對不上，B10 就分不出「故意丟的」與「右表重複鍵造成的放大」（丟 1,000 列就蓋得住 500 列的放大）。在 keys 上丟，落地的 `train_keys` 就是建表的輸入，B10 照樣逐列相等。代價是 r < 1 時 train 的 label 多接一次（窄表）；r ＝ 1（預設）時整步跳過。val／test 沒有這個問題（B10 本來就不涵蓋它們），維持在建表之後過濾。

為什麼是三個頂層鍵、不是一個含三個值的鍵：版本雜湊照 `dataset` 的頂層鍵名分層（ADR-0016）。train 那個登記進 `TRAIN_SAMPLING_KEYS`（折進 `train_variant_id`）；val／test 兩個不登記（留在 `base_dataset_version`，那是 val／test 產物唯一的版本 ID）。一個鍵只能整個登記或整個不登記：前者讓「改 val 的 r」靜默讀回舊的 val 表，後者讓「只改 train 的 r」連前處理器與 val／test 一起重算。

為什麼判定放在 label 接上之後：「這一組有沒有正例」要用 `label_table` 的 label。`sample_pool` 可能自己帶一欄 label（示例就有，逐列抽樣拿它當分層鍵），但那是使用者 SQL 抄的副本，框架不保證兩者一致。當分層鍵用，不一致的後果是抽樣率偏一點；拿來判「整組丟不丟」，後果是把真正的正例整組刪掉，而且粒度閘比得過。

權重是設計權重，不是無偏估計：average precision 這類比值型指標加權後會隨組數增加收斂到未抽樣母體的值，但期望值不等於它。所以 log 與報表要印出 r 與留下來的組數，讓使用者判斷穩不穩。

### 4. HPO 多兩個目標：`pooled_average_precision`、`macro_per_item_average_precision`

精確算法（scikit-learn 的 `average_precision_score`，吃決定 3 的權重），預設目標不變。選了其中之一時 `val_zero_positive_group_ratio` 必須 > 0，在 CLI 入口檢查。

為什麼與 ADR-0024 的 `pr_auc` 並存、名字不同：那邊是全量 Spark，ADR-0024 因全量排序的成本選了分箱近似；HPO 只算 val、資料在 driver 上，精確排序便宜。而且 HPO 是拿幾十個 trial 互比——分箱的邊界取自每次的分數範圍，每個 trial 的箱都不同，近似誤差會混進 trial 之間的差距。兩者定義不同、不可對帳，所以名字必須不同；也不單獨叫 `average_precision`，因為 repo 裡 AP 已經是組內排序指標的名字。

`pooled_average_precision` 把所有列倒在一起排，會獎勵「認出哪些 entity 本來就容易有正例」，這對組內排序沒有幫助。它仍然是正式目標而不是備用選項，因為以曝光紀錄訓練的部署主要看的就是這個數字（ADR-0024 背景）；文件要把這個性質寫明。

**補記（2026-09-22，#430 實作後由使用者拍板）：`macro_per_item_average_precision` 量的是什麼。** 上面只說它「每個 item 各算一個再平均，讓曝光量大的 item 不會蓋過曝光量小的」，沒說它的邊界。average precision 只看分數的高低順序；而這個目標判斷一列算高還是算低，只拿它跟同一個 item 的其他列比。所以它檢查「同一個 item 裡，正例有沒有排在負例前面」，不檢查不同 item 之間的分數比不比得起來：給某個 item 的所有分數加同一個常數，它的值不變；item A 的負例分數高過 item B 的正例，它也不扣分（`pooled_average_precision` 會扣）。

實作審查時一度把這件事讀成「它看不到組內排序，應該拿掉」。那是拿排序的尺去量一個二元預測的目標：本決定的用意是把每一列預測準，「同一個 item 內分得開」本身就是一種預測準，它缺的是跨 item 可比。所以兩個目標都保留，分工寫明：每個 item 的分數各自使用時（例如每個 item 各自定門檻）選 macro；分數要跨 item 比較時（同一個 query group 挑一個 item，或全部 item 共用一條門檻）選 pooled。數值例子在 `docs/pipelines/training.md` §3.2。

## 更正 ADR-0021

- **〈考慮過、沒選的做法〉那一節裡以「代價要老實說：」開頭的那一段，說兩處的語意會「反轉」——不對**（該段已在 ADR-0021 就地加註）。 粒度閘 B10 與 evaluation 對 identity 的重複檢查，宣告 `event` 之後語意**不變**：「同一筆候選出現兩次」仍然是上游壞了，只是「同一筆」的定義多了欄位。同一個 item 的多次曝光 `event` 不同，本來就不是同一筆。要改的只有錯誤訊息的文字。所有重複檢查全部保留。
- **決定 4 列的「要逐處帶上 `event` 的地方」，在決定 2 落地後大半自動成立**：label join、抽樣分桶、val／test 取鍵的去重都取 `identity_columns`，identity 加寬它們就跟著加寬。真正要逐處改的只剩同分規則與 `k_values` 的 `"all"`。
- 同分規則定為「先比 `item`、再比 `event` 各欄由小到大」。已知代價：`event` 是時間戳時，同分的早曝光永遠排前面；在「宣告了 `event` 但沒接逐筆即時特徵」的設定下實測過，方向讓 mAP 差約 0.02（#378 留言）。考慮過用 identity 的雜湊決勝（與時間無關），沒選：Spark 與 numpy 兩份規則要算出同一個雜湊值，得在 dataset 多落一欄。改成讓評估報表印出同分列的佔比。
- ADR-0021 決定 1（`time` 維持時段）、3、5 沿用。

## 考慮過、沒選的做法

**讓使用者直接寫一份 query group 的欄位清單。** 最自由，但要靠檢查擋「清單必須含 `time`、不能含 `item`、必須是 identity 的一段」，而且允許比 `entity` 粗的分組——那與 `entity` 的定義（替誰排）矛盾。用角色宣告，query group 的形狀由結構保證，而那份自由沒有已知的用途。

**label 直接放在 `sample_pool`，或讓 `sample_pool` 不帶新角色的欄、由 `label_table` 帶進來。** 前者讓 dataset 多一種模式，每個資料閘都要顧兩遍；後者讓「join 後列數變多」成為合法狀態，粒度閘再也分不出合法變多與上游壞了，而且抽樣發生在 join 之前，屆時曝光還不存在。使用者的代價很小：曝光情境下兩張表本來就出自同一份曝光紀錄。

**宣告了新角色就自動換 HPO 目標、或自動打開「只留一正一負的組」。** 組夠不夠大、看哪個指標，是部署的事。框架只印出判斷用的數字（單一 label 組的佔比、同分列的佔比、進入平均的 item 數）。

**把未曝光的候選當負例補進來，或提供傾向分數逐列加權。** 沒被展示的 item 不知道會不會得到正向回應；當成 0 等於教模型模仿舊系統。曝光偏差與位置偏差的正解是隨機流量（#396）或傾向分數加權（#425），兩者的前提目前都不成立；本 ADR 的骨架不擋它們之後接進來。

## 後果

- 決定 2 的歸類判錯時，後果只在宣告 `occasion` 時出現，既有示例驗不出來。第一個驗得到的地方是形狀二的示例實跑（含離線推論），而示例目前沒有「一次請求」的欄位，要先造資料。
- val／test 的 r > 0 時，固定整數 K 的組內排序指標逐值不變（Spark 與 numpy 兩邊都在計算時跳過無正例的組），但算在過濾之前的量會變：item 種數、由它解析出的 `"all"` 的 K、`dataset_overview` 的總數。這是母體變大的誠實反映。
- 權重欄必須一路帶到預測表，而且 catalog 要宣告它——預測表的欄位是寫死的清單，落地時沒宣告的欄會被靜默丟掉。ADR-0024 的家族要從「數列數」改成「加總權重」，並在 test 的 r 為 0 時於入口擋下；否則它報表上「母體：全部曝光」那句是假的（ADR-0024 決定 3 說的「過濾之前」指 evaluation 內的過濾，dataset 在更早就丟過了）。
- `CLAUDE.md`〈這個專案是什麼〉寫著「對每個 query group（`time` × `entity`）」。在 `occasion` 落地之前這句仍然是真的，所以本 ADR 不改它；`occasion` 落地的那張 PR 要一起改（改 `CLAUDE.md` 前照該檔的規矩載入 `maintain-agent-rules`）。`CONTEXT.md` 在這段期間以「尚未實作」的標記與它並存。
- 新角色的欄位不得成為特徵，連 `categorical_columns` 那個出口也不開（`item` 是靠它成為特徵的）。
- r 該設多少、HPO 把 val 拉到 driver 撐不撐得住，都沒量過；repo 裡的合成資料推不出生產的數字。
