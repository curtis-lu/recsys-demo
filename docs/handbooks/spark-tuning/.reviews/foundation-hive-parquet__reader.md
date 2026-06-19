# 目標讀者審查：Hive/Metastore 與 Parquet 三段新內容

**審查視角**：銀行資料分析師／科學家，SQL-first，**沒有資料工程背景**。
只熟「Hive = 我在 Hue 打 SQL 的地方」，沒聽過 HDFS 內部、檔案格式、Hive 架構。
**只看可讀性**（一個聰明的 SQL 人能不能跟上），不查技術正確性。

## 待讀段落
1. ch01 §1.1 新 `###` 子節「順帶認識：你查的「表」存在哪、又是誰記得它在哪」（01-how-spark-runs-your-sql.md，L41 起）
   - 核心問題：Hive 是兩件事（表/Metastore vs Hive on Tez 引擎）的拆解，對只知道「Hive=打 SQL 的地方」的人有沒有落地？Metastore 從零講清楚了嗎？HDFS 存 bytes / Metastore 記表 的切分直覺嗎？
2. ch03 §3.3 Parquet 括號註解（03-sql-tuning.md，L82）— 幫助還是打斷？
3. ch05 §5.2 新 `###` 子節「進階：Parquet 檔內部長什麼樣」（05-storage-efficiency.md，L65 起）含 mermaid 圖
   - 核心問題：row group/column chunk/page 階層好懂嗎？「排序收緊 min/max → 跳更多」可操作嗎？這些詞首次承重使用時定義了嗎：row group、column chunk、page、page index、dictionary encoding、vectorized reader、bloom filter、stripe、HDFS block？「ORC 同物異名」橋接清楚嗎？進階段有沒有誠實標示日常可跳過？

---

## 即時反應（邊讀邊寫）

### 段落 1：ch01 §1.1「順帶認識：你查的『表』存在哪」（L41–57）

讀進來前，前文（L15）已經把 **HDFS** glossed 成「叢集的分散式檔案系統，把大檔案切塊、分存到多台機器」、**CDP**=Cloudera 大數據平台、**YARN**=資源管家。所以進這節時 HDFS 已熟，good。

- L43 開場「`SELECT … FROM card_txn` 其實是兩樣東西合起來」——非常好的鉤子，正中我（SQL 人）的盲點：我從來沒想過 `card_txn` 不只是「一張表」。
- L45–46 把「資料本身（HDFS 上的檔）」vs「登記（Metastore）」拆開，**Metastore 從零講清楚了**：「另有一份目錄，記著有哪些表、欄位、分區、檔案放 HDFS 哪個路徑」。metadata/中繼資料也當場 gloss。✅ 第一次承重使用就定義，符合要求。
- L48「HDFS 存的是位元組、Metastore 記的是『有哪些表、在哪裡』」——這句一錘定音，bytes/tables 的切分**很直覺**。✅ 核心問題之一達標。
- L50–55 的「Hive 兩件事」拆解：對只知道「Hive=我在 Hue 打 SQL 的地方」的人——
  - **這裡有個小斷層**：我心中的「Hive」是「我打 SQL 的地方（Hue）」，但文中兩個定義是「(1) Hive 表/Metastore」「(2) Hive on Tez 引擎」。**「Hue」這個我每天用的詞，跟這兩個定義都沒接上**。讀者會問：那我在 Hue 按下執行，到底是用到哪個？Hue 是不是就是 Hive 引擎？建議補半句把 Hue 接上：「（你在 Hue 點的『Hive』下拉，選的就是第 2 個——Hive 引擎；但它查的表登記在第 1 個 Metastore 裡）」。否則「兩件事」澄清了，但跟我唯一的錨點 Hue 仍是斷的。
  - 「**Hive on Tez**」「**Tez**」：括號裡「Hive（on Tez）」的 **Tez 完全沒解釋**。對 SQL-first 讀者 Tez 是純黑話。雖然這裡只是順帶提，但既然寫出來了，建議 gloss 一句（如「Tez 是 Hive 底層的執行引擎，類比於 Spark 之於 Spark SQL」）或乾脆只寫「Hive 這個查詢引擎」、把 on Tez 留到第 06 章。
  - L52「三個引擎（Spark、Hive、Impala）共用」——**Impala 第一次出現、未 gloss**。前文沒提過 Impala。SQL-first 銀行讀者未必知道 Impala 是什麼。建議首次出現加半句「（Impala：另一種查 Hive 表的高速互動查詢引擎）」。
- L55「後面很多話才不會打結」+ 兩個對照例子（用 Spark 查別人用 Hive 建的表 vs Spark 比老 Hive 快）——這個「打結 vs 不打結」的對照**非常有效**，把抽象的「兩件事」落到讀者真會聽到的兩句話上。✅
- L57 來源註的「NameNode（記檔案清單的）跟 Metastore 是兩份不同目錄」——好的前向防混淆，但 NameNode 在本章此處還沒出現（§5.4 才細講），這裡突然冒出來對讀者是個沒上下文的新詞。可接受（在來源/⚠️ 區），但若能加「§5.4 會講」更好（其實有寫 §1.2/§5.4，OK）。

**段落 1 小結**：HDFS/Metastore/bytes-vs-tables 的核心切分**漂亮達標**；扣分都在「Hive 兩件事」那段冒出的未 gloss 黑話：**Hue 沒接上、Tez 沒解釋、Impala 首現未 gloss**。

### 段落 2：ch03 §3.3 Parquet 括號註解（L82）

語境：在講「別 SELECT *」的原理，順勢解釋寬表用 Parquet/ORC 欄式存。

- 括號「**Parquet＝一種存在 HDFS 上、為大數據查詢設計的開源檔案格式，是 Spark 生態最常見的選擇；ORC 則是 CDP 上 Hive 受管表的預設——兩者同類、效果相近**」：對 SQL-first 讀者剛剛好。把 Parquet 從「沒聽過的字」拉到「喔就是個檔案格式」，且點出 ORC 是同類、CDP 預設，預先消掉「那我們用的到底哪個」的疑問。
- 「**受管表**」（managed table）這詞**第一次出現、未 gloss**。SQL-first 讀者不知道 managed/external 表的差別。不過這裡只是順帶定位 ORC，受管表的細節在 §5.8，影響輕微。建議：要嘛簡化成「CDP 上 Hive 表的預設」省掉「受管」，要嘛加 4 字「受管（managed，Hive 自己管資料生命週期的表）」。
- 括號是否打斷閱讀？**略長但不致命**。它一口氣塞了 Parquet 定義＋ORC 對比＋「見 §5.2」三件事。對想快速讀「別 SELECT *」操作建議的人，這串括號讓那句主幹（「把同一欄收在一起存」）被推遲了。但內容都相關、且結尾「內部結構見 §5.2」正確地把深水區外推。可接受，**偏「lands」但稍重**。若要更順，可把 ORC 那半句也外推到 §5.2，這裡只留「Parquet＝欄式檔案格式（§5.2 詳）」。

**段落 2 小結**：基本 lands，gloss 有效；唯一真正的未定義詞是「**受管表**」。

### 段落 3：ch05 §5.2「進階：Parquet 檔內部長什麼樣」（L65–102）

- L67 開場就**誠實標示可跳過**：「這節偏進階——日常查詢用不到也沒關係；但當你要負責產出、調校共用大表時…」。✅ 核心問題之一達標，標得很清楚。
- L69–84 階層（mermaid + 條列）：
  - **Row group**：gloss「把整個檔『橫著』切成幾大段，每段 ~128MB；是跳塊的基本單位」。✅ 「橫著切」配合前面「欄式是直著收」很好懂。
  - **Column chunk（欄塊）**：「一個 row group 裡某一欄的全部值」。✅ 接回 §3.3 column pruning。
  - **Page（頁）**：「欄塊再切成許多頁，~1MB，壓縮與編碼真正作用的最小單位」。✅
  - 三層 row group→column chunk→page **階層好懂**，由大到小、配圖。mermaid 圖把 page 只畫在 amount 欄塊下、且只畫一個 page，略簡，但文字補足，OK。
- L86–91「為什麼決定跳得掉多少」+「取決於資料怎麼排」：
  - **page index** 第一次出現（L86 括號「較新的 Parquet 還有 page 層級的 page index，能跳到更細的頁」）——**gloss 夠**（說明它是 page 層級、能更細跳）。✅
  - **排序收緊 min/max → 跳更多**：L88–91 的亂序 vs 排序對照**非常 actionable**：「亂序→每個 row group 的 [min,max] 橫跨整個範圍→一段都跳不掉」「照常篩的欄 SORT BY 再寫出→範圍又窄又不重疊→大量排除」，並明說「**這是你產表時能主動做的一招**」。✅ 核心問題達標，且給了可操作動作（寫出前 SORT BY）。
- L93–98 進階點：
  - **dictionary encoding（字典編碼）**：gloss「存一份編號→值對照表＋一串編號」+ 具體例（product_type 22 種值）。✅ 定義清楚、有銀行味的例子。
  - **vectorized reader（向量化讀取）**：gloss「一次一批（預設 4096 列）用欄式批次解碼…比逐列快」。✅ 且給了「對複雜巢狀型別會自動關掉、掃描莫名變慢時可疑」的可操作徵兆。good。
    - 小詞：「**巢狀型別**（nested type）」未 gloss。SQL-first 讀者（多半只用過扁平欄位 int/string/date）未必知道巢狀型別=array/struct/map 這種欄中有欄。建議加 4 字例：「複雜巢狀型別（如 array／struct 這種欄裡還有結構）」。
  - **HDFS block**：L97「row group 別大過 HDFS block」——**HDFS block 在本章此處尚未定義**（§5.4 L148 才講「HDFS 預設區塊 128MB」）。本節 L97 用到時只說「（§5.4，預設 128MB）」，算有給數字+前向指標，但「block＝HDFS 把檔案切成的固定大小儲存塊」這個定義要翻到 §5.4。對線性讀到這的人是個小坑。建議在 L97 首次用時加半句：「HDFS block（HDFS 在磁碟上存檔的固定大小區塊，預設 128MB）」。
    - `parquet.block.size` 這個 config 名直接出現，且容易誤會——它名字叫 block.size 控的卻是 **row group** 大小（文中 L97/L102 有點到，但讀者可能困惑「block 不是 HDFS 的嗎」）。文中 L102 來源註有澄清 `parquet.block.size`=row group 預設，OK，但正文若一句點破「（這個設定名雖叫 block，控的是 row group）」會更防混淆。
  - **bloom filter**：gloss「一種能快速回答『這段裡一定沒有這個值』的小索引」。✅ 定義精準好懂，且講清楚用途（補高基數等值查詢 min/max 跳不掉的洞）、且標「屬進階、預設多半不開」。
- L100「**ORC 也是同一套，只是換了名字**」橋接：
  - **stripe**：gloss「≈ row group，~64MB」。✅ 直接給對應物，bridge 清楚。
  - 「每 10000 列一個 **row group index**」——這裡 ORC 的「row group」跟 Parquet 的「row group」**同名不同義**（ORC 的 stripe≈Parquet row group，但 ORC 內又有個叫 row group 的更細索引單位）。對讀者**有潛在混淆**：剛學會「row group=128MB 大段」，這裡又冒出「ORC 每 10000 列一個 row group index」。建議加半句點破：「（注意 ORC 把更細的跳塊單位也叫 row group，跟 Parquet 的 row group 不是同一層，別混）」。
  - 整體「同一回事、名詞不同」的 bridge **lands**：stripe/bloom filter 對應清楚，心法（排序收緊、低基數好壓、bloom 補高基數）明說兩邊都成立。✅
- L102 來源/⚠️ 區誠實標示哪些是近似、無逐字數字。✅ 符合手冊風格。

**段落 3 小結**：階層、排序→跳塊、可跳過標示**都達標**；多數術語首次承重使用都有 gloss（dictionary/vectorized/bloom/page index/stripe 都過）。剩三個小坑：**巢狀型別未 gloss、HDFS block 在此節定義靠前向指標、ORC「row group」與 Parquet「row group」同名不同義易混**。

---

## 最終裁決

- **段落 1（Hive/Metastore）**：**lands，但需小修**。bytes-vs-tables 核心切分漂亮；扣分在「Hive 兩件事」段冒出未 gloss 的 Hue 接點缺失、Tez、Impala。
- **段落 2（ch03 Parquet gloss）**：**lands**。有效、外推得當；唯一未定義詞「受管表」影響輕微。
- **段落 3（ch05 Parquet 內部）**：**lands，但需小修**。階層/排序心法/可跳過標示都達標、gloss 覆蓋率高；剩巢狀型別、HDFS block 定義位置、ORC row group 同名衝突三個小坑。

## 必修可讀性清單（依嚴重度）

1.（段落1，最重）**Hue 沒接上「Hive 兩件事」**。讀者唯一錨點是「Hive=我在 Hue 打 SQL 的地方」，但兩個定義(表/Metastore、on Tez 引擎)都沒提 Hue。建議在 L55 後補半句：「（你在 Hue 選的『Hive』就是第 2 個——引擎；它查的表登記在第 1 個 Metastore 裡）」。
2.（段落1）**Impala 首次出現未 gloss**（L52）。加：「（Impala：另一種直接查 Hive 表的高速互動查詢引擎）」。
3.（段落1）**Tez 是純黑話**（L53「Hive（on Tez）」）。要嘛 gloss「Tez＝Hive 底層的執行引擎，類比 Spark 之於 Spark SQL」，要嘛此處只寫「Hive 這個查詢引擎」、把 on Tez 留到第 06 章。
4.（段落3）**ORC 的「row group」與 Parquet 的「row group」同名不同義**（L100），剛學完 Parquet row group=128MB 大段，這裡「每 10000 列一個 row group index」易混。加半句：「（ORC 把更細的索引單位也叫 row group，跟 Parquet 那層不同，別混）」。
5.（段落3）**HDFS block 在 §5.2 此處用到但定義在 §5.4**（L97）。線性讀者撞到無定義詞。L97 首次用時補：「HDFS block（HDFS 在磁碟上存檔的固定大小區塊，預設 128MB）」。
6.（段落3）**巢狀型別未 gloss**（L96）。加例：「複雜巢狀型別（如 array／struct 這種欄裡還有結構）」。
7.（段落2，輕）**受管表未 gloss**（L82）。簡化成「CDP 上 Hive 表的預設」或加「受管（managed，Hive 自管資料生命週期）」。
8.（段落3，可選）**`parquet.block.size` 名實不符**（控的是 row group 不是 HDFS block）。正文一句點破可進一步防混淆（來源註已澄清）。

非阻塞優點：bytes/tables 一句定調(L48)、「打結 vs 不打結」對照(L55)、亂序 vs SORT BY 對照(L88–91 actionable)、bloom filter 的 gloss(L98)、ORC「同一套換名字」bridge(L100) 都很好。

