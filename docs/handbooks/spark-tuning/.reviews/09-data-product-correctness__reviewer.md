# 第 09 章技術審查日誌（reviewer）

> 待審：`docs/handbooks/spark-tuning/09-data-product-correctness.md`
> 角色：技術審查員（只查證、不改稿）。對齊 Spark 3.3.x / Hive 3.1.3-CDP / dbt / Airflow 官方。
> 圖例：✅ 已驗證(附出處) ／ ❌ 錯誤(正確值+出處) ／ ⚠️ 無法查證 ／ 🟡 措辭/方向觀察。
> 邊查邊 append，逐條一條一寫。

---

## 進度

### §9.2 — dbt 四個內建 generic test 名稱與宣告

✅ **四個內建 generic test = `unique` / `not_null` / `accepted_values` / `relationships`** — 與內文（L64）、footer（L107）完全一致。
出處：dbt — Add data tests to your DAG（docs.getdbt.com/docs/build/data-tests）逐字「Out of the box, dbt ships with four generic data tests already defined: unique, not_null, accepted_values, and relationships.」

✅ **`accepted_values` 的參數是 `values:`** — 與內文 L55-56 一致。
✅ **`relationships` 的參數是 `to:` / `field:`** — 與內文 L59-61 一致。
出處：同上頁。

✅ **`tests:` → `data_tests:` 改名屬實，舊鍵相容** — 內文 footer L107 與精確度說明 L333 都正確標註「新版 dbt（≥1.8）把 `tests:` 改名為 `data_tests:`（舊鍵仍相容）」。dbt 官方頁逐字確認改名（因 unit tests 引入而 disambiguation），舊 `tests:` 仍向後相容。

🟡 **觀察（非錯誤，但建議 reader 留意）：現代 dbt 範例把參數收進 `arguments:` 區塊。** 官方頁現行範例為：
```yaml
- accepted_values:
    arguments:
      values: ['placed', ...]
- relationships:
    arguments:
      to: ref('customers')
      field: id
```
內文 L44-62 的範例直接把 `values:` / `to:` / `field:` 放在 test 名稱下（無 `arguments:` 層），這是**舊式（≤1.x 仍可用）寫法**。footer L107 已明說「並可把參數收進 `arguments:`」，且舊語法相容 → **不算錯**，但內文主範例用舊式、footer 講新式，兩者沒對齊到同一個範例；若要對齊「全書用最新語法」可斟酌。判定：誤讀層級（不改或微調）。

### §9.2 — dbt severity

✅ **`severity` 取值 `error`/`warn`，預設 `error`** — 內文 L70（「`severity: error`（預設）」）、L71、footer L107 一致。
✅ **`error` 失敗擋 build／`warn` 只示警不擋** — 內文 L70-71、L83 一致。出處逐字「a test with severity: warn will only ever return a warning, and not cause errors.」
✅ **`error_if` / `warn_if` 為閾值條件式，預設 `!=0`** — 內文 L72、L80 用法（`error_if: ">1000"`）與語意一致。出處：dbt — severity（docs.getdbt.com/reference/resource-configs/severity）逐字確認預設 `!=0`、條件式可用 `>5`/`=0`/`between` 等。
出處：dbt — severity 頁。

🟡 **觀察：內文 L74-81 範例把 `severity`/`error_if` 放在 `config:` 下** — 這與官方 severity 頁範例（`config:` 下放 `severity`/`error_if`/`warn_if`）一致 ✅，寫法正確。

### §9.3 — Spark 3.3 無原生 AS OF / temporal join（立論關鍵）

✅ **Spark 3.3.2 SQL join 類型 = inner / cross / left[outer] / right[outer] / full[outer] / left semi / left anti，且無 AS OF / temporal / point-in-time join** — 與內文 L166、footer L168、取捨總表 L306 完全一致。
出處：Spark 3.3.2 archive — JOIN（archive.apache.org/dist/spark/docs/3.3.2/sql-ref-syntax-qry-select-join.html），Join Types 段逐字列出上述 7 型，全頁無 AS OF/temporal/point-in-time 字樣。
→ §9.3 的立論「snapshot-partition 模型只需等值 `f.snapshot_date = l.snapshot_date`、繞開 as-of join」在 Spark 3.3.2 為**正確**。

### §9.4 / §9.5 — ALTER TABLE ADD COLUMNS

✅ **`ALTER TABLE … ADD COLUMNS (col data_type)` 語法存在、且只加不改** — 與內文 L182、footer L192 一致。
出處：Spark 3.3.2 archive — ALTER TABLE（archive.apache.org/dist/spark/docs/3.3.2/sql-ref-syntax-ddl-alter-table.html），逐字「ALTER TABLE ADD COLUMNS statement adds mentioned columns to an existing table.」語法 `ALTER TABLE table_identifier ADD COLUMNS ( col_spec [ , ... ] )`。只 append、不 rename/drop。
→ §9.4「加欄安全」論述成立。

### §9.5 — 雙層 PARTITIONED BY DDL

✅ **`PARTITIONED BY (snapshot_date STRING, build_version STRING)` 多欄分區合法** — 與內文 L244 一致。
✅ **分區欄只在 PARTITIONED BY 宣告、不重複進主欄列表** — 與內文 L238-245（主欄只列 cust_id/feat_a/feat_b，分區欄在 PARTITIONED BY）一致。
出處：Spark 3.3.2 archive — CREATE TABLE (Hive format)，`PARTITIONED BY ( col_name col_type, ... )`，範例 `CREATE TABLE student (id INT, name STRING) PARTITIONED BY (age INT) STORED AS ORC;`（age 不重複在主列表）。

### §9.5 — INSERT OVERWRITE … PARTITION(a=,b=) 寫子分區

✅ **`INSERT OVERWRITE TABLE t PARTITION (a='..', b='..') SELECT ...` 多欄靜態分區合法** — 與內文 L249-252 一致。
✅ **全部分區欄給靜態值時，SELECT 不含分區欄** — 與內文 L251 註解「不含兩個分區欄」一致。
✅ **OVERWRITE 只覆寫指定子分區、其他分區不動** — 支撐內文 L234/L248「新 build 寫新子分區、舊 build 原封不動」。
出處：Spark 3.3.2 archive — INSERT，`PARTITION ( partition_col_name = partition_col_val [ , ... ] )`；範例 `INSERT OVERWRITE students PARTITION (student_id = 222222) SELECT name, address ...`（分區欄不在 SELECT），且只覆寫該分區。

### §9.5 — current-build view（CREATE OR REPLACE VIEW）

✅ **`CREATE OR REPLACE VIEW ... AS SELECT ...` 合法** — 與內文 L279 一致。
出處：Spark 3.3.2 archive — CREATE VIEW，`CREATE [ OR REPLACE ] [ [ GLOBAL ] TEMPORARY ] VIEW [ IF NOT EXISTS ] view_identifier ... AS query`。

### §9.5 — current_timestamp() 在 audit 正確、與 §8.4 禁 current_date() 不衝突

✅ **`current_timestamp()` 回傳「query 評估開始時的時間」、可帶空括號呼叫** — 與內文 L228 用法、L232 論述一致。
✅ **§8.4 禁 `current_date()` 當業務日期 vs audit 用 `current_timestamp()` 記實際跑的時刻——論述站得住。** 兩者語意不同維度：§8.4 反對的是用「執行當天」冒充**業務邏輯日期**（會破壞冪等/回填確定性）；audit 帳本要記的正是「這次實際幾點跑」這個**維運事實**，本就該用真實時鐘。兩個 current_* 行為本身一致（都取 query 開始時刻），差別在「拿它當什麼用」，內文 L232 區分正確。
出處：Spark 3.3.2 archive — Built-in Functions（current_timestamp / current_date 皆「Returns ... at the start of query evaluation」，無括號語法自 2.0.1 支援）。

### §9.5 — audit 帳本 INSERT INTO（append）

✅ **`CREATE TABLE IF NOT EXISTS ... STORED AS PARQUET` + `INSERT INTO ... SELECT`（append）語法/語意正確** — audit 為不可變歷史事實、只追加不覆寫，內文 L213-232 論述自洽，呼應 §8.2「append 只在絕不重跑同一批時才對」。INSERT INTO 為 append 語意（與 INSERT OVERWRITE 相對），Spark 3.3.2 INSERT 文件確認。

### §9.2 — union-all 品質閘 SQL（HAVING COUNT(*)>0 用法）逐段語意

✅ **`HAVING COUNT(*) > 0` 無 GROUP BY 合法、整個結果視為單一群組** — 內文 L88-100 三個分支的設計（有違規回一列、無違規回零列）成立。
出處：Spark 3.3.2 archive — HAVING，逐字有「`HAVING` clause without a `GROUP BY` clause」範例 `SELECT sum(quantity) AS sum FROM dealer HAVING sum(quantity) > 10;`。
- 分支1 `cust_id_null`：聚合無 GROUP BY → 單一群組，`HAVING COUNT(*)>0` → 有 null 列才回一列（bad_rows=該數）。✅ 行為如註解。
- 分支2 `label_out_of_domain`：同上，label∉{0,1} 有列才回。✅
- 分支3 `dup_grain`：內層 `GROUP BY cust_id HAVING COUNT(*)>1` 找出重複的 cust_id（每個重複 key 一列、cnt=該 key 列數），外層 `SELECT 'dup_grain', cnt FROM (...) d`。✅ 語意成立：**有任一 grain 重複就至少回一列** → 外層作業判「有列＝違規」即正確擋線。

🟡 **觀察1（語意不對稱，非錯誤）：分支3 的 `bad_rows` 欄語意與分支1/2 不同。** 分支1/2 的第二欄是「違規列總數」（單一數字）；分支3 把**每個重複 key 各回一列**、值是「該 key 重複幾次（cnt）」。所以若有 3 個 cust_id 各重複，分支3 回 **3 列**、各自 cnt 值，而非「一列、值=總重複列數」。
- 對「外層只看有沒有回列來判生死」（內文 L85-86/L103 的設計）→ **完全不影響正確性**（有列即擋）。
- 但 `bad_rows` 欄在三分支間語意不齊（總數 vs 單一 key 計數、且分支3 可能多列）。若讀者把回傳當「每個 check 一列、bad_rows=該 check 違規數」的報表來解讀，分支3 會「展開成多列」而稍微反直覺。判定：誤讀層級（可在註解補一句「dup_grain 會每個重複 key 一列」更精準；不改也不算技術錯誤，因立論是「有列＝違規」）。

✅ **UNION ALL 欄位對齊**：三分支皆 2 欄（check_name:STRING, 數值:BIGINT/LONG）。`COUNT(*)` 與子查詢 `cnt`(=COUNT(*)) 皆 BIGINT，字串常數皆 STRING → 型別/欄數對齊，UNION ALL 合法。內文 L89/L93/L97 只在第一分支命名欄（`AS check_name`/`AS bad_rows`），UNION ALL 以**第一個** SELECT 的欄名為準，符合 SQL 標準行為。✅

### §9.2 — 單欄 unique 誤判 + dbt_utils.unique_combination_of_columns（⚠️ 警示框 L66 / footer L107）

✅ **內建 `unique` 只測單欄；snapshot-partition grain (cust_id, snapshot_date) 對 cust_id 單欄下 `unique` 會誤判** — 內文 L66 警示框、footer L107、精確度說明 L333、取捨總表 L305 一致且正確。
✅ **多欄組合唯一性正解 = `dbt_utils.unique_combination_of_columns`（或自訂/代理鍵）** — 與內文一致。
出處：dbt 官方 FAQ「Can I test the uniqueness of two columns?」（docs.getdbt.com/faqs/Tests/uniqueness-two-columns）逐字確認內建 `unique` 僅單欄、推薦 `dbt_utils.unique_combination_of_columns`（大資料更 performant）；dbt-utils repo 確認該 test 存在（combination_of_columns 參數）。
→ 此為本章較硬的技術主張之一，**完全正確且方向對**（抄 `- unique` 上去會天天假警報）。

### §9.3 — 兩組 ❌/✓ join（鎖 snapshot_date）SQL 與洩漏邏輯

✅ **第一組（事件時間上界）**：❌ `SUM(amount) ... GROUP BY cust_id`（無時間上界）會把 snapshot 之後的消費算進來 → 洩漏；✓ 加 `WHERE txn_date <= DATE '2026-05-31'` 擋掉。語法（`DATE '2026-05-31'` 字面量、GROUP BY）皆 Spark 3.3 合法，洩漏因果方向正確（用未來資料→特徵偷看答案）。內文 L134-145。✅
✅ **第二組（join 鎖 snapshot_date）**：❌ 只 `ON f.cust_id = l.cust_id` 漏 snapshot_date → label 可能配到別月特徵（跨 snapshot 取錯）；✓ 加 `AND f.snapshot_date = l.snapshot_date` + `WHERE f.snapshot_date='2026-05-31'`。語法合法，修法對齊時間切線、語意如文中所述。內文 L149-162。✅
- 🟡 觀察（非錯誤）：✓ 範例同時有 `f.snapshot_date = l.snapshot_date`（等值對齊）**與** `WHERE f.snapshot_date='2026-05-31'`（釘死單一 snapshot）。兩者並存合理（既對齊又限定那批），但嚴格說「鎖住對齊」靠的是 ON 的等值條件，WHERE 只是再限定處理範圍——內文把重點放在 ON 的 `AND f.snapshot_date = l.snapshot_date`（L160 註解「鎖在同一條時間切線上」）正確，無誤導。

### §9.5 — current-build view（MAX(build_version) self-join）

✅ **`JOIN (SELECT snapshot_date, MAX(build_version) ... GROUP BY snapshot_date) latest ON f.snapshot_date=latest.snapshot_date AND f.build_version=latest.build_version`** 取每個 snapshot 最新 build — 語法合法、語意正確（每個 snapshot_date 取 MAX(build_version) 那一版的全部列）。內文 L279-287。✅
✅ **`MAX(build_version)` 取最新依賴「版本字串可排序」** — 內文 L278 註解、招式一 L202、footer L296 都明說「用可排序的時間戳格式（如 20260605T0200）MAX 才能正確選到最新」。此前提講清楚了 → **正確且誠實**（沒把它當無條件成立）。
🟡 觀察（非錯誤）：字典序 MAX 對 `YYYYMMDDThhmm` 格式恆等於時間序，前提已明示；若有人用非零填補或不同長度格式才會壞，但內文已釘死格式建議，無缺陷。

### 跨章引用正確性

✅ **§9.5 L232「§8.4 禁的是拿執行當天冒充業務邏輯日期」** — 與第 08 章 §8.4 實際內容一致。§8.4 L227 逐字「排程／可回填的 SQL 裡別寫 `current_date()`」「取的是腳本實際執行的那天、不是你要補的那天」。→ §9.5「audit 用 current_timestamp() 記實際跑時刻 vs §8.4 禁 current_date() 當業務日期，兩者不衝突」**論述完全站得住**。
✅ **§9.4 L176「契約一 schema 只加不改（接 §5.8）」** — 第 05 章 §5.8 L318/L329-330 確有「schema 只加不改」「加欄安全、改型別/改名/刪欄危險」，方向一致。
✅ **其餘跨章引用（§8.2 冪等、§8.3 就緒閘門/_SUCCESS/sensor、§8.5 set -euo pipefail、§8.6 清過期分區、§8.7 踩雷表、§5.4 高基數分區膨脹）章節編號與描述皆與實際標題吻合**（08-operating-pipelines.md §8.2/8.3/8.4/8.5/8.6/8.7、05-storage-efficiency.md §5.4/5.8 均存在）。

### Airflow on_failure_callback（§9.2 L105）

✅ **「Airflow 可掛 `on_failure_callback` 通到 email/Slack」** — Airflow 官方確認 `on_failure_callback` 為 task instance 失敗時呼叫的 callback（可路由告警），DAG/default_args/task 三層可設。
出處：Airflow — Callbacks（airflow.apache.org/.../logging-monitoring/callbacks.html）。措辭是「可掛」（能力陳述）而非硬規定，恰當。

### §12 風格校驗（因果方向 / 軟建議寫成硬限制 / 暗示對立面完美）

✅ **因果方向**：全章「做 X→Y」方向皆正確——
- §9.3「用到未來資訊 → 離線評估虛高、上線崩盤」(L113) 方向對（洩漏使離線偷看答案→虛高；上線無未來資訊→原形畢露）。
- §9.5「overwrite 同分區 → 舊版被蓋掉、重現不出」(L198/L208) 方向對。
- §9.5「build_version 升為分區鍵 → 不同 build 不互相覆蓋、舊版留著」(L234) 方向對。
- §9.5「build_version 變第二層分區 → 分區數翻倍、可能小檔」(L289) 方向對（接 §5.4）。

✅ **軟建議 vs 硬限制**：未發現把官方「建議」誤寫成「硬上限/規定」。
- dbt severity 預設、error_if/warn_if 為**事實陳述**（有官方出處），非作者規定。
- 「schema 只加不改」「破壞性變更版本化」「過渡期」「保留版數/清理政策」均明說**依下游數量/遷移成本/稽核需求/儲存預算而定**（L186/L188/L292/L296），未硬性量化。
- 漂移門檻 20%（L103）明標「示意值，依你資料波動設定」。✅
- 「Spark 3.3 無原生 AS OF join」是**事實限制**（非建議），標成硬限制正確。✅

🟡 **§12「暗示對立面完美」掃描**：§9.3 講 snapshot 模型「天生幫你對齊」「是個好設計」措辭略帶推銷，但有給條件（兩邊都讀同一 snapshot_date 分區、定義同一份），未過度宣稱 as-of 模型「一定差」——只說它在此情境「又貴又易錯」(L166)，且限定在「特徵帶任意 effective_date」時才需要。無「暗示對照組完美」問題。

🟡 **取捨總表 L306「as-of join：... Spark 3.3 無原生」** — 與內文一致、正確。整張取捨總表 5 列方向皆與內文吻合，無新主張。

### ⚠️ 新發現：§9.2 `accepted_values` 對整數欄缺 `quote: false`（可能踩雷）

⚠️→🟡 **內文 L53-56 對 `label`（整數欄）用 `accepted_values: values: [0, 1]`，未設 `quote: false`。**
dbt 的 `accepted_values` 有 `quote` 參數、**預設 `true`＝把值 single-quote**，產生的測試 SQL 會比成 `... not in ('0','1')`（字串）。官方明文：「To test non-strings (like integers or boolean values) explicitly set the `quote` config to `false`.」
出處：dbt — data tests property（docs.getdbt.com/reference/resource-properties/data-tests）逐字；範例 `values: [1,2,3,4]` 搭 `quote: false`。
- **是否真的壞？** 在 Spark/CDP，`int_col IN ('0','1')` 因隱式型別轉換通常**仍能跑、結果正確**，所以不一定 fail；但這是**仰賴隱式轉換**、非官方建議寫法，跨引擎/嚴格型別下可能誤判或型別錯誤。
- **判定**：可加強（斟酌）。內文示範整數值域檢查的「教科書正解」應為 `values: [0,1]` + `quote: false`；現狀對 SQL-first 讀者直接照抄、又剛好在 Spark 不報錯，反而養成漏 `quote: false` 的習慣，到別的引擎才踩。建議補 `quote: false` 或加一句但書。**不是硬錯誤**（Spark 多半能跑），但屬「示範最佳實踐時的不精確」。

### 其他 SQL 可跑性確認

✅ **audit 表 `CREATE TABLE IF NOT EXISTS ... STORED AS PARQUET`、雙層分區表 `STORED AS PARQUET`** — 語法合法。CDP 下 Spark `CREATE TABLE` 多建 external（§5.8），對本例（append audit / 雙層分區留歷史）無影響。
✅ **靜態多欄 `INSERT OVERWRITE ... PARTITION(snapshot_date=, build_version=)` 不需開動態分區設定** — 因兩個分區欄都給靜態值，不觸發 `spark.sql.sources.partitionOverwriteMode`/dynamic partition 需求。內文 L249-252 可直接跑。

---

## 結尾彙整（三級）

### A. 真缺陷（必補）
**無。** 本章所有可查證的硬技術主張（dbt 四 test 名稱/參數、severity 預設與 error_if/warn_if、Spark 3.3.2 無 AS OF join 與 7 種 join 類型、ALTER TABLE ADD COLUMNS 只加不改、雙層 PARTITIONED BY、INSERT OVERWRITE 靜態子分區、CREATE OR REPLACE VIEW、current_timestamp 語意、HAVING 無 GROUP BY、dbt_utils.unique_combination_of_columns、Airflow on_failure_callback、跨章引用 §8.4/§5.8/§8.x/§5.4）**全部查證為正確、且出處對得上**。無「SQL 跑不動」「語意不符」「因果反號」「出處掛錯」「軟建議寫成硬限制」。

### B. 可加強（斟酌、不影響正確性）
1. **§9.2 `accepted_values: values: [0,1]` 缺 `quote: false`**（L53-56）。dbt 預設 `quote: true` 會 single-quote 值；官方明示測非字串欄應設 `quote: false`。Spark 因隱式轉換多半仍跑對、不一定 fail，但這是示範最佳實踐處的不精確，建議補 `quote: false` 或加但書（避免讀者照抄到嚴格型別引擎才踩）。**這是本章唯一值得實際動筆的一條。**
2. **§9.2 dbt 主範例用舊式（無 `arguments:` 層）語法**（L44-62），footer 才提新式 `arguments:`。舊式相容、不算錯；若全書統一最新語法可斟酌讓主範例也用 `arguments:`。

### C. 誤讀／可微調（不改也成立）
1. **§9.2 union-all 品質閘分支3（dup_grain）的 `bad_rows` 語意與分支1/2 不對稱**（每個重複 key 各一列、值=該 key 計數，vs 前兩支的「違規總數一列」）。對「有列＝違規＝擋線」的立論**完全不影響**；僅當讀者把回傳當報表逐欄解讀時稍反直覺。可在註解補「dup_grain 每個重複 key 一列」一句，或不改。
2. **§9.3 ✓ join 範例同時有 `ON ... AND f.snapshot_date=l.snapshot_date` 與 `WHERE f.snapshot_date='2026-05-31'`**——兩者並存合理（對齊＋限定批次），內文把「鎖時間切線」歸給 ON 等值條件，正確、無誤導。
3. **§9.5 `MAX(build_version)` 依賴版本字串可排序**——前提（時間戳格式 `YYYYMMDDThhmm`）已在 L202/L278/footer 明示，誠實、無缺陷。

### 出處清單（皆權威來源）
- dbt: docs.getdbt.com/docs/build/data-tests、/reference/resource-configs/severity、/reference/resource-properties/data-tests、/faqs/Tests/uniqueness-two-columns；dbt-utils repo（unique_combination_of_columns）。
- Spark 3.3.2 archive: sql-ref-syntax-qry-select-join / -ddl-alter-table / -ddl-create-table-hiveformat / -dml-insert-table / -ddl-create-view / -qry-select-having / api/sql/index（current_timestamp、current_date）。
- Airflow: airflow.apache.org/.../logging-monitoring/callbacks.html（on_failure_callback）。
- 跨章：本 repo 08-operating-pipelines.md §8.2-8.7、05-storage-efficiency.md §5.4/§5.8（編號與描述吻合）。

