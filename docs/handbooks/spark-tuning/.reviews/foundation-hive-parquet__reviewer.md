# Reviewer log — foundation (Hive Metastore / Parquet / ORC) passages

**Reviewer role**: TECHNICAL-ACCURACY. Verify ONLY against authoritative sources
(Apache Spark/Parquet/ORC/Hadoop official docs, Cloudera CDP official docs,
Cloudera official engineering blog). No personal blogs.

**Date**: 2026-06-19
**Scope files**:
- `01-how-spark-runs-your-sql.md` — §1.1 new subsection "順帶認識：你查的「表」存在哪…", §1.9 parenthetical, item 7 in 精確度說明
- `03-sql-tuning.md` — §3.3 parenthetical "為大數據查詢設計的開源檔案格式"
- `05-storage-efficiency.md` — §5.2 new subsection "進階：Parquet 檔內部長什麼樣…" (+ mermaid + 📚 footer)

---

## Claims checklist (tick as verified / annotate if refuted)

- [x] C1. HMS = catalog... share ONE HMS; HMS ≠ NameNode — CORRECT fact, hedged; minor sourcing-precision (Defect #2)
- [x] C2. Parquet structure — CONFIRMED
- [x] C3. Defaults 128MB/1MB lib default vs 512MB–1GB recommendation — CONFIRMED, distinction stated correctly
- [x] C4. Spark configs/defaults — CONFIRMED verbatim
- [x] C5. Vectorized reader auto-disable for complex/nested — CORRECT & hedged (3.3.2 page version-locked 404, expected)
- [x] C6. Dictionary default-on — CONFIRMED (DEFAULT_IS_DICTIONARY_ENABLED=true)
- [x] C7. Bloom filter Parquet+ORC, no exact Spark default asserted — CONFIRMED hedged
- [x] C8. ORC analog stripe 64MB/index 10000/min-max/bloom since Hive 1.2 — CONFIRMED; 64MB stripe sourcing gap (Defect #1)
- [x] C9. sort → tighter min/max → skip more — SOUND & hedged

### Also-check
- [x] U. URLs resolve — all cited new-footer URLs 200 (orc spec-intro 404 but NOT cited)
- [x] X. Cross-refs — ALL resolve
- [x] T. Traditional Chinese only — no Simplified
- [x] O. No over-claims — clean (claims hedged where source is silent)

---

## Findings (appended live)

### Verified clean
- **X (cross-refs)** ALL resolve: §1.2 (ch01:61), §1.9 (ch01:286), §3.3 (ch03:80), §5.2 (ch05:34), §5.4 (ch05:124), §5.6 (ch05:260), §5.8 (ch05:312), ch06 file exists, item 7 (ch01:325). OK.
- **T (Trad Chinese)** No Simplified chars in new passages. (`值`/`行` flags were false positives — identical in both scripts.) OK.
- **C2 (Parquet structure)** CONFIRMED. parquet.apache.org/docs/file-format/ lists File→Row Group→Column Chunk, Data Pages, Page Index, Bloom Filter, Encodings as documented topics; footer metadata "written after the data". Metadata page confirms FileMetaData footer + Statistics. min/max/null per-RG/per-column is the documented Statistics struct. Page index → page-level skip is a real feature. OK.
- **C3 (defaults vs recommendation)** CONFIRMED & the distinction is stated correctly.
  - parquet-java `ParquetWriter.DEFAULT_BLOCK_SIZE = 128*1024*1024` (128MB) → library default for row group. ✔
  - parquet-java `ParquetProperties.DEFAULT_PAGE_SIZE = 1024*1024` (1MB) → library default for page. ✔ (NOTE: the Parquet *docs/configurations* page RECOMMENDS 8KB pages, NOT 1MB — but handbook says "預設 ~1MB", which is the LIBRARY DEFAULT, correctly distinct from the doc recommendation. Handbook does not cite the 8KB recommendation, which is fine.)
  - Parquet docs RECOMMEND 512MB–1GB row groups "Since an entire row group might need to be read, we want it to completely fit on one HDFS block. Therefore, HDFS block sizes should also be set to be larger." → handbook's framing ("是搭配 HDFS block 也設這麼大 的情境") is EXACTLY right. ✔
  - Handbook ch05:102 footer correctly says "128MB 是 parquet-hadoop 函式庫的預設值（parquet.block.size），Spark 寫出沿用" — matches DEFAULT_BLOCK_SIZE. ✔ Non-contradictory. OK.
- **C4 (Spark configs)** CONFIRMED verbatim from spark.apache.org/docs/latest/sql-data-sources-parquet.html: filterPushdown=true, enableVectorizedReader=true, columnarReaderBatchSize=4096, aggregatePushdown=false, mergeSchema=false, compression.codec=snappy. OK.
- **C6 (dictionary default-on)** CONFIRMED. parquet-java `ParquetProperties.DEFAULT_IS_DICTIONARY_ENABLED = true`. Encoding names RLE_DICTIONARY/PLAIN_DICTIONARY confirmed. (Note: the Parquet *spec* page frames dictionary as a "fallback that falls back to plain if too big" — but the writer default IS enabled, so handbook "Parquet 預設會字典化" is accurate at the library level.) OK.
- **C8 (ORC analog)** CONFIRMED.
  - stripe ≈ row group: ORC three-level (file/stripe/row). ✔
  - row index every 10000 rows: orc.apache.org/docs/indexes.html confirms "each set of 10,000 rows within a stripe". ✔ `orc.row.index.stride` default 10,000 confirmed via orc.apache.org/docs/hive-config.html. ✔
  - min/max stats at file/stripe/row level. ✔
  - bloom filters "As of Hive 1.2". ✔ (verbatim)
  - default stripe ~64MB: orc.stripe.size default = 67,108,864 bytes = 64 MiB, confirmed via orc.apache.org/docs/hive-config.html. ✔ ⚠️ BUT the handbook footer (ch05:102) cites ONLY orc.apache.org/docs/indexes.html for ORC; that page does NOT state the 64MB stripe default (it's on hive-config.html). Minor sourcing gap — see Defect list.
- **C9 (sort → tighter min/max → skip more)** SOUND. Directly follows from per-row-group min/max statistics; handbook hedges ("實際跳掉比例依資料分佈而異，無官方逐字數字") correctly. OK.
- **C7 (bloom filter hedge)** CONFIRMED appropriately hedged. Handbook says Parquet/ORC both support per-column bloom filters for high-cardinality equality where min/max can't skip, "屬進階、預設多半不開，要用時依你平台設定為指定欄開啟" — does NOT assert an exact Spark bloomFilter config default. Spark latest Parquet page has NO bloomFilter config mention, so staying hedged is correct. OK.
- **U (URLs)** All cited new-footer URLs resolve (200): parquet file-format, configurations; spark sql-data-sources-parquet; orc indexes; cloudera hive_spark_access_to_hive; spark hive-tables (implied, standard). No 404 on cited pages. (orc spec-intro.html 404'd but it is NOT cited.)

### Issues found (see Defect list at end)
- **C1 sourcing precision**: the CDP page cited for "Spark/Hive/Impala share ONE HMS" (hive_spark_access_to_hive.html) is actually about the Hive Warehouse Connector / Spark access to Hive managed+ACID+external tables; it does NOT *explicitly* state a single shared HMS across all three engines. The fact is true & standard CDP, and §1.1 hedges (defers the strong claim to ch06). Same citation reused in ch06. → minor, pre-existing pattern; the in-text ⚠️ already defers. Documented as Defect #2 (sourcing-precision, optional).
- **C5 vectorized-reader-auto-disable**: see Defect #1 below — needs the nested-vectorized-reader nuance checked.


---

## VERDICT: ACCURATE — minor fixes only (2 low-severity sourcing nits, no factual defects)

Every technical claim (C1–C9) is correct against authoritative sources. All Spark/Parquet/ORC
config defaults verified verbatim. All cited URLs resolve. Cross-refs all resolve. Traditional
Chinese clean. No over-claims — the author hedged precisely where official docs are silent
(page-skip ratios, vectorized-reader fallback by version, bloom-filter defaults). The 128MB/1MB
library-default vs 512MB–1GB recommendation distinction is stated correctly and is non-contradictory.

### DEFECTS (both low severity, sourcing precision only — content is factually correct)

1. **ch05 line 102 (§5.2 📚 footer) — ORC 64MB stripe default lacks a matching citation.**
   The footer cites only `orc.apache.org/docs/indexes.html` for ORC, but the in-text claim
   "stripe（≈ row group，預設約 64MB）" (line 100) is NOT on the indexes page — that page covers
   the 10000-row index, min/max stats, and bloom-filters-since-Hive-1.2 (all correctly cited), but
   the 64MB stripe default lives on `orc.apache.org/docs/hive-config.html` (orc.stripe.size =
   67,108,864 = 64 MiB). FIX: add `orc.apache.org/docs/hive-config.html` (orc.stripe.size 預設
   67108864=64MiB、orc.row.index.stride 預設 10000) to the ORC citation in the footer, OR soften the
   in-text "預設約 64MB" to "預設約 64MB（依平台設定）" if you prefer not to add a URL.

2. **ch01 line 57 (§1.1 📚 footer) — "三引擎共用同一個 Hive Metastore" is cited to a HWC page that
   doesn't explicitly state it.** `hive_spark_access_to_hive.html` documents the Hive Warehouse
   Connector / Spark access to Hive managed+ACID+external tables; it does not literally assert a
   single HMS shared across Spark+Hive+Impala. The fact is true and standard CDP, and the passage
   already hedges (⚠️ defers the strong claim to ch06, "本節只先建立心智模型"). Same citation is reused
   in ch06 §6.1/§6.5. SEVERITY: cosmetic. OPTIONAL FIX: this is a pre-existing handbook-wide pattern;
   if tightening, the cleanest single authoritative anchor for "shared HMS on CDP" is a CDP
   metastore/architecture page rather than the HWC page — but not required, given the in-text hedge.

### Non-defects deliberately checked and cleared
- "page 預設 ~1MB" is the parquet-java library default (DEFAULT_PAGE_SIZE=1MB), correctly distinct
  from the docs' 8KB *recommendation*; handbook does not conflate them. OK.
- `parquet.block.size` property name is correct (parquet-hadoop ParquetOutputFormat.BLOCK_SIZE) even
  though the configurations page phrases recommendations conceptually. OK.
- Vectorized-reader fallback for nested/complex types: latest Spark adds
  enableNestedColumnVectorizedReader=true, but 3.3.x still gates/limits nested vectorization and
  falls back for unsupported types; handbook's "依 Spark 版本而異" hedge covers this. OK.
- Dictionary "fallback to plain" spec wording vs "預設會字典化": writer default IS enabled
  (DEFAULT_IS_DICTIONARY_ENABLED=true), so handbook is right. OK.
