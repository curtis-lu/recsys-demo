# 08 · 營運（一） — TARGET-READER review (live log)

**Reader profile**: bank data analyst/scientist, SQL-first, NO data-engineering background.
Has read ch 01–06 (Spark mental model, Spark UI, SQL tuning, config, storage, engine selection).
Only ever "written SQL in Hue." Now learning operations.
**I am NOT checking technical correctness** — only whether a smart SQL-first reader can follow.

Sections I'll read:
- 8.1 本章地圖：三層工具 (dbt/Airflow/cron)
- 8.2 冪等與可重跑
- 8.3 排程相依
- 8.4 回填
- 8.5 監控與退化
- 8.6 檔案與統計維護
- 8.7 取捨總表
- 8.8 一句話帶走
- 資料來源與精確度說明

Watch list (recurring defect = ungloss​ed infra terms at first load-bearing use):
dbt, Airflow, cron, sensor, DAG, idempotent, backfill, catchup, sentinel, SLA, YARN queue,
_SUCCESS, Jinja/var(), incremental model, fan-out.

---

## Reactions (inline as I read)

### Chapter header / 前提 (lines 1–9)
- Prereq box is clear, lists prior chapters with what each covered. Good.
- "優化線 vs 營運線" framing introduced here and "正確 > 可靠 > 可維護 > 快" — motivating. Clear.
- "產出對不對…留給第 09 章" — clean scope split. Good.

### 8.1 三層工具
- Opening: optimization-line vs operations-line restated with a concrete "ad-hoc 跑錯重跑就好 vs 排程跑錯隔天才發現+污染下游" — this LANDS. Good motivation.
- Mermaid diagram: three subgraphs (排程層 / 轉換層 / DATA). Reasonably graspable. The arrows "到點觸發 dbt run" help. BUT: a reader who has only used Hue may not know what "spark-sql / beeline 腳本" are — beeline is unglossed. Minor; it's in a diagram label.
- dbt bullet: glossed well from zero — "用 SQL SELECT 定義一張表怎麼算（一個 SELECT 就是一個 model）". "ref()" introduced and explained. "dbt 不是排程器" stated explicitly with WHY. GOOD — this is exactly the from-zero intro needed.
- Airflow bullet: glossed — "用 Python 定義…的開源排程器(scheduler)". Clear.
- cron bullet: glossed — "作業系統內建的定時器". The "Airflow 把可靠性內建、cron 沒有" contrast is set up as the recurring teaching device. Clear and motivating.
- DAG warning callout: EXCELLENT. Pre-empts the exact confusion (DAG used at two levels). This is the kind of gloss that's usually missing.
- ✅ Verdict 8.1: LANDS. All three tools introduced from zero. Only nit: "beeline" in diagram unglossed (minor, low priority).

### 8.2 冪等與可重跑
- "冪等(idempotent)" defined plainly: "同一步驟重跑幾次，結果都跟只跑一次一樣". Clear from zero. GOOD.
- append vs overwrite: concrete numbers (100萬→200萬 重複). Lands hard. GOOD.
- Mermaid contrast diagram reinforces. Clear.
- "指定分區值" safe default SQL: clear, the comment "不含分區欄" is a helpful gotcha. Good.
- partitionOverwriteMode static/dynamic footgun: the danger ("覆寫範圍比你以為的大…可能清掉的遠不只你這批") is stated and the safe pattern (回到指定分區值 / 先 SET dynamic) is given. This LANDS for a SQL reader. GOOD.
- dbt incremental block: FIRST place Jinja appears heavily ({{ config(...) }}, {% if is_incremental() %}, {{ var(...) }}). The reader "has never seen dbt/Jinja." Problems:
  - "incremental（增量）" glossed inline — ok.
  - `{{ ... }}` / `{% ... %}` syntax is NOT explained at all. A SQL-only reader will not know this is Jinja templating, what `{% if is_incremental() %}` does, or that `{{ var("run_date") }}` is a value substituted at run time. → NEEDS a one-line gloss: "dbt 用 Jinja 模板語法（`{{ }}` 取值、`{% %}` 流程控制）".
  - `is_incremental()` not explained (it's true only on incremental re-runs). Minor but the WHERE-only-on-incremental logic is invisible to a SQL reader.
  - `var("run_date")` — what var() is (a value you pass at run time via --vars) is only revealed later in §8.4. At first appearance here it's unexplained. → gloss at first use.
- "關鍵 caveat：該分區資料要全部重新 select 出來" — well explained, ties back to idempotency. Good.
- cron bullet: clear, "而且沒有人會告訴你" reinforces. Good.
- ⚠️ Verdict 8.2: mostly LANDS, but the Jinja/dbt block is the weak spot — Jinja syntax + var() + is_incremental() unglossed at first heavy use. NEEDS WORK (one gloss line fixes most of it).

### 8.3 排程相依
- "閘門(gate)" introduced, "就緒不是時間到了，是上游成功完成了" — the readiness concept LANDS. Good.
- "扇出(fan-out)給 N 個模型" — glossed inline with Chinese. The risk (上游壞→N下游一起遭殃) lands. Good.
- Mermaid GOOD/BAD contrast clear.
- "sensor" defined: "Airflow 裡一種一直等到某條件成立才放行的特殊任務". Good gloss.
- ExternalTaskSensor Python: readable to SQL person? The code has comments on every line (等哪個上游DAG / 只有成功才算就緒 / 等最多2小時). The `>>` operator ("wait_for_features >> train_model") has a comment explaining it = ordering. A SQL reader can follow WHAT it does even without Python. GOOD — comments carry it.
- cron bash: "sentinel 檔" introduced and glossed ("一個我好了的記號檔，如 HDFS 上的 _SUCCESS"). `_SUCCESS` shown concretely. The bash loop has a comment. A SQL reader can follow the intent (loop, check file, timeout, exit). `hdfs dfs -test -e` is unglossed but the comment makes intent clear. Acceptable.
- "這段手寫的等待＋逾時，正是 ExternalTaskSensor 內建做掉的" — great payoff, reinforces the recurring contrast. Good.
- ✅ Verdict 8.3: LANDS. sensor, gate, fan-out, sentinel, _SUCCESS all glossed. Python/bash carried by comments.

### 8.4 回填
- "回填(backfill)" glossed: "補算過去某段時間本該產出的資料" with 3 concrete examples. Clear from zero. GOOD.
- Three rules (分批/可中斷/別擾鄰) clear. "分批＋冪等是天生一對" ties back to §8.2. Good.
- "catchup" footgun: glossed ("catchup=True → DAG 一上線就自動補跑 start_date 到今天每個沒跑的區間"). The footgun (start_date 三年前 → 瞬間上千 run 塞爆) LANDS. The "所以一般設 catchup=False" advice is clear. GOOD.
- airflow dags backfill command: --max-active-runs 4 commented "(別擾鄰)". Clear.
- dbt backfill: now `var("run_date")` ties back ("呼應 §8.2"). The shell for-loop is readable. Good — but note this is where var() FINALLY gets context ("用變數把要補的分區傳進去"); should have been at first use in §8.2.
- cron: "全靠你寫迴圈＋記進度" — consistent with the recurring theme. Clear.
- 資源別擾鄰: "YARN 佇列(queue)" glossed inline with Chinese 佇列; cross-ref §4.7. "dynamic allocation 用完還回去" mentioned. A reader who read ch04 will connect. Clear. Cross-ref point lands.
- ✅ Verdict 8.4: LANDS. backfill, catchup, YARN queue all clear. (var() context arrives late but here it's fine.)

### 8.5 監控與退化
- "退化" framing: "10分鐘→半年後40分鐘還不自知" concrete. Lands.
- Three things to watch (跑多久/資料量/shuffle-spill) clear.
- History Server cross-ref §2.2: "比較同一支作業這週vs上週" — concrete, lands.
- "SLA" — appears in prose "撞上 SLA" (line 213) BEFORE it's glossed. Then in the Airflow block sla=timedelta(hours=1) has comment "超過1小時沒跑完→觸發SLA miss通知" which is effectively the gloss. The FIRST use (line 213 "撞上 SLA") is unglossed — a SQL reader may not know SLA = service-level agreement / a time budget. → gloss SLA at line 213 first use, e.g. "SLA（服務水準協議，這裡就是『該在幾點前跑完』的承諾）".
- retries/retry_delay: commented "(cron 沒有這個)". Good. The "老靠重試才成功＝惡化警報" insight lands.
- dbt run_results.json: clear enough.
- ⚠️ Verdict 8.5: mostly LANDS but SLA used in prose before glossed. NEEDS minor fix (gloss SLA at first use).

### 8.6 檔案與統計維護
- Opening "共用表是活的…不維護會慢慢爛" with three concrete rots (小檔/統計過期/過期分區), each cross-ref'd. Clear.
- "compaction（合併小檔）＝重寫" — the WHY (external Parquet/ORC 沒有 ALTER COMPACT) is explained via the ACID-table contrast. This LANDS for the key question. Good.
  - "ACID 交易表" — glossed? Says "Hive managed 才有 ALTER TABLE…COMPACT". "ACID 交易表" itself not defined but contrast with external Parquet is clear enough for the operational point. Borderline ok (ch06 §6.7 cross-ref covers it).
- REPARTITION(4) hint commented, ties to §5.5. Clear.
- "它正是 §8.2 的冪等覆寫" — nice reinforcement.
- ANALYZE: ties to §5.6, "產完表就跟著重算、排進同一支作業" — the WHEN-to-maintain answer. Clear.
- 清過期分區: DROP PARTITION with the external-table caveat (Metastore vs HDFS file). Honest. Clear.
- WHEN to do maintenance: "排進排程的例行公事，不是出事才做" + "產完表就跟著重算" — the question "does reader understand WHEN" is answered. Good.
- ✅ Verdict 8.6: LANDS. compaction=rewrite WHY is clear; WHEN is clear.

### 8.7 取捨總表 — clean summary, maps to each section. Clear.
### 8.8 一句話帶走 — good single-sentence recap + tool-layer reminder. Clear.
### 資料來源與精確度說明 — thorough, honest about simplifications. Good.

---

## VERDICT per section

- 8.1 三層工具 — **LANDS**. dbt/Airflow/cron all introduced from zero; "dbt 不是排程器" + WHY is explicit; DAG double-meaning callout is exemplary.
- 8.2 冪等與可重跑 — **NEEDS WORK** (small). Idempotent + append/overwrite + static/dynamic footgun all land. Weak spot: the dbt **Jinja** block is the first heavy Jinja use and `{{ }}` / `{% %}` / `is_incremental()` / `var()` are unglossed.
- 8.3 排程相依 — **LANDS**. gate/readiness/sensor/fan-out/sentinel/_SUCCESS all glossed; code carried by per-line comments.
- 8.4 回填 — **LANDS**. backfill/catchup footgun/YARN queue clear.
- 8.5 監控與退化 — **NEEDS WORK** (small). Degradation/History-Server land. Weak spot: **SLA** used in prose (line 213) before it's effectively glossed (line 225).
- 8.6 維護 — **LANDS**. compaction=rewrite WHY clear; WHEN clear.
- 8.7 / 8.8 / 來源 — **LANDS**.

Overall: strong chapter. The recurring "Airflow builds reliability in / cron makes you do it by hand" device works well as a motivator. Only two genuine readability gaps, both small and both the SAME class of defect this handbook keeps hitting (term unglossed at first load-bearing use).

## MUST-FIX (numbered, concrete)

1. **§8.2 — gloss Jinja before/at the first dbt block (lines 95–108).** A reader who "never saw dbt/Jinja" hits `{{ config(...) }}`, `{% if is_incremental() %}`, `{{ var("run_date") }}` with zero syntax intro. Add one line above the block: 「dbt model 用 Jinja 模板：`{{ }}` 是取值/設定、`{% %}` 是流程控制（如 if）；這些在 `dbt run` 時才展開成真正的 SQL。」

2. **§8.2 — gloss `var()` and `is_incremental()` at first use (line 106).** `{{ var("run_date") }}` only gets context in §8.4. Add inline: 「`var("run_date")` = 你在 `dbt run --vars` 時傳進來的值；`is_incremental()` 只在增量重跑時為真，所以這個 WHERE 只在重跑時生效。」

3. **§8.5 — gloss SLA at its first appearance (line 213, "撞上 SLA").** Currently the effective gloss is only the code comment on line 225. A SQL-first reader may not know SLA = a completion-time promise. Add at line 213: 「SLA（服務水準協議，這裡就是『該在幾點前跑完』的承諾）」.

## NICE-TO-HAVE (low priority, not blocking)

4. **§8.1 diagram — "beeline" unglossed** (line 30 label). It's only a diagram label and the meaning ("跑 SQL 腳本") is inferable, but a Hue-only reader won't know beeline. Could add "(Hive 的 SQL 命令列)" or drop it.
5. **§8.3 — `hdfs dfs -test -e`** unglossed in the bash loop (line 161,166). Comment carries intent so acceptable; a 3-word gloss "(測試檔案是否存在)" would remove the last friction.
