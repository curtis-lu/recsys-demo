# Reader R2 re-review — ch08 newly-added footguns + §8.7 table

Reviewer stance: target reader (bank analyst/scientist, SQL-first, NO data-eng background).
Checking ONLY followability of the 4 new passages, not technical correctness.

---

## 1. §8.2 — `INSERT … SELECT` matches by position, not name (line 118–131)

Reading: "把 SELECT 的第 1、2、3 欄塞進目標表的第 1、2、3 欄——完全不看名字對不對" → very clear.
The "feat_a 的值寫進 feat_b 的格子" makes the corruption concrete for a SQL person. GOOD.
"上游多加一欄/調動欄序 → 沒有任何錯誤、下游照跑、結果全錯" → the silent-corruption danger lands hard.
❌/✓ code block: ❌ `SELECT *` vs ✓ explicit列出, ordered comments inline. Before/after is crisp.

ISSUE: `BY NAME` appears in the ✓ comment ("新版 Spark 可用 BY NAME 按名稱對齊") with NO example
and no gloss of the syntax. A reader who's never seen it can't act on it — and it's a teaser they
can't use. The "按名稱對齊" half-glosses the concept but not where it goes in the statement.
Minor: "裸欄序" (line 121 comment) is jargon-ish but recoverable from context.

VERDICT: LANDS. One nit (BY NAME under-specified).

---

## 2. §8.4 — don't write `current_date()` in scheduled/backfill SQL (line 227–237)

Reading: "補哪一天，就用哪一天的邏輯算" states the invariant up front. Then current_date()/now()
"取的是腳本實際執行的那天、不是你要補的那天" — the mechanism is explicit. GOOD.
❌/✓: ❌ `WHERE event_date = current_date()` vs ✓ param `'{{ var("run_date") }}'`. Clear.
"backfill 2026-03-01 卻拿今天的資料去算 → 補出來的歷史全錯、明天再跑又是另一個結果" — failure lands,
and "明天再跑又不一樣" nails the non-reproducibility angle nicely.
Fix ("所有『現在幾號』一律從外面當參數傳進來") is concrete and tied back to §8.2's var("run_date").

ISSUE: `logical_date` (Airflow) appears in the ✓ comment and in the prose with no gloss. A cron/SQL
reader doesn't know it = "the run's assigned date, passed in by Airflow". It's load-bearing (it's
the Airflow equivalent of the fix) but opaque. current_date() itself is adequately explained in prose.

VERDICT: LANDS. One nit (logical_date ungloss).

---

## 3. §8.5 — cron silent failure + `set -euo pipefail` (line 265–274)

Reading: opening contrast "Airflow 失敗會標紅/重試/通知；cron 預設不會——默默跑完回一個你沒在看的
exit code" sets the stakes well. The two shell defaults (①中間失敗後面照跑 ②pipe 只看最後一個) are
spelled out in plain language BEFORE the flag is shown — good ordering for a non-shell reader.
The flag's inline comment glosses all three letters: "-e 任一失敗就停 / -u 未設變數報錯 / pipefail
pipe 中間失敗也算失敗". This is the single best-glossed term in the new content.
"監控的前提是失敗真的會冒出來" + closing "都建立在沙上——壞了也不會有人知道" — the framing LANDS.

ISSUE: `hdfs dfs -touchz` in the code block has no gloss. A SQL-first reader won't know `touchz`
= "create an empty marker file at this HDFS path". The trailing comment ("真的成功才落 _SUCCESS")
explains the intent but not the command. _SUCCESS itself is fine — introduced at §8.3 line 175.
Minor: "exit code" appears unglossed here, but is intuitive enough from "回一個你沒在看的 exit code".

VERDICT: LANDS (best-explained of the four). One nit (touchz ungloss).

---

## 4. §8.7 — 常見維運踩雷（速查）table (line 319–339)

Reading: intro line "多數的共通特徵是『不報錯、只是默默算錯或默默不跑』" — the framing is clear and
is exactly the right unifying lens. The 4-column shape (症狀你會看到的 / 成因 / 修法 / 哪節) is
scannable and the §-links make it actionable.
Symptom column is phrased as observable outcomes ("重跑後資料變兩份", "回填出來的歷史是錯的、且重現
不出來", "cron 作業『成功』了其實半路就壞", "Impala 看不到剛寫的新資料") — these read like things you'd
actually SEE. GOOD.
Last-3-rows callout (line 339) correctly flags they're a ch09 preview. Helpful.

ISSUES:
- Row "新 DAG 一上線就把叢集塞爆" (line 330): §8.4 link is CORRECT — verified §8.4 line 202–205 fully
  covers catchup + the catchup=True footgun + --max-active-runs. (My initial concern was a false alarm.)
  So this row is well-supported; just not part of the 4 reviewed footgun passages.
- Row "可用 `BY NAME`" (line 327): same un-glossed teaser as passage 1 — table inherits the gap.
- Row "Impala REFRESH／寫完跑 ANALYZE" (line 334): `REFRESH`/`ANALYZE` assume §6.6/§8.6 context;
  fine as a速查 row since it links out, but "metadata 沒同步、統計過期" is two distinct causes folded
  into one row — slightly dense but acceptable for a cheatsheet.
No undefined terms that block comprehension at table level; all heavy terms link to a section.

VERDICT: LANDS. Scannable and useful. Nits are inherited teasers + one possibly-misleading §link.

---

## Conclusion

All four passages LAND on the danger/before-after/why axis. §8.5 is the strongest (every flag letter
glossed). The recurring defect (un-glossed infra term at first load-bearing use) shows up as small
teasers rather than blockers: `BY NAME`, `logical_date`, `hdfs dfs -touchz`. None stop comprehension
of the main point, but each leaves the reader unable to act on the exact fix mentioned.

Must-fix (readability), priority order:
1. `BY NAME` (line 125, 327): add a one-line example or gloss, e.g.
   "BY NAME＝在 INSERT 後加關鍵字讓 Spark 按欄名對齊（新版才有）；不確定版本就用明確欄位清單最穩。"
2. `hdfs dfs -touchz` (line 271): gloss inline, e.g.
   "# touchz＝在該 HDFS 路徑放一個空的標記檔（這裡就是 _SUCCESS）".
3. `logical_date` (line 234, 237, 328): one-clause gloss at first use, e.g.
   "Airflow 的 logical_date＝這次排程被指派的『邏輯日』，由 Airflow 自動傳進來，不是執行當天".

(Dropped: §8.7 catchup-row §-link — verified correct, §8.4 covers catchup fully.)
