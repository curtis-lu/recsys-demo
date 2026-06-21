# Re-review R2 — 08-operating-pipelines.md (NEW content only)

Reviewer: technical-accuracy reviewer (R2), focused re-review.
Scope: ONLY the newly-added passages (rest already passed).
Authoritative sources only: Apache Spark docs, Cloudera CDP docs, Apache Airflow docs, dbt docs, POSIX/GNU bash.

## Checklist

- [ ] §8.2 footgun: INSERT…SELECT matches columns BY POSITION not name
  - [ ] positional (ordinal) matching is correct for Spark SQL INSERT/INSERT OVERWRITE
  - [ ] adding/reordering columns silently misaligns (no error)
  - [ ] `BY NAME` clause exists in recent Spark (3.5/4.x) — verify version
  - [ ] hedging "新版 Spark" appropriate for 3.3.2-baseline handbook
  - [ ] ❌/✓ code correct
  - [ ] does this claim need its OWN citation?
- [ ] §8.4 footgun: current_date() in scheduled/backfill SQL breaks reproducibility
  - [ ] claim sound
  - [ ] pass dates as params recommendation correct
- [ ] §8.5 footgun: cron silent failure + bash snippet
  - [ ] cron does not fail loudly (correct characterization)
  - [ ] set -e (stop on error) semantics correct
  - [ ] set -u (unset var errors) semantics correct
  - [ ] set -o pipefail (pipe middle failure counts) semantics correct
  - [ ] hdfs dfs -touchz creates zero-length file (-touchz vs -touch)
- [ ] §8.7 table — verify EVERY row cause→fix + §-pointer
  - [ ] all §-pointers exist (§8.2-8.6, §9.3, §9.4, §6.6, §5.4-5.5)
- [ ] Traditional Chinese only (flag Simplified)

## Checklist (resolved)

- [x] §8.2 footgun: INSERT…SELECT matches columns BY POSITION not name — CONFIRMED
  - [x] positional matching is Spark default — CONFIRMED (latest docs: "match columns by position (ordinal) by default")
  - [x] adding/reordering columns silently misaligns — CONFIRMED (no name check; reorder = wrong slot, no error)
  - [x] `BY NAME` exists in recent Spark — CONFIRMED. SPARK-42750 "Support INSERT INTO by name", landed/released in **Spark 3.5.0** (reported vs 3.4.0). NOT in 3.3.2 (3.3.2 syntax line has NO `[BY NAME]`).
  - [x] hedge "新版 Spark" appropriate for 3.3.2 baseline — YES. Handbook never claims a version number; never claims 3.3.2 has it. Hedge is accurate & conservative.
  - [x] ❌/✓ code correct — YES. `SELECT *` ❌ vs explicit column list ✓ is the right fix; both are valid Spark SQL.
  - [x] needs own citation? — Footer §8.2 already cites the INSERT syntax page (which IS where BY-NAME / positional behavior is documented in latest). Adequate. Minor: could add an explicit BY-NAME version note, but the existing footer link is the authoritative source. Not a defect.
- [x] §8.4 footgun: current_date() breaks reproducibility — CONFIRMED sound. current_date()/now() bind to wall-clock run day, not the logical/backfill date; passing date as param is the standard fix. Correct.
- [x] §8.5 footgun: cron silent failure + bash — CONFIRMED
  - [x] cron does not fail loudly — CORRECT (cron just runs, exit code unseen unless MAILTO/redirect)
  - [x] set -e (任一指令失敗就停) — CORRECT for the simple sequential script shown (no &&-chain / if / || that would suppress -e)
  - [x] set -u (用到未設變數就報錯) — CORRECT
  - [x] set -o pipefail (pipe 中間失敗也算失敗) — CORRECT (default reflects only last cmd's status; pipefail fixes)
  - [x] hdfs dfs -touchz creates zero-length file — CORRECT. `-touchz` = create zero-length (errors if exists non-empty). `-touch` = update timestamps (wrong tool for a marker). Handbook used the right one.
- [x] §8.7 table — all rows + §-pointers
  - [x] §-pointers to EXISTING chapters all correct: §8.2, §8.3, §8.4, §8.5, §8.6 (intra-doc, all present); §6.6 = Impala REFRESH/INVALIDATE (verified line 150) ✓; §5.5 = small files (verified) ✓; §5.4 = partition design ✓
  - [!] §9.3 and §9.4 point to `09-data-product-correctness.md` which DOES NOT EXIST YET (forward reference to unwritten chapter). Section numbers cannot be verified. See defect list — caveat, not hard defect (whole file treats ch.09 as forthcoming "下一章").
  - [x] every cause→fix technically correct (see row-by-row below)
- [x] Traditional Chinese only — CONFIRMED, zero Simplified chars (python scan: NONE)

### §8.7 row-by-row (cause→fix correctness)
1. INSERT INTO append → dup / fix INSERT OVERWRITE partition — correct (§8.2)
2. dynamic overwrite wipes whole table / static default → SET dynamic — correct (§8.2)
3. positional column misalign → explicit column list, BY NAME — correct (§8.2)
4. current_date() backfill wrong → param date — correct (§8.4)
5. downstream reads half-baked → ExternalTaskSensor/ref()/_SUCCESS — correct (§8.3)
6. catchup=True floods cluster → catchup=False + backfill --max-active-runs — correct (§8.4)
7. cron "succeeds" but broke → set -euo pipefail + _SUCCESS — correct (§8.5)
8. retries amplify non-idempotent → make idempotent first — correct (§8.2+§8.5)
9. job hangs, no timeout → set timeout on sensor/long jobs — correct (§8.3)
10. Impala can't see new data → REFRESH + ANALYZE — correct (§6.6+§8.6)
11. queries slower, bad plan → compaction+ANALYZE+drop old partitions — correct (§8.6+§5.5)
12. schema change breaks N readers → add-only / version — correct in substance (→§9.4, unwritten ch)
13. trains well/serves poorly → time-point leakage / read only pre-snapshot — correct in substance (→§9.3, unwritten ch)

## Findings / Defects

D1 (minor, forward-ref caveat): §8.7 last two rows point to §9.3 / §9.4 in `09-data-product-correctness.md`, which is not yet written. The §-numbers can't be verified. Consistent with rest of file (ch.09 is "下一章, forthcoming") so acceptable as-is, but the specific §9.3/§9.4 numbers may drift when ch.09 is authored — re-check anchors once ch.09 exists.

D2 (optional, not required): The BY-NAME positional-insert behavior is load-bearing and currently leans on the §8.2 footer's generic INSERT-syntax link. That link IS authoritative (latest docs document both positional default and BY NAME). A one-line "BY NAME 自 Spark 3.5 起 (SPARK-42750)，3.3.2 無此語法" would sharpen it, but NOT a defect — the claim is correct and sourced.

## VERDICT (new content only): ACCURATE — minor/optional only

No technical defects. Positional-insert + BY NAME claim: correct and version-appropriately hedged (BY NAME = Spark 3.5.0 / SPARK-42750, absent in 3.3.2 — handbook never overclaims). hdfs dfs -touchz: correct command. set -euo pipefail: all three semantics correct. current_date() reproducibility: sound. §8.7: every cause→fix correct; all §-pointers to existing chapters correct. Traditional Chinese throughout. Only items: D1 (forward-ref to unwritten ch.09 §9.3/§9.4 — recheck later) and D2 (optional sharpening of BY-NAME version note).
