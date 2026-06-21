# Reviewer log — 08-operating-pipelines.md (technical accuracy)

Started: 2026-06-21. Reviewer: technical-accuracy pass.
Sources allowed: Apache Spark official docs, Cloudera CDP official docs, Apache Airflow official docs, dbt official docs. No blogs.

## Claims checklist

- [ ] C1. INSERT OVERWRITE TABLE t PARTITION(col='val') SELECT ... overwrites exactly that one partition; static value -> must NOT select partition column.
- [ ] C2. spark.sql.sources.partitionOverwriteMode default = static; dynamic overwrites only partitions in new data; Spark 3.3 datasource-table change (migration guide).
- [ ] C3. dbt-spark insert_overwrite: dynamically replaces partitions when partition_by set; must re-select all data for a partition; merge is Delta/Iceberg/Hudi only.
- [ ] C4. Airflow ExternalTaskSensor: waits for task/DAG in another DAG; params external_dag_id/external_task_id/allowed_states/timeout; default waits for success; now in providers-standard.
- [ ] C5. Airflow retries default 0, retry_delay default 5 min (BaseOperator); sla triggers SLA-miss; catchup controls backfill (default effectively True); chapter recommends catchup=False.
- [ ] C6. airflow dags backfill --start-date --end-date --max-active-runs exists and limits concurrent runs.
- [ ] C7. _SUCCESS marker file is a Hadoop output convention usable as readiness signal.
- [ ] C8. external (non-ACID) Parquet/ORC tables have NO ALTER TABLE COMPACT (ACID-only); compaction = rewrite via INSERT OVERWRITE; REPARTITION(4) hint controls output file count.
- [ ] C9. ANALYZE TABLE ... PARTITION ... COMPUTE STATISTICS and ALTER TABLE ... DROP PARTITION syntax correct for Spark 3.3.
- [ ] C10. compaction SQL example correct/safe (selecting partition column would be wrong).

## URL resolution checklist (footers)

- [ ] U1. docs.getdbt.com/docs/build/models
- [ ] U2. docs.getdbt.com/reference/dbt-jinja-functions/ref
- [ ] U3. airflow.apache.org/.../core-concepts/dags.html
- [ ] U4. spark.apache.org/docs/3.3.2/sql-ref-syntax-dml-insert-overwrite-table.html
- [ ] U5. spark.apache.org/docs/3.3.2/sql-migration-guide.html
- [ ] U6. docs.getdbt.com/reference/resource-configs/spark-configs
- [ ] U7. airflow.apache.org/docs/apache-airflow-providers-standard/stable/sensors/external_task_sensor.html
- [ ] U8. spark.apache.org/docs/latest/monitoring.html
- [ ] U9. airflow.apache.org/.../core-concepts/tasks.html
- [ ] U10. docs.getdbt.com/reference/artifacts/run-results-json
- [ ] U11. docs.getdbt.com/reference/commands/run
- [ ] U12. docs-archive.cloudera.com/runtime/7.1.0/using-hiveql/topics/hive_hive_3_tables.html
- [ ] U13. spark.apache.org/docs/3.3.2/sql-ref-syntax-aux-analyze-table.html
- [ ] U14. spark.apache.org/docs/3.3.2/sql-ref-syntax-ddl-alter-table.html

## Other checks
- [ ] Internal cross-refs (5.5, 5.6, 5.8, 6.7, 4.7, 2.2, ch09, ch10) point to real sections.
- [ ] Traditional Chinese only (no Simplified).
- [ ] Code snippets syntactically valid/idiomatic.
- [ ] No over-claims.

## Findings (appended as verified)


### Claims
- [x] C1 CONFIRMED. archive 3.3.2 INSERT TABLE page: static PARTITION(col=value) -> partition col omitted from SELECT; overwrites that partition. Example "INSERT OVERWRITE students PARTITION (student_id=222222) SELECT name, address ...".
- [~] C2 PARTIAL/DEFECT. default=static and dynamic semantics: correct (established Spark behavior since 2.4). BUT "Spark 3.3 changed datasource-table INSERT OVERWRITE...PARTITION to only-matching" is FALSE. Migration guide: that change is "Upgrading from Spark SQL 2.0 to 2.1", NOT 3.2->3.3. The only INSERT OVERWRITE bullet in 3.2->3.3 is about INSERT OVERWRITE DIRECTORY using built-in writer. => DEFECT D1 (body L91 + footer L114).
- [x] C3 CONFIRMED. dbt spark-configs page: insert_overwrite "dynamically replaces all partitions included in your query"; caveat verbatim "Be sure to re-select all of the relevant data for a partition"; merge requires file_format delta/iceberg/hudi.
- [x] C4 CONFIRMED. ExternalTaskSensor waits for task in another DAG; params external_dag_id/external_task_id/allowed_states/timeout; default allowed_states=['success']; external_task_id=None -> waits whole DAG; import airflow.providers.standard.sensors.external_task matches.
- [~] C5 PARTIAL/DEFECT. retries default 0 + retry_delay default 300s(5min) CONFIRMED (BaseOperator, all versions). catchup CONFIRMED (Airflow2=True default, Airflow3=False). BUT `sla` REMOVED in Airflow 3.0 (tasks.html stable now = Airflow 3.2.2: "The SLA feature from Airflow 2 has been removed in 3.0 ... replaced in Airflow 3.1 with Deadlines Alerts"). Chapter's sla= example + footer cite describe Airflow 2.x but cite stable(=3.x) URL. => DEFECT D2.
- [x] C6 DEFECT. `airflow dags backfill --start-date --end-date` is Airflow 2.x. In Airflow 3 (stable docs) command is `airflow backfill create --dag-id --from-date --to-date --max-active-runs`. --max-active-runs still exists. => DEFECT D3 (body L188-192 + footer).
- [x] C7 CONFIRMED. _SUCCESS = FileOutputCommitter SUCCEEDED_FILE_NAME; config mapreduce.fileoutputcommitter.marksuccessfuljobs correct; default-on hedge fine.
- [x] C8 CONFIRMED. CDP (hive-compaction-tasks, hive_3_internals): ALTER TABLE COMPACT 'major'/'minor' is for ACID/transactional tables; external non-ACID has none. REPARTITION hint controls output file count (3.3.2 select-hints page 200). NOTE: cited URL U12 (hive_hive_3_tables.html) confirms ACID-vs-external but does NOT contain COMPACT syntax -> citation-scope nit D6.
- [x] C9 CONFIRMED. archive 3.3.2: ANALYZE TABLE [partition_spec] COMPUTE STATISTICS correct; ALTER TABLE ... DROP [IF EXISTS] partition_spec [PURGE] correct.
- [x] C10 CONFIRMED. compaction example correct/safe: static PARTITION(snapshot_date='...'), SELECT omits snapshot_date, REPARTITION(4) valid. Idiomatic.

### URLs (footers)
- U1 docs.getdbt.com/docs/build/models -> 200 OK
- U2 docs.getdbt.com/reference/dbt-jinja-functions/ref -> 200 OK
- U3 airflow .../core-concepts/dags.html -> 200 OK (now Airflow 3.2.2)
- U4 spark 3.3.2 sql-ref-syntax-dml-insert-overwrite-table.html -> 404 (live AND archive). DOUBLE DEFECT D4: (a) slug renamed to sql-ref-syntax-dml-insert-table.html in 3.3; (b) live spark.apache.org/docs/3.3.2 tree is gone, only resolves on archive.apache.org/dist/spark/docs/3.3.2/.
- U5 spark 3.3.2 sql-migration-guide.html -> 404 on live; 200 on archive.apache.org. DEFECT D5.
- U6 docs.getdbt.com/reference/resource-configs/spark-configs -> 200 OK
- U7 airflow-providers-standard .../external_task_sensor.html -> 200 OK
- U8 spark /docs/latest/monitoring.html -> 200 OK
- U9 airflow .../core-concepts/tasks.html -> 200 OK (but now Airflow 3.x where SLA removed; see D2)
- U10 docs.getdbt.com/reference/artifacts/run-results-json -> 200 OK
- U11 docs.getdbt.com/reference/commands/run -> 200 OK
- U12 docs-archive.cloudera.com .../hive_hive_3_tables.html -> 200 OK (but no COMPACT content; nit D6)
- U13 spark 3.3.2 sql-ref-syntax-aux-analyze-table.html -> 404 live; 200 archive. DEFECT D5.
- U14 spark 3.3.2 sql-ref-syntax-ddl-alter-table.html -> 404 live; 200 archive. DEFECT D5.

### Other
- Internal anchors 2.2/4.7/5.4/5.5/5.6/5.8/6.7 all exist in ch02/04/05/06. OK.
- Forward-refs ch07, ch09 (incl. 9.5), ch10 (10-scenario-playbooks.md) -> target files DO NOT EXIST yet (planned; index lists them, ch07 marked "撰寫順序暫緩"). Not a hard defect; flag D7 (broken links until those chapters land).
- Traditional Chinese: clean (only 跟/脆 matched, both valid Traditional). No Simplified.
- Code snippets: Spark SQL / dbt Jinja / Airflow Python / bash all syntactically valid & idiomatic (modulo Airflow 2.x-vs-3.x command/param drift, D2/D3).

## VERDICT: minor-to-real fixes. Core Spark/dbt SQL claims accurate; defects are (1) wrong Spark-version attribution (D1), (2) Airflow 2.x-vs-3.x drift now mismatching cited stable docs (D2/D3), (3) all Spark 3.3.2 links 404 on live domain + one wrong slug (D4/D5).
