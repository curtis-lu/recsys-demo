"""Tests for SQL renderer."""

import pytest

from recsys_tfb.pipelines.source_etl.models import TableConfig
from recsys_tfb.pipelines.source_etl.sql_renderer import SQLRenderer


@pytest.fixture()
def tmp_sql_dir(tmp_path):
    """Create a temporary SQL directory with sample files."""
    sql = tmp_path / "feature"
    sql.mkdir()
    (sql / "feature_aum.sql").write_text(
        "--partition by: snap_date\n\n"
        "SELECT cust_id, total_aum\n"
        "FROM feature_store.feat_aum\n"
        "WHERE snap_date = '${target_date}'\n"
    )
    (sql / "feature_concat.sql").write_text(
        "--partition by: snap_date\n\n"
        "SELECT *\n"
        "FROM ${target_db}.feature_aum\n"
        "WHERE snap_date = '${target_date}'\n"
    )
    return tmp_path


class TestRender:
    def test_substitutes_variables(self, tmp_sql_dir):
        renderer = SQLRenderer(tmp_sql_dir)
        result = renderer.render(
            "feature/feature_aum.sql", {"target_date": "2024-01-31"}
        )
        assert "'2024-01-31'" in result
        assert "${target_date}" not in result

    def test_multiple_variables(self, tmp_sql_dir):
        renderer = SQLRenderer(tmp_sql_dir)
        result = renderer.render(
            "feature/feature_concat.sql",
            {"target_date": "2024-01-31", "target_db": "ml_feature"},
        )
        assert "ml_feature.feature_aum" in result
        assert "'2024-01-31'" in result

    def test_unresolved_variable_raises(self, tmp_sql_dir):
        renderer = SQLRenderer(tmp_sql_dir)
        with pytest.raises(ValueError, match="Unresolved template variables"):
            renderer.render("feature/feature_concat.sql", {"target_date": "2024-01-31"})


class TestStripHeaderComments:
    def test_strips_leading_comments(self):
        sql = "--partition by: snap_date\n\nSELECT 1"
        assert SQLRenderer.strip_header_comments(sql) == "SELECT 1"

    def test_no_comments(self):
        sql = "SELECT 1\nFROM t"
        assert SQLRenderer.strip_header_comments(sql) == "SELECT 1\nFROM t"

    def test_preserves_inline_comments(self):
        sql = "--header\nSELECT 1 -- inline\nFROM t"
        assert SQLRenderer.strip_header_comments(sql) == "SELECT 1 -- inline\nFROM t"


class TestBuildInsertOverwrite:
    def test_partition_names_only_no_types(self):
        cfg = TableConfig(
            name="feature_aum",
            sql_file="feature/feature_aum.sql",
            partition_by={"snap_date": "DATE"},
        )
        result = SQLRenderer.build_insert_overwrite(cfg, "SELECT 1", "ml_feature")
        assert result.startswith(
            "INSERT OVERWRITE TABLE ml_feature.feature_aum PARTITION (snap_date)"
        )
        assert "PARTITION (snap_date DATE)" not in result

    def test_multiple_partitions(self):
        cfg = TableConfig(
            name="label_ccard",
            sql_file="label/label_ccard.sql",
            partition_by={"prod_name": "STRING", "snap_date": "DATE"},
        )
        result = SQLRenderer.build_insert_overwrite(cfg, "SELECT 1", "ml_feature")
        assert "PARTITION (prod_name, snap_date)" in result


class TestBuildAlignedSelect:
    def test_single_partition_cast_and_order(self):
        result = SQLRenderer.build_aligned_select(
            select_sql="SELECT cust_id, amt, snap_date FROM t",
            select_columns=["cust_id", "amt", "snap_date"],
            partition_by={"snap_date": "DATE"},
        )
        assert "CAST(snap_date AS DATE) AS snap_date" in result
        assert result.index("cust_id") < result.index("CAST(snap_date")

    def test_multi_partition_in_config_order(self):
        result = SQLRenderer.build_aligned_select(
            select_sql="SELECT cust_id, prod_name, snap_date FROM t",
            select_columns=["cust_id", "prod_name", "snap_date"],
            partition_by={"prod_name": "STRING", "snap_date": "DATE"},
        )
        assert result.index("CAST(prod_name") < result.index("CAST(snap_date")

    def test_case_insensitive(self):
        result = SQLRenderer.build_aligned_select(
            select_sql="SELECT CUST_ID, Snap_Date FROM t",
            select_columns=["CUST_ID", "Snap_Date"],
            partition_by={"snap_date": "DATE"},
        )
        assert "CAST(Snap_Date AS DATE) AS snap_date" in result

    def test_missing_partition_raises(self):
        with pytest.raises(ValueError, match="missing from SELECT output"):
            SQLRenderer.build_aligned_select(
                select_sql="SELECT cust_id FROM t",
                select_columns=["cust_id"],
                partition_by={"snap_date": "DATE"},
            )

    def test_strips_header_comments(self):
        result = SQLRenderer.build_aligned_select(
            select_sql="--partition by: snap_date\nSELECT snap_date FROM t",
            select_columns=["snap_date"],
            partition_by={"snap_date": "DATE"},
        )
        assert "--partition by" not in result


class TestBuildHiveCtas:
    def test_emits_stored_as_parquet_not_using(self):
        cfg = TableConfig(
            name="feature_aum",
            sql_file="feature/feature_aum.sql",
            partition_by={"snap_date": "DATE"},
        )
        result = SQLRenderer.build_hive_ctas(cfg, "SELECT 1", "ml_feature")
        assert "STORED AS PARQUET" in result
        assert "USING PARQUET" not in result
        # Hive CTAS: PARTITIONED BY takes names only; types are inferred from SELECT.
        assert "PARTITIONED BY (snap_date)" in result
        assert "CREATE TABLE ml_feature.feature_aum" in result

    def test_multi_partition_names(self):
        cfg = TableConfig(
            name="label_ccard",
            sql_file="label/label_ccard.sql",
            partition_by={"prod_name": "STRING", "snap_date": "DATE"},
        )
        result = SQLRenderer.build_hive_ctas(cfg, "SELECT 1", "ml_feature")
        # Hive CTAS: names only, preserving config order.
        assert "PARTITIONED BY (prod_name, snap_date)" in result
