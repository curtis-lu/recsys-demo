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
        "WHERE snap_date = '${snap_date}'\n"
    )
    (sql / "feature_concat.sql").write_text(
        "--partition by: snap_date\n\n"
        "SELECT *\n"
        "FROM ${target_db}.feature_aum\n"
        "WHERE snap_date = '${snap_date}'\n"
    )
    return tmp_path


class TestRender:
    def test_substitutes_variables(self, tmp_sql_dir):
        renderer = SQLRenderer(tmp_sql_dir)
        result = renderer.render(
            "feature/feature_aum.sql", {"snap_date": "2024-01-31"}
        )
        assert "'2024-01-31'" in result
        assert "${snap_date}" not in result

    def test_multiple_variables(self, tmp_sql_dir):
        renderer = SQLRenderer(tmp_sql_dir)
        result = renderer.render(
            "feature/feature_concat.sql",
            {"snap_date": "2024-01-31", "target_db": "ml_feature"},
        )
        assert "ml_feature.feature_aum" in result
        assert "'2024-01-31'" in result

    def test_unresolved_variable_raises(self, tmp_sql_dir):
        renderer = SQLRenderer(tmp_sql_dir)
        with pytest.raises(ValueError, match="Unresolved template variables"):
            renderer.render("feature/feature_concat.sql", {"snap_date": "2024-01-31"})


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
    def test_single_partition(self):
        cfg = TableConfig(
            name="feature_aum",
            sql_file="feature/feature_aum.sql",
            partition_by=["snap_date"],
        )
        result = SQLRenderer.build_insert_overwrite(
            cfg, "--partition by: snap_date\n\nSELECT 1", "ml_feature"
        )
        assert result.startswith(
            "INSERT OVERWRITE TABLE ml_feature.feature_aum PARTITION (snap_date)"
        )
        assert "SELECT 1" in result
        assert "--partition by" not in result

    def test_multiple_partitions(self):
        cfg = TableConfig(
            name="label_ccard",
            sql_file="label/label_ccard.sql",
            partition_by=["prod_name", "snap_date"],
        )
        result = SQLRenderer.build_insert_overwrite(cfg, "SELECT 1", "ml_feature")
        assert "PARTITION (prod_name, snap_date)" in result
