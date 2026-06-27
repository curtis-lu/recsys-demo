import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from anchor_check import slugify, heading_slugs, check_dir


def test_slugify_known_handbook_anchors():
    assert slugify("全書地圖") == "全書地圖"
    assert slugify("場景速查：依你的工作型態") == "場景速查依你的工作型態"
    assert slugify("5.5 管好檔案大小：小碎檔問題") == "55-管好檔案大小小碎檔問題"
    assert slugify("Spark 怎麼跑你的 SQL") == "spark-怎麼跑你的-sql"
    assert slugify("用 `EXPLAIN` 找瓶頸") == "用-explain-找瓶頸"
    assert slugify("（進階）何時與如何改用 PySpark DataFrame API") == \
        "進階何時與如何改用-pyspark-dataframe-api"


def test_heading_slugs_dedup_and_skips_fenced_code():
    md = "# A\n```\n## not-a-heading\n```\n## A\n"
    assert heading_slugs(md) == ["a", "a-1"]


def test_check_dir_detects_broken_anchor(tmp_path):
    (tmp_path / "a.md").write_text("# T\n[x](b.md#missing)\n", encoding="utf-8")
    (tmp_path / "b.md").write_text("## 場景速查：依你的工作型態\n", encoding="utf-8")
    assert any("missing anchor" in e for e in check_dir(tmp_path))


def test_check_dir_passes_valid_anchor(tmp_path):
    (tmp_path / "a.md").write_text("[x](b.md#場景速查依你的工作型態)\n", encoding="utf-8")
    (tmp_path / "b.md").write_text("## 場景速查：依你的工作型態\n", encoding="utf-8")
    assert check_dir(tmp_path) == []


def test_check_dir_detects_missing_file(tmp_path):
    (tmp_path / "a.md").write_text("[x](nope.md)\n", encoding="utf-8")
    assert any("missing file" in e for e in check_dir(tmp_path))


def test_check_dir_ignores_links_in_code_fence(tmp_path):
    (tmp_path / "a.md").write_text("# T\n```\n[x](nope.md)\n```\n", encoding="utf-8")
    assert check_dir(tmp_path) == []


def test_check_dir_skips_dot_directories(tmp_path):
    (tmp_path / ".reviews").mkdir()
    (tmp_path / ".reviews" / "note.md").write_text("[x](nope.md)\n", encoding="utf-8")
    (tmp_path / "ok.md").write_text("# Fine\n", encoding="utf-8")
    assert check_dir(tmp_path) == []
