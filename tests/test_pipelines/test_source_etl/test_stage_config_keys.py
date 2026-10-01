"""防漂移：程式碼讀的 ETL stage 鍵，與八支範本 YAML 寫的鍵必須是同一組。

目的：以後有人在 ``_run_etl`` 或 ``SQLRunner.__init__`` 多讀一個 stage 鍵、卻沒寫進
範本 YAML（使用者看不見的預設值），這裡會紅；反過來 YAML 多寫一個沒人讀的鍵也會紅。
"""

import ast
import inspect
import textwrap
from pathlib import Path

import pytest
import yaml

from recsys_tfb import __main__ as main_mod
from recsys_tfb.pipelines.source_etl.sql_runner import SQLRunner

REPO_ROOT = Path(__file__).resolve().parents[3]

EXPECTED_STAGE_KEYS = {
    "target_dates",
    "rendered_sql_dir",
    "variables",
    "source_checks",
    "tables",
    "audit",
}

STAGES = ("feature", "label", "sample_pool", "inference_population")
YAML_FILES = [
    REPO_ROOT / root / f"parameters_{stage}_etl.yaml"
    for root in ("conf/base", "examples/ad/conf/base")
    for stage in STAGES
]


def _keys_read_from(func, receiver: str) -> set[str]:
    """``receiver.get("<鍵>", ...)`` 與 ``receiver["<鍵>"]`` 讀到的字串鍵。"""
    tree = ast.parse(textwrap.dedent(inspect.getsource(func)))
    keys: set[str] = set()
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "get"
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == receiver
            and node.args
            and isinstance(node.args[0], ast.Constant)
            and isinstance(node.args[0].value, str)
        ):
            keys.add(node.args[0].value)
        elif (
            isinstance(node, ast.Subscript)
            and isinstance(node.value, ast.Name)
            and node.value.id == receiver
            and isinstance(node.slice, ast.Constant)
            and isinstance(node.slice.value, str)
        ):
            keys.add(node.slice.value)
    return keys


def test_code_reads_exactly_the_documented_stage_keys():
    read = _keys_read_from(main_mod._run_etl, "etl_config") | _keys_read_from(
        SQLRunner.__init__, "config"
    )
    assert read == EXPECTED_STAGE_KEYS, (
        f"程式碼讀的鍵多了 {sorted(read - EXPECTED_STAGE_KEYS)}、"
        f"少了 {sorted(EXPECTED_STAGE_KEYS - read)}。多讀的鍵要先寫進八支範本 YAML"
        f"（含註解）並加進 EXPECTED_STAGE_KEYS；少讀的鍵要從 YAML 拿掉。"
    )


@pytest.mark.parametrize("path", YAML_FILES, ids=lambda p: f"{p.parent.parent.parent.name}/{p.name}")
def test_every_template_yaml_declares_exactly_the_stage_keys(path):
    stage = path.name[len("parameters_"):-len(".yaml")]
    block = yaml.safe_load(path.read_text(encoding="utf-8"))[stage]
    assert set(block) == EXPECTED_STAGE_KEYS, (
        f"{path}: 多了 {sorted(set(block) - EXPECTED_STAGE_KEYS)}、"
        f"少了 {sorted(EXPECTED_STAGE_KEYS - set(block))}"
    )
