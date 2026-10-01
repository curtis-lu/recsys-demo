"""防漂移：程式碼讀的 ETL stage 鍵，與八支範本 YAML 寫的鍵必須是同一組。

目的：以後有人在 ``_run_etl`` 或 ``SQLRunner.__init__`` 多讀一個 stage 鍵、卻沒寫進
範本 YAML（使用者看不見的預設值），這裡會紅；反過來 YAML 多寫一個沒人讀的鍵也會紅。

**這是靜態掃描，擋得住的範圍很窄，不要當成「所有新增的鍵都會被抓到」：**

擋得住：在 ``_run_etl`` 裡對 ``etl_config``、在 ``SQLRunner.__init__`` 裡對 ``config``，
用字面字串當鍵的 ``x.get("k")``、``x["k"]``、``"k" in x``、``x.pop("k")``、
``x.setdefault("k")``。

擋不住（新增的鍵會靜默綠，因為讀到的集合沒變、YAML 也沒人去加）：
- 換變數名（``cfg = etl_config; cfg.get("k")``）；
- 用常數當鍵（``etl_config.get(KEY)``）；
- 把設定交給 helper 讀（``helper(etl_config)``，例如 ``etl_stage_config_errors``、
  ``etl_cli_var_errors`` 都不在掃描範圍內）；
- 在 ``__init__`` 以外的方法讀（``self._config.get(...)``）；
- 串接取值（``params_etl.get(stage, {}).get("k")``）。
反過來，把現有鍵改成上面這些寫法，讀到的集合變小，測試會紅（誤報，但很大聲）。
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
    """``receiver`` 上以字面字串當鍵的 get／pop／setdefault、下標、``in`` 讀到的字串鍵。"""
    tree = ast.parse(textwrap.dedent(inspect.getsource(func)))
    keys: set[str] = set()
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr in ("get", "pop", "setdefault")
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == receiver
            and node.args
            and isinstance(node.args[0], ast.Constant)
            and isinstance(node.args[0].value, str)
        ):
            keys.add(node.args[0].value)
        elif (
            isinstance(node, ast.Compare)
            and len(node.ops) == 1
            and isinstance(node.ops[0], (ast.In, ast.NotIn))
            and isinstance(node.left, ast.Constant)
            and isinstance(node.left.value, str)
            and isinstance(node.comparators[0], ast.Name)
            and node.comparators[0].id == receiver
        ):
            keys.add(node.left.value)
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


def test_scanner_recognises_the_documented_read_forms():
    def sample(cfg):
        cfg.get("a")
        cfg["b"]
        "c" in cfg
        cfg.pop("d", None)
        cfg.setdefault("e", 1)

    assert _keys_read_from(sample, "cfg") == {"a", "b", "c", "d", "e"}
