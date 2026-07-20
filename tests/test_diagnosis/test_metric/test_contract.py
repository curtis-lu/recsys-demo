"""診斷契約的測試：registry 形狀、必要符號檢查、常數唯一來源。

最後一條（``test_every_registered_diagnosis_satisfies_contract``）是**整套契約
真正的價值所在**：它是報表層「不必認識任何單一診斷」這個宣稱的執行點。前面
幾條驗的是 ``check_module`` 這個函式本身正確，只有這條驗的是 registry 裡的
名字**真的**都滿足契約。
"""
from __future__ import annotations

import importlib
import inspect
import subprocess
import sys
import types

import pytest

from recsys_tfb.diagnosis.metric import contract


def test_registry_is_exactly_the_planned_diagnoses():
    assert contract.DIAGNOSES == ("config_shift", "item_ability", "model_capacity")


def test_registry_has_no_duplicates():
    """順序即閱讀順序，也決定 HTML 檔名前綴——重複會讓兩頁搶同一個檔名。"""
    assert len(contract.DIAGNOSES) == len(set(contract.DIAGNOSES))


def test_check_module_accepts_a_module_with_every_required_symbol():
    """兩個函式必須寫出契約要求的參數名——``*a, **k`` 不算數。

    這裡原本是 ``lambda *a, **k``，在 ``check_module`` 只檢查符號存在的年代
    可以過。加上簽章檢查之後改成具名參數：一個吞掉所有參數的簽章正是這條
    檢查要擋的東西（它讓任何呼叫形狀看起來都成立），假模組不該示範它。
    """
    mod = types.SimpleNamespace(
        NAME="fake",
        TITLE="假診斷",
        SCOPE=object(),
        compute=lambda diagnosis_sample, parameters: {},
        render=lambda result, parameters: None,
    )
    contract.check_module(mod)  # 不應 raise


def test_check_module_names_the_missing_symbol():
    """訊息必須含缺的符號名——只說「不符合契約」的話，補的人得自己翻原始碼。"""
    mod = types.SimpleNamespace(
        NAME="fake",
        TITLE="假診斷",
        compute=lambda *a, **k: {},
        render=lambda *a, **k: None,
    )
    with pytest.raises(AttributeError, match="SCOPE"):
        contract.check_module(mod)


def test_check_module_reports_all_missing_symbols_at_once():
    """collect-all：一次補完，不要補一個跑一次才知道還缺下一個。"""
    mod = types.SimpleNamespace(NAME="fake")
    with pytest.raises(AttributeError) as exc:
        contract.check_module(mod)
    message = str(exc.value)
    for symbol in ("TITLE", "SCOPE", "compute", "render"):
        assert symbol in message


def test_check_module_rejects_a_wrong_compute_signature():
    """簽章形狀是 Task 2.2 默默立的；不檢查的話後四項診斷寫錯照樣綠。

    ``match`` 挑 ``diagnosis_sample``——只有簽章檢查的訊息會出現這個字，缺符號
    那條訊息不會，避免 pattern 被別條規則的訊息滿足而假綠。
    """
    mod = types.SimpleNamespace(
        NAME="fake", TITLE="假診斷", SCOPE=object(),
        compute=lambda diagnosis_sample, parameters, extra: {},
        render=lambda result, parameters: None,
    )
    with pytest.raises(TypeError, match="diagnosis_sample"):
        contract.check_module(mod)


def test_check_module_rejects_a_wrong_render_signature():
    """參數**名字**也要對：report_builder 之後若改用關鍵字呼叫，一個
    ``render(payload, ...)`` 只有在真的跑報表時才炸，而那是 pipeline 最尾端。"""
    mod = types.SimpleNamespace(
        NAME="fake", TITLE="假診斷", SCOPE=object(),
        compute=lambda diagnosis_sample, parameters: {},
        render=lambda payload, parameters: None,
    )
    with pytest.raises(TypeError, match="payload"):
        contract.check_module(mod)


#: 每項診斷的 ``compute`` 期望參數名——**寫死**，不是從 ``contract.
#: compute_params_for`` 動態導出（那樣會讓「常數與模組一起被改掉」的漂移測不
#: 出來，見下方 docstring）。多數診斷吃共用抽樣，``model_capacity`` 宣告了
#: 自己的 ``INPUTS``（不吃 ``diagnosis_sample``，改吃 ``gain_ledger``／
#: ``item_ability``）——所以這裡不能再對全部名字套同一組期望值，改成逐名字
#: 查表。
_EXPECTED_COMPUTE_PARAMS = {
    "config_shift": ("diagnosis_sample", "parameters"),
    "item_ability": ("diagnosis_sample", "parameters"),
    "model_capacity": ("gain_ledger", "item_ability", "parameters"),
}


@pytest.mark.parametrize("name", contract.DIAGNOSES)
def test_registered_diagnoses_use_the_agreed_signatures(name):
    """真模組的簽章對照**寫死的**期望值，而不是對照 ``contract._SIGNATURES``。

    ``check_module`` 拿契約自己的常數去比，所以「常數與模組一起被改掉」的漂移
    它抓不到。這裡把約定的形狀獨立釘一份，改契約時必須有意識地也改這裡。
    """
    mod = importlib.import_module(f"recsys_tfb.diagnosis.metric.{name}")
    assert list(inspect.signature(mod.compute).parameters) == list(
        _EXPECTED_COMPUTE_PARAMS[name]
    )
    assert list(inspect.signature(mod.render).parameters) == ["result", "parameters"]


def test_contract_pulls_in_no_heavy_dependencies():
    """``contract`` 只能依賴 stdlib——``core/consistency.py`` 為了驗每項診斷的
    enabled 旗標而 import 它，而 ``validate_config_consistency`` 在**每個**
    pipeline 的 CLI entry 都會跑。

    這條守的是一個實際發生過的回歸：contract 曾 re-export
    ``report.figures.MAX_FIGURE_POINTS``，那條 import 連帶把 plotly 拉進來，
    import 成本 ~0 → 374ms，dataset／training／inference 三條不畫圖的 pipeline
    每次啟動都白付。

    用子行程量測，因為同一個 pytest session 裡 plotly 早被別的測試載入了，
    在本行程檢查 ``sys.modules`` 永遠會是 False positive。
    """
    result = subprocess.run(
        [sys.executable, "-c",
         "import sys;"
         "import recsys_tfb.diagnosis.metric.contract;"
         "print('plotly' in sys.modules or 'pandas' in sys.modules)"],
        capture_output=True, text=True, check=True,
    )
    assert result.stdout.strip() == "False", (
        f"contract 拉進了重量級依賴：{result.stdout.strip()}"
    )


@pytest.mark.parametrize("name", contract.DIAGNOSES)
def test_every_registered_diagnosis_satisfies_contract(name):
    """registry 裡每個名字都要能 import 且通過 ``check_module``。

    這條是 ``report_builder`` 零改動的前提：它只走 registry，不認得任何
    單一診斷，所以 registry 說有的東西必須真的存在且形狀正確。
    """
    mod = importlib.import_module(f"recsys_tfb.diagnosis.metric.{name}")
    contract.check_module(mod)


def test_default_inputs_apply_when_module_is_silent():
    """沒宣告 ``INPUTS`` 的模組（目前的 config_shift）落回吃共用抽樣的預設。"""
    from recsys_tfb.diagnosis.metric import config_shift

    assert contract.inputs_for(config_shift) == ("diagnosis_sample", "parameters")


def test_declared_inputs_override_the_default():
    mod = types.SimpleNamespace(INPUTS=("gain_ledger", "parameters"))
    assert contract.inputs_for(mod) == ("gain_ledger", "parameters")


def test_compute_params_strip_the_evaluation_prefix():
    mod = types.SimpleNamespace(
        INPUTS=("gain_ledger", "evaluation_item_ability", "parameters"))
    assert contract.compute_params_for(mod) == (
        "gain_ledger", "item_ability", "parameters")


def test_signature_mismatch_against_declared_inputs_raises():
    """契約的實際價值：宣告 INPUTS 卻寫錯 compute 簽章 → TypeError。

    match 用 'gain_ledger' 而不是 'contract'——後者會被別條規則的訊息滿足。
    """
    mod = types.SimpleNamespace(
        NAME="x", TITLE="X", SCOPE=object(),
        INPUTS=("gain_ledger", "parameters"),
        compute=lambda diagnosis_sample, parameters: {},   # ← 沒跟著改
        render=lambda result, parameters: (),
    )
    with pytest.raises(TypeError, match="gain_ledger"):
        contract.check_module(mod)


def test_parameters_must_be_the_last_input():
    """§3 不變量 1：``make_diagnosis_node`` 用位置 ``-1`` 取 ``parameters``。

    宣告 ``INPUTS`` 卻沒把 ``parameters`` 放最後，node 會把別的東西當成
    ``parameters`` 餵給 ``compute``，而不會有任何錯誤訊息——這條測試守的是
    「登記進 registry 的每一項診斷都遵守這個位置慣例」。
    """
    assert contract.DEFAULT_INPUTS[-1] == "parameters"
    for name in contract.DIAGNOSES:
        mod = importlib.import_module(f"recsys_tfb.diagnosis.metric.{name}")
        declared = contract.inputs_for(mod)
        assert declared[-1] == "parameters", (
            f"{name}: INPUTS 的最後一個元素是 {declared[-1]!r}，"
            "不是 'parameters'——make_diagnosis_node 靠位置 -1 取值"
        )


def test_every_diagnosis_node_input_has_a_catalog_entry():
    """§3 不變量 2：``INPUTS`` 裡除 ``parameters``／``diagnosis_sample`` 外的
    每個名字，都必須在 ``catalog.yaml`` 有 entry。

    打錯字時 runner 會給 MemoryDataset 的 ``None``，而 node 照樣跑得完——
    沒有這條測試就沒有任何東西擋。
    """
    from pathlib import Path

    import yaml

    catalog_path = (Path(__file__).resolve().parents[3]
                    / "conf" / "base" / "catalog.yaml")
    catalog = yaml.safe_load(catalog_path.read_text(encoding="utf-8"))
    for name in contract.DIAGNOSES:
        mod = importlib.import_module(f"recsys_tfb.diagnosis.metric.{name}")
        declared = contract.inputs_for(mod)
        for key in declared:
            if key in ("parameters", "diagnosis_sample"):
                continue
            assert key in catalog, (
                f"{name}: INPUTS 含 {key!r}，但 catalog.yaml 沒有這個 entry"
                "——runner 會給 MemoryDataset 的 None，node 照樣跑得完"
            )


def test_diagnosis_node_rejects_a_short_inputs_list():
    """少給一個 input 必須 TypeError，不得靜默。

    這是 Plan 1.5 整個重構的核心宣稱，換成 INPUTS 之後要重新釘一次：工廠若
    改用 ``*args`` 直接轉呼叫（不核對個數），少給一個 input 只會讓某個位置
    參數被錯當成 ``parameters`` 餵進去，而不會有任何錯誤訊息。
    """
    from recsys_tfb.pipelines.evaluation.nodes_spark import make_diagnosis_node

    node_fn = make_diagnosis_node("config_shift")
    with pytest.raises(TypeError, match="expected 2 inputs"):
        node_fn(None)  # 少給 parameters


def test_every_registry_diagnosis_has_a_catalog_entry():
    """registry 有的診斷，``catalog.yaml`` 必須有對應的 JSONDataset。

    漏掉的話 catalog 會自動建一個 MemoryDataset：pipeline 跑得完、頁面也產得
    出來，但**磁碟上沒有那份 JSON**——離線重繪少一頁，而且沒有任何訊息。
    Plan 2-5 每加一項診斷都要補一條 entry，所以這個動作會重複四次。

    連 ``type`` 一起驗：只驗 key 存在的話，寫成 MemoryDataset 照樣通過，而那
    正是要擋的東西。
    """
    from pathlib import Path

    import yaml

    from recsys_tfb.diagnosis.metric.contract import DIAGNOSES

    catalog_path = (Path(__file__).resolve().parents[3]
                    / "conf" / "base" / "catalog.yaml")
    catalog = yaml.safe_load(catalog_path.read_text(encoding="utf-8"))
    for name in DIAGNOSES:
        key = f"evaluation_{name}"
        assert key in catalog, (
            f"{key} 不在 catalog.yaml——診斷結果不會落地，離線重繪看不到它"
        )
        assert catalog[key]["type"] == "JSONDataset", (
            f"{key} 的 type 是 {catalog[key]['type']}，"
            "非 JSONDataset 就不會有磁碟產物"
        )
        assert catalog[key]["filepath"].endswith(f"diagnosis/{name}.json"), (
            f"{key} 的 filepath 不是 diagnosis/{name}.json——"
            "render_diagnosis_pages 按檔名讀，路徑不對就讀不到"
        )
