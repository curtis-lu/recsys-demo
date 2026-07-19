"""診斷契約的測試：registry 形狀、必要符號檢查、常數唯一來源。

最後一條（``test_every_registered_diagnosis_satisfies_contract``）是**整套契約
真正的價值所在**：它是報表層「不必認識任何單一診斷」這個宣稱的執行點。前面
幾條驗的是 ``check_module`` 這個函式本身正確，只有這條驗的是 registry 裡的
名字**真的**都滿足契約。
"""
from __future__ import annotations

import importlib
import inspect
import types

import pytest

from recsys_tfb.diagnosis.metric import contract


def test_registry_is_exactly_the_planned_diagnoses():
    assert contract.DIAGNOSES == ("config_shift",)


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


@pytest.mark.parametrize("name", contract.DIAGNOSES)
def test_registered_diagnoses_use_the_agreed_signatures(name):
    """真模組的簽章對照**寫死的**期望值，而不是對照 ``contract._SIGNATURES``。

    ``check_module`` 拿契約自己的常數去比，所以「常數與模組一起被改掉」的漂移
    它抓不到。這裡把約定的形狀獨立釘一份，改契約時必須有意識地也改這裡。
    """
    mod = importlib.import_module(f"recsys_tfb.diagnosis.metric.{name}")
    assert list(inspect.signature(mod.compute).parameters) == [
        "diagnosis_sample", "parameters",
    ]
    assert list(inspect.signature(mod.render).parameters) == ["result", "parameters"]


def test_max_figure_points_is_re_exported_not_redefined():
    """唯一定義在 ``report.figures``；這裡另外賦值的話兩個常數會漂移，而
    ``assert_within_budget`` 只認 figures.py 那個，報表會靜默超量。"""
    from recsys_tfb.report import figures

    assert contract.MAX_FIGURE_POINTS is figures.MAX_FIGURE_POINTS


@pytest.mark.parametrize("name", contract.DIAGNOSES)
def test_every_registered_diagnosis_satisfies_contract(name):
    """registry 裡每個名字都要能 import 且通過 ``check_module``。

    這條是 ``report_builder`` 零改動的前提：它只走 registry，不認得任何
    單一診斷，所以 registry 說有的東西必須真的存在且形狀正確。
    """
    mod = importlib.import_module(f"recsys_tfb.diagnosis.metric.{name}")
    contract.check_module(mod)
