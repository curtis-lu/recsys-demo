"""Tests for recsys_tfb.core.safe_eval — the expression security boundary."""

import pytest

from recsys_tfb.core.safe_eval import SafeEvalError, safe_eval


class TestAllowed:
    def test_arithmetic(self):
        assert safe_eval("2 + 3 * 4", {}) == 14
        assert safe_eval("(10 - 4) / 2", {}) == 3.0
        assert safe_eval("7 // 2", {}) == 3
        assert safe_eval("2 ** 5", {}) == 32
        assert safe_eval("7 % 3", {}) == 1

    def test_names_from_context(self):
        assert safe_eval("num_leaves * 2", {"num_leaves": 8}) == 16

    def test_unary(self):
        assert safe_eval("-x", {"x": 5}) == -5
        assert safe_eval("not flag", {"flag": False}) is True

    def test_comparison_and_bool(self):
        assert safe_eval("a < b and b < 10", {"a": 1, "b": 4}) is True
        assert safe_eval("a == 1 or b == 99", {"a": 0, "b": 99}) is True
        assert safe_eval("3 <= n <= 8", {"n": 5}) is True

    def test_membership(self):
        assert safe_eval("obj in ['lambdarank', 'rank_xendcg']", {"obj": "lambdarank"}) is True
        assert safe_eval("x not in [1, 2]", {"x": 3}) is True

    def test_ifexp(self):
        assert safe_eval("10 if big else 1", {"big": True}) == 10

    def test_allowlisted_calls(self):
        assert safe_eval("min(a, b)", {"a": 3, "b": 7}) == 3
        assert safe_eval("max(1, n, 4)", {"n": 9}) == 9
        assert safe_eval("abs(-x)", {"x": 5}) == 5
        assert safe_eval("int(2.9)", {}) == 2
        assert safe_eval("round(3.14159, 2)", {}) == 3.14


class TestRejected:
    @pytest.mark.parametrize("expr", [
        "__import__('os')",
        "x.__class__",
        "x.foo",
        "().__class__.__bases__",
        "eval('1')",
        "open('f')",
        "lambda: 1",
        "[i for i in range(3)]",
        "{1: 2}",
        "{1, 2}",
        "x[0]",
        "a := 1",
    ])
    def test_disallowed_constructs_raise(self, expr):
        with pytest.raises(SafeEvalError):
            safe_eval(expr, {"x": [1], "a": 1})

    def test_unknown_name_raises(self):
        with pytest.raises(SafeEvalError, match="unknown name"):
            safe_eval("mystery + 1", {})

    def test_call_to_non_allowlisted_raises(self):
        with pytest.raises(SafeEvalError, match="call"):
            safe_eval("sorted([3,1])", {})

    def test_syntax_error_wrapped(self):
        with pytest.raises(SafeEvalError, match="syntax"):
            safe_eval("1 +", {})

    def test_error_is_valueerror(self):
        assert issubclass(SafeEvalError, ValueError)
