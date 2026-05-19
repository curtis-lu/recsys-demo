"""Restricted arithmetic/boolean expression evaluator (stdlib ``ast`` only).

Used by the declarative HPO search space: ``when`` guards and
expression-valued bounds (wired in Phase 3). Built and fully tested in
Phase 2 as the security foundation. No ``eval``/``exec``, no imports, no
attribute access, no comprehensions/lambda, calls only to a tiny numeric
allowlist. Production constraint: no third-party expression library.
"""

from __future__ import annotations

import ast
import operator
from typing import Any

__all__ = ["SafeEvalError", "safe_eval"]


class SafeEvalError(ValueError):
    """Raised when evaluation fails for any reason: syntax error, disallowed
    construct/name/call, oversized exponent, too-deeply-nested expression, or
    any other runtime evaluation error."""


_BIN = {
    ast.Add: operator.add, ast.Sub: operator.sub, ast.Mult: operator.mul,
    ast.Div: operator.truediv, ast.FloorDiv: operator.floordiv,
    ast.Mod: operator.mod, ast.Pow: operator.pow,
}
_UNARY = {ast.UAdd: operator.pos, ast.USub: operator.neg, ast.Not: operator.not_}
_CMP = {
    ast.Eq: operator.eq, ast.NotEq: operator.ne, ast.Lt: operator.lt,
    ast.LtE: operator.le, ast.Gt: operator.gt, ast.GtE: operator.ge,
    ast.In: lambda a, b: a in b, ast.NotIn: lambda a, b: a not in b,
}
_CALLS = {
    "min": min, "max": max, "abs": abs, "round": round,
    "int": int, "float": float, "len": len,
}

_MAX_POW_EXPONENT = 64  # HPO bound expressions never need a larger exponent;
                        # bigger is a config typo -> fail loud, not a compute bomb
_MAX_DEPTH = 64         # cap recursion depth; beyond this raise SafeEvalError, not RecursionError


def safe_eval(expr: str, context: dict) -> Any:
    """Evaluate ``expr`` against name->value ``context``.

    Raises ``SafeEvalError`` on:
    - a syntax error in the expression,
    - any construct, name, or call outside the allowlist,
    - an exponent exceeding ``_MAX_POW_EXPONENT``,
    - an expression nested deeper than ``_MAX_DEPTH``, or
    - any other runtime evaluation error (e.g. ZeroDivisionError, TypeError).
    """
    try:
        tree = ast.parse(expr, mode="eval")
    except SyntaxError as exc:
        raise SafeEvalError(f"syntax error in expression {expr!r}: {exc}") from exc
    try:
        return _eval(tree.body, context)
    except SafeEvalError:
        raise
    except Exception as exc:
        raise SafeEvalError(
            f"error evaluating expression {expr!r}: {exc}"
        ) from exc


def _eval(node: ast.AST, ctx: dict, depth: int = 0) -> Any:
    if depth > _MAX_DEPTH:
        raise SafeEvalError("expression nesting too deep")
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.Name):
        if node.id not in ctx:
            raise SafeEvalError(f"unknown name {node.id!r}")
        return ctx[node.id]
    if isinstance(node, (ast.List, ast.Tuple)):
        # Collection literals intentionally normalize to list (sufficient for membership tests;
        # tuple identity is not needed by HPO expressions).
        return [_eval(e, ctx, depth + 1) for e in node.elts]
    if isinstance(node, ast.UnaryOp):
        op = _UNARY.get(type(node.op))
        if op is None:
            raise SafeEvalError(f"disallowed unary op {type(node.op).__name__}")
        return op(_eval(node.operand, ctx, depth + 1))
    if isinstance(node, ast.BinOp):
        op = _BIN.get(type(node.op))
        if op is None:
            raise SafeEvalError(f"disallowed operator {type(node.op).__name__}")
        left = _eval(node.left, ctx, depth + 1)
        right = _eval(node.right, ctx, depth + 1)
        if isinstance(node.op, ast.Pow) and isinstance(right, (int, float)) and abs(right) > _MAX_POW_EXPONENT:
            raise SafeEvalError(
                f"exponent {right} exceeds the maximum allowed ({_MAX_POW_EXPONENT})"
            )
        return op(left, right)
    if isinstance(node, ast.BoolOp):
        vals = node.values
        if isinstance(node.op, ast.And):
            result = True
            for v in vals:
                result = _eval(v, ctx, depth + 1)
                if not result:
                    return result
            return result
        result = False
        for v in vals:
            result = _eval(v, ctx, depth + 1)
            if result:
                return result
        return result
    if isinstance(node, ast.Compare):
        left = _eval(node.left, ctx, depth + 1)
        for op_node, comp in zip(node.ops, node.comparators):
            op = _CMP.get(type(op_node))
            if op is None:
                raise SafeEvalError(
                    f"disallowed comparison {type(op_node).__name__}"
                )
            right = _eval(comp, ctx, depth + 1)
            if not op(left, right):
                return False
            left = right
        return True
    if isinstance(node, ast.IfExp):
        return (
            _eval(node.body, ctx, depth + 1)
            if _eval(node.test, ctx, depth + 1)
            else _eval(node.orelse, ctx, depth + 1)
        )
    if isinstance(node, ast.Call):
        if (
            not isinstance(node.func, ast.Name)
            or node.func.id not in _CALLS
            or node.keywords
        ):
            raise SafeEvalError("disallowed call (only min/max/abs/round/int/float/len, no kwargs)")
        return _CALLS[node.func.id](*[_eval(a, ctx, depth + 1) for a in node.args])
    raise SafeEvalError(f"disallowed expression: {type(node).__name__}")
