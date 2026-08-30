"""Declarative HPO search space: ParamSpec + Optuna define-by-run builder.

The YAML ``training.search_space`` is an ordered list of ParamSpec maps.
Order is meaningful: Optuna samples in list order so a later param's
Phase-3 ``when``/expression may reference earlier params. This module is
schema-only + sampling; ALL validation lives in
``core.consistency.search_space_errors`` (collect-all, CLI entry). It
imports nothing from ``nodes.py`` so it is unit-testable in isolation.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ParamSpec:
    """One hyperparameter. ``name`` is BOTH the Optuna suggest name and the
    algorithm param key (must map 1:1 to a native LightGBM/XGBoost param so
    ``finalize_model``'s ``**best_params`` merge stays correct).

    ``when`` is parsed and stored for Phase 3; Phase 2 rejects it fail-loud
    in the consistency layer (it is never silently ignored here).
    """

    name: str
    type: str
    low: object = None
    high: object = None
    step: object = None
    log: bool = False
    choices: list | None = None
    when: str | None = None


def parse_search_space(raw: list) -> list[ParamSpec]:
    """Turn the raw YAML list into ParamSpec objects, preserving order.

    Pure structural mapping; assumes ``raw`` already passed
    ``core.consistency.search_space_errors`` at CLI entry.
    """
    specs: list[ParamSpec] = []
    for item in raw:
        specs.append(
            ParamSpec(
                name=item["name"],
                type=item["type"],
                low=item.get("low"),
                high=item.get("high"),
                step=item.get("step"),
                log=bool(item.get("log", False)),
                choices=item.get("choices"),
                when=item.get("when"),
            )
        )
    return specs


def build_trial_params(trial, search_space: list) -> dict:
    """Sample one trial's params from the declarative space, in list order.

    Returns ``{spec.name: value}``; ``spec.name`` is also the Optuna suggest
    name so ``study.best_params`` keys flow unchanged into the final refit.
    """
    specs = parse_search_space(search_space)
    out: dict = {}
    for s in specs:
        if s.type == "int":
            kwargs = {"log": s.log}
            if s.step is not None:
                kwargs["step"] = s.step
            out[s.name] = trial.suggest_int(s.name, s.low, s.high, **kwargs)
        elif s.type == "float":
            kwargs = {"log": s.log}
            if s.step is not None:
                kwargs["step"] = s.step
            out[s.name] = trial.suggest_float(s.name, s.low, s.high, **kwargs)
        elif s.type == "categorical":
            out[s.name] = trial.suggest_categorical(s.name, s.choices)
        else:  # unreachable: search_space_errors rejects unknown type at CLI
            raise ValueError(f"unknown ParamSpec.type {s.type!r}")
    return out
