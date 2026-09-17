"""Date settings written as a range instead of a list (#374).

A date-list setting can be written either as the list itself or as a range::

    train_snap_dates:
      start: "2025-01-31"
      end: "2025-10-31"
      step: month_end        # day | week | month_start | month_end

**Both ends are included, and both must land on the step.** ``week`` counts
7 days from ``start``; ``month_start``／``month_end`` want the first／last day
of a month. A ``start`` or ``end`` that misses the step is an error, not a
silent shift to the nearest date: a typo such as ``2025-01-30`` under
``month_end`` would otherwise move the whole split without anyone noticing.

The range is expanded by :class:`~recsys_tfb.core.config.ConfigLoader` right
after the config is loaded, into ``YYYY-MM-DD`` strings in ascending order —
the spelling the shipped conf uses for a hand-written list. Nothing downstream
ever sees the range, so a range and the list it stands for hash to the same
``base_dataset_version`` and the same evaluation config fingerprint. That is
the point of expanding early: version IDs are computed from the loaded dict
(``core/versioning.py::_hash8`` dumps it as-is), and a second spelling of the
same dates must not mint a second identity.

The range has no "up to today" form on purpose: an end that moves with the run
date would give the same config a different version ID tomorrow.
"""

from __future__ import annotations

import calendar
import datetime as _dt

#: Dotted key paths, inside each ``parameters*.yaml``, that accept a range.
#: Dotted rather than tuples so no element spells an example column name
#: (architecture-constraints S6).
DATE_LIST_KEYS: tuple[str, ...] = (
    "dataset.train_snap_dates",
    "dataset.calibration_snap_dates",
    "dataset.val_snap_dates",
    "dataset.test_snap_dates",
    "evaluation.snap_date",
)

STEPS: tuple[str, ...] = ("day", "week", "month_start", "month_end")

_RANGE_KEYS = frozenset({"start", "end", "step"})


class DateRangeError(ValueError):
    """A range-form date setting that cannot be expanded."""


def _parse_day(value, loc: str, field: str) -> _dt.date:
    # Unquoted YAML dates arrive as ``datetime.date``; quoted ones as text.
    if isinstance(value, _dt.datetime):
        return value.date()
    if isinstance(value, _dt.date):
        return value
    if isinstance(value, str):
        try:
            return _dt.date.fromisoformat(value.strip())
        except ValueError:
            pass
    raise DateRangeError(
        f"{loc}.{field} = {value!r} is not a YYYY-MM-DD date"
    )


def _month_last_day(day: _dt.date) -> _dt.date:
    return day.replace(day=calendar.monthrange(day.year, day.month)[1])


def _next_month(day: _dt.date) -> _dt.date:
    """The first day of the month after ``day``'s month."""
    if day.month == 12:
        return _dt.date(day.year + 1, 1, 1)
    return _dt.date(day.year, day.month + 1, 1)


def expand_date_range(spec: dict, loc: str) -> list[str]:
    """The ``YYYY-MM-DD`` list a ``{start, end, step}`` range stands for.

    ``loc`` names the setting in error messages, e.g.
    ``parameters_dataset.yaml -> dataset.train_snap_dates``.
    """
    keys = set(spec)
    if keys != _RANGE_KEYS:
        missing = sorted(_RANGE_KEYS - keys)
        extra = sorted(keys - _RANGE_KEYS)
        parts = []
        if missing:
            parts.append(f"missing {missing}")
        if extra:
            parts.append(f"unknown {extra}")
        raise DateRangeError(
            f"{loc}: a date range takes exactly start, end, step "
            f"({'; '.join(parts)})"
        )
    step = spec["step"]
    if step not in STEPS:
        raise DateRangeError(
            f"{loc}.step = {step!r}; expected one of {list(STEPS)}"
        )
    start = _parse_day(spec["start"], loc, "start")
    end = _parse_day(spec["end"], loc, "end")
    if end < start:
        raise DateRangeError(
            f"{loc}: end {end.isoformat()} is before start {start.isoformat()}"
        )

    if step == "day":
        days = [
            start + _dt.timedelta(days=i)
            for i in range((end - start).days + 1)
        ]
    elif step == "week":
        if (end - start).days % 7:
            raise DateRangeError(
                f"{loc}: end {end.isoformat()} is not a whole number of weeks "
                f"after start {start.isoformat()} (step: week counts 7 days "
                "from start, and both ends are included)"
            )
        days = [
            start + _dt.timedelta(days=7 * i)
            for i in range((end - start).days // 7 + 1)
        ]
    else:
        on_step = (
            (lambda d: d.day == 1)
            if step == "month_start"
            else (lambda d: d == _month_last_day(d))
        )
        for field, day in (("start", start), ("end", end)):
            if not on_step(day):
                raise DateRangeError(
                    f"{loc}.{field} = {day.isoformat()} is not a "
                    f"{'first' if step == 'month_start' else 'last'} day of a "
                    f"month (step: {step}; both ends are included, so both "
                    "must land on the step)"
                )
        days = []
        month = start.replace(day=1)
        while month <= end:
            days.append(month if step == "month_start" else _month_last_day(month))
            month = _next_month(month)

    return [d.isoformat() for d in days]


def as_date_list(value) -> list[str]:
    """A setting that holds one date or several, as a list of date texts.

    For readers of a key that may be a single date *or* a list — today only
    ``evaluation.snap_date``. A single value becomes a one-element list; a
    list keeps its order; each date is stripped and written as ``YYYY-MM-DD``
    when YAML parsed it into a date. Nothing configured gives ``[]``: whether
    that is an error is the caller's call, with the caller's message.

    Not reformatted beyond that: readers compare these texts with a STRING
    time partition that was written from rows selected with this same setting.
    """
    values = value if isinstance(value, list) else [value]
    dates = []
    for v in values:
        if isinstance(v, (_dt.date, _dt.datetime)):
            v = (v.date() if isinstance(v, _dt.datetime) else v).isoformat()
        text = "" if v is None else str(v).strip()
        if text:
            dates.append(text)
    return dates


def dates_label(dates: list[str]) -> str:
    """The path segment for a run over ``dates``: ``YYYYMMDD`` or ``first-last``.

    One date keeps the segment a single-date run has always used, so existing
    outputs stay where they are. Several dates name the earliest and the
    latest (ISO text sorts as dates do). Two different sets with the same ends
    share a segment: a rerun under the other set overwrites it, the way a
    rerun under any other changed setting does, and the config fingerprint
    stops a partial rerun from mixing the two.
    """
    if not dates:
        raise ValueError("dates_label: no dates to name a path segment after")
    ordered = sorted(dates)
    first, last = ordered[0].replace("-", ""), ordered[-1].replace("-", "")
    return first if len(ordered) == 1 else f"{first}-{last}"


def _is_parameters_stem(stem: str) -> bool:
    return stem == "parameters" or stem.startswith("parameters_")


def expand_date_ranges(config: dict[str, dict]) -> dict[str, dict]:
    """Replace every range-form value at :data:`DATE_LIST_KEYS` with its list.

    ``config`` is :class:`ConfigLoader`'s per-file dict (stem -> content).
    Only ``parameters*`` files are looked at; a value that is not a dict (a
    list, a single date, absent) is left exactly as written. Every bad range
    is reported in one :class:`DateRangeError` (collect-all, like the env
    placeholder check next to it).
    """
    errors: list[str] = []
    result = dict(config)
    for stem, data in config.items():
        if not _is_parameters_stem(stem) or not isinstance(data, dict):
            continue
        new_data = data
        for dotted in DATE_LIST_KEYS:
            section_name, key = dotted.split(".")
            section = new_data.get(section_name)
            if not isinstance(section, dict) or not isinstance(section.get(key), dict):
                continue
            try:
                expanded = expand_date_range(
                    section[key], f"{stem}.yaml -> {dotted}"
                )
            except DateRangeError as exc:
                errors.append(f"  {exc}")
                continue
            if new_data is data:
                new_data = dict(data)
            new_data[section_name] = {**section, key: expanded}
        result[stem] = new_data
    if errors:
        raise DateRangeError(
            f"{len(errors)} 個日期區間設定無法展開:\n" + "\n".join(errors)
        )
    return result
