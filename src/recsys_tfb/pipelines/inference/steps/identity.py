"""What identifies one scored row in offline inference's output.

One function, because three nodes need the same answer and the answer is a
**decision** rather than something re-derivable from the schema: offline
inference ignores the optional column roles (ADR-0025 decision 1, spec #426
decision E). It has to sit somewhere with a name, or each of the three sites
re-derives it and one of them eventually gets it wrong in silence.
"""

from __future__ import annotations

from recsys_tfb.core.schema import OPTIONAL_ROLE_KEYS


def scored_row_columns(schema: dict) -> list[str]:
    """The columns that identify one row of this pipeline's output.

    ``identity_columns`` **minus whatever the optional roles added** — so
    ``time + entity + item``, whatever else the deployment declares.

    **Why inference drops them.** Its candidates are not observed events; the
    framework manufactures them, every configured item for every entity in the
    population. There is no impression to name, so there is no ``event`` value
    that could go in this list — and asking a frame for a column that cannot
    exist is how this was first written (it raised ``Column 'impression_id'
    does not exist`` inside ``build_inference_population_features``, found by
    the ad example's end-to-end run and by no unit test).

    So a deployment that declares ``event`` still gets offline inference: its
    output simply has no ``event`` column, and the rows are unique on this
    narrower key by construction. What that costs is that the published table
    cannot be joined to a label table at event grain — which is exactly why
    evaluation's monitoring mode is refused while an optional role is declared
    (A40), rather than being left to produce a wrong number.

    **Why a subtraction and not ``base_key_columns + [item]``.** The two are
    equal today and will stay equal, so this is not about the value — it is
    about which sentence the code says. ``base_key + item`` says "an entity at
    a time, and an item", which is a coincidence that happens to hold;
    subtracting says "identity, except the roles this pipeline ignores", which
    is the actual rule from ADR-0025 and stays the actual rule when
    ``occasion`` lands and has to be dropped here too — automatically, with no
    edit to this function. It is also what keeps S7 honest rather than
    exempted: S7 forbids *rebuilding* a derived list and deliberately permits
    *trimming* one, because a trim still has a single source
    (docs/agents/architecture-constraints.md, S7 "這個檢查看不到").
    """
    # Every optional role, not just ``event``: ADR-0025 decision 1 says
    # offline inference ignores *both*, so ``occasion`` starts being dropped
    # here the moment it is added to OPTIONAL_ROLE_KEYS, with no edit. That is
    # the intent, not an accident — but it IS the kind of silent widening the
    # occasion ticket has to re-read this function to confirm.
    dropped = {c for role in OPTIONAL_ROLE_KEYS for c in schema.get(role, [])}
    return [c for c in schema["identity_columns"] if c not in dropped]
