"""The prediction-quality family's pre-check on its input: every evaluated row
carries the zero-positive group weight that family weights rows by.
"""

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F

from recsys_tfb.core.consistency import DataConsistencyError


def require_zero_positive_group_weights(
    eval_predictions: SparkDataFrame, weight_col: str,
) -> None:
    """Pre-check (inputs): every evaluated row carries a zero-positive group
    weight."""
    missing = weight_col not in eval_predictions.columns
    if not missing and eval_predictions.filter(
        F.col(weight_col).isNull()
    ).limit(1).count() == 0:
        return
    what = ("have no column" if missing else "hold a NULL in column")
    raise DataConsistencyError(
        f"The evaluated model's dataset version was built with "
        f"dataset.test_zero_positive_group_ratio above 0 (its manifest), so the "
        f"prediction-quality family weights every row by {weight_col!r} — but "
        f"the evaluated predictions {what} {weight_col!r}: training wrote them "
        f"without it (it writes the weight only while its own conf's "
        f"dataset.test_zero_positive_group_ratio is above 0). Rewrite this "
        f"model's test predictions under a conf whose ratio is above 0."
    )
