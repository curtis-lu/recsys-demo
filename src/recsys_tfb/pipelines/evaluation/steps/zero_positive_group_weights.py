"""The prediction-quality family's pre-check on its input: every evaluated row
carries the zero-positive group weight that family weights rows by.
``compute_prediction_quality`` calls it when the conf weights at all.
"""

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F

from recsys_tfb.core.consistency import DataConsistencyError


def require_zero_positive_group_weights(
    eval_predictions: SparkDataFrame, weight_col: str,
) -> None:
    """Pre-check: every evaluated row carries a zero-positive group weight.

    Raises ``DataConsistencyError`` naming both fixes. One action over one
    column; ``limit(1)`` stops at the first NULL, so a mismatch answers fast.
    """
    missing = weight_col not in eval_predictions.columns
    if not missing and eval_predictions.filter(
        F.col(weight_col).isNull()
    ).limit(1).count() == 0:
        return
    what = ("have no column" if missing else "hold a NULL in column")
    raise DataConsistencyError(
        f"dataset.test_zero_positive_group_ratio is above 0 in this conf, so the "
        f"prediction-quality family weights every row by {weight_col!r} — but "
        f"the evaluated predictions {what} {weight_col!r}: they were not written "
        f"under a positive test ratio. The model being evaluated "
        f"(--model-version, or `best`) was trained on a dataset whose test ratio "
        f"differs from this conf's. Evaluate a model built under this conf, or "
        f"set dataset.test_zero_positive_group_ratio back to the value that "
        f"model was built with."
    )
