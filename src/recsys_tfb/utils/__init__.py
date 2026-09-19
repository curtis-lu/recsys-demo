"""Import submodules directly (``recsys_tfb.utils.spark`` and so on).

This package re-exports nothing on purpose. It used to re-export
``get_or_create_spark_session``, which nobody imported from here, and that one
line made ``import recsys_tfb.utils.<anything>`` load pyspark — including
``utils.ranking``, which pyspark-free numpy modules import (#355).
"""
