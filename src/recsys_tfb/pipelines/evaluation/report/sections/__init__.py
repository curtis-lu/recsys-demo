"""The main report's sections, one module per section (ADR-0031
principle 6 and decision 9). Each declares the results it reads
(``READS``) and builds its section from them (``build``);
``report/main.py`` orders them. ``shared`` holds what more
than one section uses; ``metric_tables`` is not a section either: the tables
both reports draw (a metric's table, 「能不能比」, the model-selection
metrics, the definitions block — #537)."""
