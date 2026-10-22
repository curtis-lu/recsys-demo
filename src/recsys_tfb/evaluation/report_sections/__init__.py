"""The main report's sections, one module per section (ADR-0031
principle 6 and decision 9). Each declares the results it reads
(``READS``) and builds its section from them (``build``);
``evaluation/report_builder.py`` orders them. ``shared`` holds what more
than one section uses."""
