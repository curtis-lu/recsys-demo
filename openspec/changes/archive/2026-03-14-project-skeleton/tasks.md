## 1. Project Initialization

- [x] 1.1 Create `pyproject.toml` with package metadata, dependencies (pyspark, pyyaml, pandas, pyarrow, lightgbm, scikit-learn, mlflow, optuna, pytest), and pytest config
- [x] 1.2 Create directory structure: `src/recsys_tfb/{core,io,pipelines,utils}/`, `conf/{base,local,production}/`, `tests/{test_core,test_io,test_pipelines}/`, `data/` with all `__init__.py` files

## 2. ConfigLoader

- [x] 2.1 Implement `src/recsys_tfb/core/config.py`: ConfigLoader with YAML loading, deep merge, `get_catalog_config()`, `get_parameters()`
- [x] 2.2 Write tests `tests/test_core/test_config.py`: base-only loading, env overlay merge, nested dict merge, list replacement, missing env dir, parameters merge

## 3. I/O Abstraction Layer

- [x] 3.1 Implement `src/recsys_tfb/io/base.py`: AbstractDataset ABC with load/save/exists
- [x] 3.2 Implement `src/recsys_tfb/io/parquet_dataset.py`: ParquetDataset with pandas and spark backends
- [x] 3.3 Implement `src/recsys_tfb/io/pickle_dataset.py`: PickleDataset for arbitrary Python objects
- [x] 3.4 Write tests `tests/test_io/test_parquet_dataset.py`: pandas load/save/exists, spark load/save/exists
- [x] 3.5 Write tests `tests/test_io/test_pickle_dataset.py`: save/load round-trip, exists check

## 4. DataCatalog

- [x] 4.1 Implement `src/recsys_tfb/core/catalog.py`: DataCatalog with type registry, load/save/exists, add() for in-memory datasets
- [x] 4.2 Write tests `tests/test_core/test_catalog.py`: instantiation from config, load/save, unknown type error, unregistered name error, add() method

## 5. Pipeline Engine

- [x] 5.1 Implement `src/recsys_tfb/core/node.py`: Node class with func, inputs, outputs, string representation
- [x] 5.2 Implement `src/recsys_tfb/core/pipeline.py`: Pipeline with topological sort (Kahn's algorithm), circular dependency detection, `only_nodes_with_outputs` filtering
- [x] 5.3 Implement `src/recsys_tfb/core/runner.py`: Runner with sequential execution, catalog integration, structured logging, per-node and total timing
- [x] 5.4 Write tests `tests/test_core/test_node.py`: creation, no-inputs node, string representation
- [x] 5.5 Write tests `tests/test_core/test_pipeline.py`: linear chain ordering, independent nodes, circular dependency error, output filtering
- [x] 5.6 Write tests `tests/test_core/test_runner.py`: successful run, missing input error, node failure handling, timing logs

## 6. Utilities

- [x] 6.1 Implement `src/recsys_tfb/utils/spark.py`: SparkSession builder utility
- [x] 6.2 Create `tests/conftest.py`: shared SparkSession fixture for tests
