# tgedr-dataops-ext

> Concrete, tested implementations of the [tgedr-dataops](https://pypi.org/project/tgedr-dataops/) abstract contracts — PySpark, Delta Lake, and Databricks, all in one place.

![Coverage](./coverage.svg)
[![PyPI](https://img.shields.io/pypi/v/tgedr-dataops-ext)](https://pypi.org/project/tgedr-dataops-ext/)
![PySpark](https://img.shields.io/badge/PySpark-4.1.1-orange)
![Delta Spark](https://img.shields.io/badge/Delta%20Spark-4.1-blue)

---

## motivation

*tgedr-dataops-ext* builds on top of [*tgedr-dataops*](https://pypi.org/project/tgedr-dataops/) (the abstract contracts layer) and provides concrete, tested implementations for distributed data processing with PySpark and Delta Lake. It covers session management, ETL pipelines, Delta table storage, data validation, and Databricks job integration, all following consistent code quality and structural standards.

---

## installation

```bash
pip install tgedr-dataops-ext
```

---

## package contents

### commons

Shared utilities and base classes used across the library.

| Class | Description | Example |
|---|---|---|
| `Dataset` | Immutable wrapper pairing a Spark DataFrame with its `Metadata` | [test](tests/tgedr_dataops_ext/commons/test_dataset.py) |
| `Metadata` | Immutable dataclass describing a dataset (name, version, framing, sources) | [test](tests/tgedr_dataops_ext/commons/test_metadata.py) |
| `UtilsSpark` | Utility class for creating and configuring Spark sessions (local, AWS Glue, or active session) and building PySpark schemas from type dictionaries | [test](tests/tgedr_dataops_ext/commons/test_utils_spark.py) |
| `UtilsDatabricks` | Utility class for retrieving the Databricks `dbutils` object from the active Spark session | [test](tests/tgedr_dataops_ext/commons/test_utils_databricks.py) |
| `EtlDatabricks` | Abstract intermediate ETL class extending `Etl` with Databricks job integration: captures `run_id`, publishes outputs via `dbutils.jobs.taskValues`, and provides the `inject_configuration` decorator for auto-wiring method parameters from configuration or upstream task values | [test](tests/tgedr_dataops_ext/commons/test_etl_databricks.py) |

### quality

Data quality validation backed by Great Expectations.

| Class | Description | Example |
|---|---|---|
| `PysparkValidation` | `GreatExpectationsValidation` implementation for validating PySpark DataFrames using the Great Expectations library | [test](tests/tgedr_dataops_ext/quality/test_pyspark_validation.py) |

### source

Implementations of the `Source` contract for reading data from various backends.

| Class | Description | Example |
|---|---|---|
| `DeltaTableSource` | Abstract `Source` base class for reading Delta Lake datasets, returning a pandas DataFrame | [test](tests/tgedr_dataops_ext/source/test_delta_table_source.py) |
| `LocalDeltaTable` | Concrete `Source` reading Delta Lake datasets from the local filesystem using pure Python (no PySpark required) | [test](tests/tgedr_dataops_ext/source/test_local_delta_table.py) |
| `S3DeltaTable` | Concrete `Source` reading Delta Lake datasets from S3 using pure Python (no PySpark required) | [test](tests/tgedr_dataops_ext/source/test_s3_delta_table.py) |
| `CatalogFileSource` | `Source` implementation for listing, copying, and retrieving metadata of files in a Databricks-accessible file system (DBFS, S3, ADLS) via `dbutils.fs` | [test](tests/tgedr_dataops_ext/source/test_catalog_file_source.py) |

### sink

Implementations of the `Sink` contract for writing and managing data in various backends.

| Class | Description | Example |
|---|---|---|
| `CatalogFileSink` | `Sink` implementation for copying and deleting files or directories in a Databricks-accessible file system via `dbutils.fs` | [test](tests/tgedr_dataops_ext/sink/test_catalog_file_sink.py) |

### store

Implementations of the `Store` contract for persistent, structured data storage.

| Class | Description | Example |
|---|---|---|
| `SparkDeltaStore` | `Store` implementation for PySpark distributed processing with Delta Lake format. Supports versioned reads, append/overwrite writes, upserts, partitioning, schema evolution, retention policies, metadata management, and column comments | [test](tests/tgedr_dataops_ext/store/test_spark_delta.py) |

---

## development

Requirements:
- [`uv`](https://github.com/astral-sh/uv)
- `bash`

```bash
# clone
git clone git@github.com:tgedr/dataops-ext
cd dataops-ext

# install dependencies
./helper.sh reqs

# run tests
./helper.sh test
```