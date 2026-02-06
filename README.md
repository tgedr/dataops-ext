# tgedr-dataops-ext

![Coverage](./coverage.svg)
[![PyPI](https://img.shields.io/pypi/v/tgedr-dataops-ext)](https://pypi.org/project/tgedr-dataops-ext/)


data operations related code - extended

## motivation
*dataops-ext* is a library with tested and used code aligning on some standards regarding code structure and quality and to avoid reinventing the wheel. It builds on top of *dataops-abs* and *dataops* providing distributed processing features based on pyspark.

## installation
        `pip install tgedr-dataops-ext`

## package namespaces and its contents

#### commons
- __Dataset__: immutable class to wrap up a dataframe along with metadata ([example](tests/tgedr_dataops_ext/commons/test_dataset.py))
- __Metadata__: immutable class depicting dataset metadata ([example](tests/tgedr_dataops_ext/commons/test_metadata.py))
- __UtilsSpark__: utility class to work with spark, mostly helping on creating a session ([example](tests/tgedr_dataops_ext/commons/test_utils_spark.py))

#### quality
- __PysparkValidation__ : __GreatExpectationsValidation__ implementation to validate pyspark dataframes with Great Expectations library ([example](tests/tgedr_dataops_ext/quality/test_pyspark_validation.py))

#### source

- __DeltaTableSource__: abstract __Source__ class used to read delta lake format datasets returning a pandas dataframe" ([example](tests/tgedr_dataops_ext/source/test_delta_table_source.py))
- __LocalDeltaTable__: __Source__ class used to read delta lake format datasets from local fs with python only, pyspark not needed, returning a pandas dataframe ([example](tests/tgedr_dataops_ext/source/test_local_delta_table.py))
- __S3DeltaTable__: __Source__ class used to read delta lake format datasets from s3 bucket with python only, pyspark not needed, returning a pandas dataframe ([example](tests/tgedr_dataops_ext/source/test_s3_delta_table.py))


#### store
- __SparkDeltaStore__ : __Store__ implementation for pyspark distributed processing with delta table format ([example](tests/tgedr_dataops_ext/store/test_spark_delta.py))



## development
- main requirements:
  - _uv_  
  - _bash_
- Clone the repository like this:

  ``` bash
  git clone git@github.com:tgedr/dataops-ext
  ```
- cd into the folder: `cd dataops-ext`
- install requirements: `./helper.sh reqs`
