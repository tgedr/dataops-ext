from pyspark.sql import SparkSession
from pyspark.sql import types as T

from tgedr_dataops_ext.commons.utils_spark import UtilsSpark


def test_local_spark():
    o = UtilsSpark.get_local_spark_session()
    assert type(o) == SparkSession


def test_local_spark_with_config():
    o = UtilsSpark.get_local_spark_session({"spark.driver.memory": "6g"})
    assert "6g" == o.conf.get("spark.driver.memory")


def test_local_spark_defined_by_env_with_config(environment_mock):
    o = UtilsSpark.get_spark_session({"spark.driver.memory": "6g"})
    assert "6g" == o.conf.get("spark.driver.memory")


def test_not_aws_spark(environment_mock_another):
    o = UtilsSpark.get_spark_session({"spark.driver.memory": "6g"})
    assert "6g" == o.conf.get("spark.driver.memory")


def test_buidl_schema_from_dtypes():
    expected = T.StructType(
        [
            T.StructField("primaryid", T.LongType(), True),
            T.StructField("rpsr_cod", T.StringType(), True),
            T.StructField("processing_time", T.LongType(), True),
            T.StructField("caseid", T.LongType(), True),
            T.StructField("period", T.StringType(), True),
            T.StructField("t_integer", T.IntegerType(), True),
            T.StructField("t_bool", T.BooleanType(), True),
            T.StructField("t_float", T.DoubleType(), True),
            T.StructField("t_datetime", T.TimestampType(), True),
            T.StructField("t_date", T.DateType(), True),
        ]
    )

    dtypes = {
        "primaryid": "bigint",
        "rpsr_cod": "string",
        "processing_time": "bigint",
        "caseid": "bigint",
        "period": "string",
        "t_integer": "int",
        "t_bool": "boolean",
        "t_float": "double",
        "t_datetime": "timestamp",
        "t_date": "date",
    }

    assert expected == UtilsSpark.build_schema_from_dtypes(dtypes)
