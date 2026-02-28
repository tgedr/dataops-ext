"""Spark utility functions and classes.

This module provides:
- UtilsSpark: A utility class with handy functions to work with Spark sessions,
  including creating local and remote sessions with Delta Lake support,
  and building PySpark schemas from data type dictionaries.
"""

import logging
import os
from typing import ClassVar
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql import types as T  # noqa: N812
from pyspark.context import SparkContext


logger = logging.getLogger(__name__)


class UtilsSpark:
    """class with handy functions to work with spark."""

    __ENV_KEY_PYSPARK_IS_LOCAL = "PYSPARK_IS_LOCAL"
    __ENV_KEY_PYSPARK_IS_AWS = "PYSPARK_IS_AWS"
    __DTYPES_MAP: ClassVar[dict[str, type]] = {
        "bigint": T.LongType,
        "string": T.StringType,
        "double": T.DoubleType,
        "int": T.IntegerType,
        "boolean": T.BooleanType,
        "timestamp": T.TimestampType,
        "date": T.DateType,
    }

    @staticmethod
    def get_local_spark_session(config: dict | None = None) -> SparkSession:
        """Get a local Spark session configured for Delta Lake.

        Parameters
        ----------
        config : dict | None, optional
            Additional Spark configuration parameters, by default None.

        Returns
        -------
        SparkSession
            A configured local Spark session instance with Delta Lake support.
        """
        logger.debug(f"[get_local_spark_session|in] ({config})")
        # PySpark 3.4 uses Scala 2.12, so we need delta-core_2.12
        builder = (
            SparkSession.builder.config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") # pyright: ignore[reportAttributeAccessIssue]
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.driver.host", "localhost")
        )

        if config is not None:
            for k, v in config.items():
                builder.config(k, v)

        spark = builder.getOrCreate()

        logger.debug(f"[get_local_spark_session|out] => {spark}")
        return spark

    @staticmethod
    def get_spark_session(config: dict | None = None) -> SparkSession:
        """Get a Spark session based on the environment configuration.

        Parameters
        ----------
        config : dict | None, optional
            Additional Spark configuration parameters, by default None.

        Returns
        -------
        SparkSession
            A configured Spark session instance.
        """
        logger.debug(f"[get_spark_session|in] ({config})")

        if "1" == os.getenv(UtilsSpark.__ENV_KEY_PYSPARK_IS_LOCAL):
            spark: SparkSession = UtilsSpark.get_local_spark_session(config)
        else:
            if "1" == os.getenv(UtilsSpark.__ENV_KEY_PYSPARK_IS_AWS):
                from awsglue.context import GlueContext  # type: ignore #   pragma: no cover  # noqa: PGH003
                glueContext = GlueContext(SparkContext.getOrCreate())  #   pragma: no cover  # noqa: N806
                active_session = glueContext.spark_session  #   pragma: no cover
            else:
                active_session = SparkSession.getActiveSession()

            spark_config = SparkConf()

            if active_session is not None:
                former_config = active_session.sparkContext.getConf().getAll()
                for entry in former_config:
                    spark_config.set(entry[0], entry[1])
            if config is not None:
                for k, v in config.items():
                    spark_config.set(k, v)
                spark: SparkSession = SparkSession.builder.config(conf=spark_config).getOrCreate() # pyright: ignore[reportAttributeAccessIssue]
            else:
                spark: SparkSession = SparkSession.builder.getOrCreate() # pyright: ignore[reportAttributeAccessIssue]

        logger.debug(f"[get_spark_session|out] => {spark}")
        return spark

    @staticmethod
    def build_schema_from_dtypes(dtypes_schema: dict[str, str]) -> T.StructType:
        """Build a PySpark StructType schema from a dictionary of data types.

        Parameters
        ----------
        dtypes_schema : dict[str, str]
            A dictionary mapping field names to their corresponding data type strings.
            Supported types: 'bigint', 'string', 'double', 'int', 'boolean', 'timestamp', 'date'.

        Returns
        -------
        T.StructType
            A PySpark StructType schema with fields defined by the input dictionary.
        """
        logger.info(f"[build_schema_from_dtypes|in] ({dtypes_schema})")
        result = T.StructType()
        for field, dtype in dtypes_schema.items():
            new_type = UtilsSpark.__DTYPES_MAP[dtype]
            result.add(field, new_type(), True)  # noqa: FBT003

        logger.info(f"[build_schema_from_dtypes|out] => {result}")
        return result
