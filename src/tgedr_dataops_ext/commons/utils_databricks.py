"""Utility functions for Databricks-related operations, including retrieval of dbutils."""
import logging
from pyspark.sql import SparkSession  # noqa: TC002
from tgedr_dataops_ext.commons.utils_spark import UtilsSpark

logger = logging.getLogger(__name__)

class UtilsDatabricks:
    """Utility class for Databricks-related operations, such as retrieving dbutils."""

    __DATABRICKS_UTILS = None

    @staticmethod
    def get_dbutils() -> object:
      """Retrieves the Databricks dbutils object using the current Spark session.

      Returns:
        object: The Databricks dbutils instance, or None if unavailable.

      """
      logger.debug("[UtilsDatabricks.get_dbutils|in]")

      spark: SparkSession = UtilsSpark.get_spark_session()
      dbutils = None
      if not UtilsDatabricks.__DATABRICKS_UTILS:
        try:
          from pyspark.dbutils import DBUtils # pyright: ignore[reportMissingImports]
          dbutils = DBUtils(spark)
          logger.debug("[UtilsDatabricks.get_dbutils] got it from spark")
          UtilsDatabricks.__DATABRICKS_UTILS = dbutils
        except Exception as x:  # noqa: BLE001
          logger.debug("[UtilsDatabricks.get_dbutils] could not get it", exc_info=x)
      else:
          dbutils = UtilsDatabricks.__DATABRICKS_UTILS

      logger.debug(f"[UtilsDatabricks.get_dbutils|out] => {dbutils}")
      return dbutils
