"""Spark Delta Lake store implementation module.

This module provides a Store implementation for working with Delta Lake format
using Apache Spark. It includes the SparkDeltaStore class which supports:
- Reading and writing data in Delta Lake format
- Versioning and time travel queries
- Update/merge operations (upserts)
- Partitioning and schema evolution
- Retention policies for log and deleted files
- Metadata management
"""

from abc import ABC
import dataclasses
import logging
from typing import Any
from datetime import datetime
from pyspark.sql import DataFrame
from delta.tables import DeltaTable
from pyspark.sql import functions as f
from pyspark.sql import types as T  # noqa: N812
from pyspark.errors import AnalysisException
from pyspark.sql.functions import monotonically_increasing_id
from tgedr_dataops_abs.store import NoStoreException, Store, StoreException
from tgedr_dataops_ext.commons.metadata import Metadata
from tgedr_dataops_ext.commons.utils_spark import UtilsSpark

logger = logging.getLogger(__name__)


class SparkDeltaStore(Store, ABC):
    """A store implementation using Spark Delta Lake format.

    This class provides methods for reading, writing, updating, and deleting
    data in Delta Lake format using Apache Spark. It supports features like
    versioning, partitioning, schema evolution, and retention policies.

    Attributes
    ----------
    config : dict[str, Any] | None
        Optional configuration dictionary for the store.

    Methods
    -------
    get(key: str, version: Optional[str] = None, **kwargs) -> DataFrame
        Retrieve data from the specified key/path.
    save(df: DataFrame, key: str, append: bool = False, ...) -> None
        Save a DataFrame to the specified key/path.
    update(df: Any, key: str, match_fields: list[str], ...) -> None
        Update or insert data using merge operation.
    delete(key: str, condition: Union[f.Column, str, None] = None, **kwargs) -> None
        Delete data from the specified key/path.
    """

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        """Initialize SparkDeltaStore with optional configuration.

        Args:
            config: Optional configuration dictionary for the store.
        """
        Store.__init__(self, config)

    def get(self, key: str, version: str | None = None, **kwargs) -> DataFrame:  # noqa: ANN003, ARG002
        """Retrieve data from the specified key/path.

        Parameters
        ----------
        key : str
            The path to the Delta table to read from.
        version : str | None, optional
            The version of the Delta table to read, is None by default.
        **kwargs
            Additional keyword arguments (currently unused).

        Returns
        -------
        DataFrame
            The Spark DataFrame containing the data from the Delta table.

        Raises
        ------
        NoStoreException
            If no data is found at the specified key/path.
        """
        logger.info(f"[get|in] ({key}, {version})")

        table = self._get_table(path=key)
        if table is None:
            raise NoStoreException(f"[get] couldn't find data in key: {key}")

        reader = UtilsSpark.get_spark_session().read.format("delta")
        if version is not None:
            reader = reader.option("versionAsOf", version)

        result = reader.load(key)

        logger.info("[get_df|out]")
        return result

    def __get_deletion_criteria(self, df: DataFrame) -> Any:
        logger.debug("[__get_deletion_criteria|in])")
        fields = df.dtypes
        numerics = [
            x
            for x in fields
            if x[1] in ["bigint", "int", "double", "float", "long", "decimal.Decimal"] or (x[1][:7]) == "decimal"
        ]
        dates = [x for x in fields if (x[1]) in ["datetime", "datetime.datetime"]]
        textuals = [x for x in fields if x[1] in ["string"]]
        if 0 < len(numerics):
            column = numerics[0][0]
            result = (f.col(column) > 0) | (f.col(column) <= 0)
        elif 0 < len(dates):  # pragma: no cover
            column = dates[0][0]
            now = datetime.now(tz=datetime.UTC) # pyright: ignore[reportAttributeAccessIssue]
            result = (f.col(column) > now) | (f.col(column) <= now)
        elif 0 < len(textuals):
            column = textuals[0][0]
            result = (f.col(column) > "a") | (f.col(column) <= "a")
        else:
            raise StoreException(
                "[__get_deletion_criteria] failed to figure out column types handy to create a full deletion criteria"
            )

        logger.debug(f"[__get_deletion_criteria|out] = {result}")
        return result

    def delete(self, key: str, condition: f.Column | str | None = None, **kwargs) -> None:  # pyright: ignore[reportPrivateImportUsage] # noqa: ANN003, ARG002
        """Delete data from the specified key/path.

        Parameters
        ----------
        key : str
            The path to the Delta table to delete from.
        condition : f.Column | str | None, optional
            The condition to filter rows for deletion. If None, deletes all rows.
        **kwargs
            Additional keyword arguments (currently unused).

        Raises
        ------
        StoreException
            If deletion fails or column types cannot be determined for full deletion.
        """
        logger.info(f"[delete|in] ({key}, {condition})")

        spark = UtilsSpark.get_spark_session()
        """
        is_s3_operation = True if key.startswith("s3") else False
        if is_s3_operation:
        """
        delta_table = DeltaTable.forPath(spark, key)
        if condition is None:
            condition = self.__get_deletion_criteria(delta_table.toDF())
        delta_table.delete(condition=condition)
        """
        else:   # local development mostly for temporary or test purposes
            spark_fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
            # get spark context path
            spark_path = spark._jvm.org.apache.hadoop.fs.Path(key)
            logger.info(f"[delete] spark path is {spark_path}")
            try:
                if spark_fs.exists(spark_path):
                    spark_fs.delete(spark_path, True)
            except AnalysisException as x:
                raise StoreException(f"[delete] couldn't do it on key {key}: {x}")
        """
        logger.info("[delete|out]")

    def save(
        self,
        df: DataFrame,
        key: str,
        append: bool = False,
        partition_fields: list[str] | None = None,
        metadata: Metadata | None = None,
        retention_days: int = 7,
        deleted_retention_days: int = 7,
        column_descriptions: dict[str, str] | None = None,
        table_name: str | None = None,
        **kwargs,  # noqa: ANN003
    ) -> None:
        """Save a DataFrame to the specified key/path in Delta format.

        Parameters
        ----------
        df : DataFrame
            The Spark DataFrame to save.
        key : str
            The path where the Delta table will be saved.
        append : bool, optional
            Whether to append to existing data, is False by default.
        partition_fields : list[str] | None, optional
            List of column names to partition the table by, is None by default.
        metadata : Metadata | None, optional
            Optional metadata to attach to the table, is None by default.
        retention_days : int, optional
            Number of days to retain log files, is 7 by default.
        deleted_retention_days : int, optional
            Number of days to retain deleted files, is 7 by default.
        column_descriptions : dict[str, str] | None, optional
            Dictionary mapping column names to their descriptions, is None by default.
        table_name : str | None, optional
            Optional table name in format 'db.table', is None by default.
        **kwargs
            Additional keyword arguments.
        """
        logger.info(
            f"[save|in] ({df}, {key}, {append}, {partition_fields}, {metadata}, {retention_days}, {deleted_retention_days}, {column_descriptions}, {table_name}, {kwargs})"
        )

        writer = df.write.format("delta").mode("append") if append else df.write.format("delta").mode("overwrite")

        if partition_fields is not None:
            table = self._get_table(path=key)
            if table is not None:
                self._set_table_partitions(path=key, partition_fields=partition_fields)
            writer = writer.partitionBy(*partition_fields)

        if self._has_schema_changed(path=key, df=df):
            writer = writer.option("overwriteSchema", "true")

        if metadata:
            writer = writer.option("userMetadata", metadata) # pyright: ignore[reportArgumentType]

        if table_name is not None:
            # assume we have db.table
            db = table_name.split(".")[0]
            UtilsSpark.get_spark_session().sql(f"CREATE DATABASE IF NOT EXISTS {db}")
            writer = writer.option("path", key).saveAsTable(table_name)

            if column_descriptions is not None:
                self.set_column_comments(db=db, table=table_name.split(".")[1], col_comments=column_descriptions) # pragma: no cover
        else:
            writer.save(key)

        logger.info("[save] optimizing...")
        table = self._get_table(path=key)

        if retention_days is not None and deleted_retention_days is not None:
            self.enforce_retention_policy(
                path=key, retention_days=retention_days, deleted_retention_days=deleted_retention_days
            )
        elif retention_days is not None:   # pragma: no cover
            self.enforce_retention_policy(path=key, retention_days=retention_days)  # pragma: no cover

        table.optimize().executeCompaction() # pyright: ignore[reportOptionalMemberAccess]

        logger.info("[save|out]")

    def update(
        self,
        df: Any,
        key: str,
        match_fields: list[str],
        partition_fields: list[str] | None = None,
        metadata: Metadata | None = None,
        retention_days: int = 7,
        deleted_retention_days: int = 7,
        **kwargs,  # noqa: ANN003
    ) -> None:
        """Update or insert data using merge operation.

        Parameters
        ----------
        df : Any
            The DataFrame containing updates to merge.
        key : str
            The path to the Delta table to update.
        match_fields : list[str]
            List of column names to use for matching rows during merge.
        partition_fields : list[str] | None, optional
            List of column names to partition the table by, is None by default.
        metadata : Metadata | None, optional
            Optional metadata to attach to the table, is None by default.
        retention_days : int, optional
            Number of days to retain log files, is 7 by default.
        deleted_retention_days : int, optional
            Number of days to retain deleted files, is 7 by default.
        **kwargs
            Additional keyword arguments.
        """
        logger.info(
            f"[update|in] ({df}, {key}, {match_fields}, {partition_fields}, {metadata}, {retention_days}, {deleted_retention_days}, {kwargs})"
        )

        table = self._get_table(path=key)
        if table is None:
            self.save(
                df=df,
                key=key,
                partition_fields=partition_fields,
                metadata=metadata,
                retention_days=retention_days,
                deleted_retention_days=deleted_retention_days,
                **kwargs,
            )
        else:
            if partition_fields is not None:
                self._set_table_partitions(path=key, partition_fields=partition_fields)
                table = self._get_table(path=key)

            match_clause = None
            for field in match_fields:
                match_clause = (
                    f"current.{field} = updates.{field}"
                    if match_clause is None
                    else f"{match_clause} and current.{field} = updates.{field}"
                )
            logger.info(f"[update] match clause: {match_clause}")

            # check if the df has all the required columns
            # as we are upserting the updated columns coming in must at least match or exceed the current columns
            for column in table.toDF().columns: # pyright: ignore[reportOptionalMemberAccess]
                # we'll assume missing columns are nullable, typically metrics
                if column not in df.columns:
                    df = df.withColumn(column, f.lit(None).cast(T.StringType()))

            table.alias("current").merge( # pyright: ignore[reportOptionalMemberAccess]
                df.alias("updates"), match_clause # pyright: ignore[reportArgumentType]
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

            if retention_days is not None and deleted_retention_days is not None:
                self.enforce_retention_policy(
                    path=key, retention_days=retention_days, deleted_retention_days=deleted_retention_days
                )
            elif retention_days is not None:  # pragma: no cover
                self.enforce_retention_policy(path=key, retention_days=retention_days)

            table.optimize().executeCompaction() # pyright: ignore[reportOptionalMemberAccess]

        logger.info("[UtilsDeltaTable.upsert|out]")

    def enforce_retention_policy(self, path: str, retention_days: int = 7, deleted_retention_days: int = 7) -> None:
        """Enforce retention policy on Delta table log and deleted files.

        Parameters
        ----------
        path : str
            The path to the Delta table.
        retention_days : int, optional
            Number of days to retain log files, is 7 by default.
        deleted_retention_days : int, optional
            Number of days to retain deleted files, is 7 by default.
        """
        logger.info(f"[enforce_retention_policy|in] ({path}, {retention_days}, {deleted_retention_days})")

        retention = f"interval {retention_days} days"
        deleted_retention = f"interval {deleted_retention_days} days"

        UtilsSpark.get_spark_session().sql(
            f"ALTER TABLE delta.`{path}` SET TBLPROPERTIES('delta.logRetentionDuration' = '{retention}', 'delta.deletedFileRetentionDuration' = '{deleted_retention}')"
        )
        logger.info("[enforce_retention_policy|out]")

    def get_latest_table_versions(self, path: str, how_many: int = 1) -> list[str]:
        """Checks the delta table history and retrieves the latest n versions.

        Sorted from the newest to the oldest.
        """
        logger.info(f"[get_latest_table_versions|in] ({path}, {how_many})")
        result: list[str] = []

        table = self._get_table(path=path)
        if table is not None:
            history_rows = table.history().orderBy(f.desc("timestamp")).limit(how_many)
            result = [str(x.version) for x in history_rows.collect()]

        logger.info(f"[get_latest_table_versions|out] => {result}")
        return result

    def get_metadata(self, path: str, version: str | None = None) -> Metadata | None:
        """Retrieve metadata from the Delta table at the specified path.

        Raises
        ------
        NoStoreException
        """
        logger.info(f"[get_metadata|in] ({path}, {version})")
        table = self._get_table(path)
        if table is None:
            raise NoStoreException(f"[get_metadata] no data in path: {path}")

        result = None

        df_history = table.history().filter(f.col("userMetadata").isNotNull())
        if version is not None:
            df_history = df_history.filter(f.col("version") <= int(version))

        df_history = df_history.orderBy(f.col("version").desc())
        if not df_history.isEmpty():
            user_metadata = df_history.take(1)[0].userMetadata
            result = Metadata.from_str(user_metadata)
            if version is not None:
                result = dataclasses.replace(result, version=version)

        logger.info(f"[get_metadata|out] => ({result})")
        return result

    def _get_table_partitions(self, path: str) -> list[str]:
        logger.info(f"[_get_table_partitions|in] ({path})")

        spark = UtilsSpark.get_spark_session()
        detail = DeltaTable.forPath(spark, path).detail()
        partition_cols = detail.select("partitionColumns").collect()[0].partitionColumns
        result: list[str] = list(partition_cols) if partition_cols else []

        logger.info(f"[_get_table_partitions|out] => {result}")
        return result

    def _vacuum_now(self, path: str) -> None:
        logger.info("[_vacuum_now|in]")

        spark = UtilsSpark.get_spark_session()
        old_conf_value = spark.conf.get("spark.databricks.delta.retentionDurationCheck.enabled")
        spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        DeltaTable.forPath(spark, path).vacuum(0)
        spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", old_conf_value) # pyright: ignore[reportArgumentType]

        logger.info("[_vacuum_now|out]")

    def _has_schema_changed(self, path: str, df: DataFrame) -> bool:
        logger.info(f"[_has_schema_changed|in] ({path},{df})")
        result: bool = False
        table = self._get_table(path=path)
        if table is not None:
            result = table.toDF().schema != df.schema
        logger.info(f"[_has_schema_changed|out] => {result}")
        return result

    def _set_table_partitions(self, path: str, partition_fields: list[str]) -> None:
        logger.info(f"[_set_table_partitions|in] ({path},{partition_fields})")

        spark = UtilsSpark.get_spark_session()
        # let's check partition_cols
        current_partition_fields = self._get_table_partitions(path=path)
        shall_we_repartition = sorted(partition_fields) != sorted(current_partition_fields)

        if shall_we_repartition:
            logger.info("[_set_table_partitions] going to repartition")
            new_df = spark.read.format("delta").load(path)
            new_df.write.format("delta").mode("overwrite").partitionBy(*partition_fields).option(
                "overwriteSchema", "true"
            ).save(path)
            self._vacuum_now(path)
            logger.info(
                f"[_set_table_partitions] changed partition cols from {current_partition_fields} to {partition_fields}"
            )
        logger.info("[_set_table_partitions|out]")

    def _get_table(self, path: str) -> DeltaTable | None:
        logger.debug(f"[_get_table|in] ({path})")
        result= None # pyright: ignore[reportAssignmentType, reportRedeclaration]
        try:
            result: DeltaTable = DeltaTable.forPath(UtilsSpark.get_spark_session(), path)
        except AnalysisException as ax:
            logger.warning(f"[_get_table] couldn't load from {path}: {ax}")

        logger.debug(f"[_get_table|out] => {result}")
        return result

    def set_column_comments(self, db: str, table: str, col_comments: dict[str, str]) -> None:
        """Set comments for columns in a Delta table.

        Parameters
        ----------
        db : str
            The database name where the table is located.
        table : str
            The table name to set column comments for.
        col_comments : dict[str, str]
            Dictionary mapping column names to their comments.
        """
        logger.info(f"[set_column_comments|in] ({db}, {table}, {col_comments})")
        spark = UtilsSpark.get_spark_session()

        table_description: DataFrame = spark.sql(f"describe {db}.{table}").withColumn(
            "set_column_comments_id", monotonically_increasing_id()
        )
        partition_info_id = (
            table_description.filter(f.col("col_name") == "# Partition Information").collect()[0].set_column_comments_id
        )

        table_description = table_description.filter(
            (f.col("set_column_comments_id") < f.lit(partition_info_id)) & (f.col("col_name") != "")
        ).drop("set_column_comments_id")
        rows = [r.asDict() for r in table_description.collect()]
        for row in rows:
            col = row["col_name"]
            data_type = row["data_type"]
            if col in col_comments:
                new_comment = col_comments[col]
                logger.info(f"[set_column_comments] setting new comment ({new_comment}) to column {col}")
                spark.sql(f"ALTER TABLE {db}.{table} CHANGE COLUMN {col} {col} {data_type} COMMENT '{new_comment}'")

        logger.info("[set_column_comments|out]")
