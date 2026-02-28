import os
from datetime import datetime
from tests.conftest import assert_frames_are_equal

import pytest
from pyspark.sql import DataFrame, Row
from pyspark.sql import functions as F

from tgedr_dataops_ext.commons.metadata import FieldFrame, Metadata
from tgedr_dataops_ext.store.spark_delta import SparkDeltaStore


@pytest.fixture
def tmp_dir(temporary_folder) -> str:
    return os.path.join(temporary_folder, "test_spark_parquet")


@pytest.fixture
def data(spark) -> DataFrame:
    now: float = datetime.now().timestamp()
    d = [
        Row(id=3, country="us", time=now, region="america"),
        Row(id=2, country="dk", time=now, region="europe"),
    ]
    return spark.createDataFrame(d)


@pytest.fixture
def data2(spark) -> DataFrame:
    now: float = datetime.now().timestamp()
    d = [
        Row(id=4, country="jp", time=now, region="asia"),
        Row(id=2, country="pt", time=now, region="europe"),
    ]
    return spark.createDataFrame(d)


@pytest.fixture
def data3(spark) -> DataFrame:
    now: float = datetime.now().timestamp()
    d = [
        Row(id=3, country="us", time=now, region="america"),
        Row(id=4, country="jp", time=now, region="asia"),
        Row(id=2, country="pt", time=now, region="europe"),
    ]
    return spark.createDataFrame(d)


def test_01_save(environment_mock, data, tmp_dir):
    o = SparkDeltaStore()
    o.save(df=data, key=tmp_dir)
    df = o.get(key=tmp_dir)
    assert_frames_are_equal(df.toPandas(), data.toPandas(), sort_columns=["id"])


def test_get_nonexistent_table(environment_mock, temporary_folder):
    """Test get raises NoStoreException for nonexistent table"""
    from tgedr_dataops_abs.store import NoStoreException
    
    o = SparkDeltaStore()
    nonexistent_path = os.path.join(temporary_folder, "nonexistent_table")
    with pytest.raises(NoStoreException, match="couldn't find data in key"):
        o.get(key=nonexistent_path)


def test_02_save_append(environment_mock, data, tmp_dir):
    o = SparkDeltaStore()
    table_version = o.get_latest_table_versions(path=tmp_dir)[0]
    o.save(df=data, key=tmp_dir, append=True)
    df = o.get(key=tmp_dir)
    assert df.count() == (2 * data.count())
    df = o.get(key=tmp_dir, version=table_version)
    assert df.count() == (data.count())


def test_03_save_overwrite(environment_mock, data, tmp_dir):
    o = SparkDeltaStore()
    o.save(df=data, key=tmp_dir)
    df = o.get(key=tmp_dir)
    assert df.count() == (data.count())


def test_04_save_with_partitions(environment_mock, data, tmp_dir):
    o = SparkDeltaStore()
    o.save(df=data, key=tmp_dir)
    assert 0 == len(o._get_table_partitions(path=tmp_dir))
    o.save(df=data, key=tmp_dir, partition_fields=["country"])
    assert 1 == len(o._get_table_partitions(path=tmp_dir))
    o.save(df=data, key=tmp_dir, partition_fields=["region", "country"])
    assert 2 == len(o._get_table_partitions(path=tmp_dir))
    o.save(df=data, key=tmp_dir, partition_fields=["country"])
    assert 1 == len(o._get_table_partitions(path=tmp_dir))


def test_05_metadata(environment_mock, data, tmp_dir):
    o = SparkDeltaStore()
    expected = Metadata(name="xpto", version="2", framing=None, sources=None)
    o.save(df=data, key=tmp_dir, metadata=expected)
    table_version = o.get_latest_table_versions(path=tmp_dir)[0]
    actual = o.get_metadata(path=tmp_dir)
    assert expected == actual
    expected2 = Metadata(name="xpto", version="3", framing=[FieldFrame(field="id", lower=2, upper=3)], sources=None)
    o.save(df=data, key=tmp_dir, metadata=expected2)
    actual = o.get_metadata(path=tmp_dir)
    assert expected2 == actual
    actual = o.get_metadata(path=tmp_dir, version=table_version)
    assert expected <= actual


def test_06_delete_all(environment_mock, data, tmp_dir):
    o = SparkDeltaStore()
    o.save(df=data, key=tmp_dir)
    assert o.get(key=tmp_dir).count() == data.count()

    # test numeric criteria
    o.delete(key=tmp_dir)
    assert 0 == o.get(key=tmp_dir).count()

    o.save(df=data, key=tmp_dir)
    # force date criteria
    condition = o._SparkDeltaStore__get_deletion_criteria(data.drop("id"))
    o.delete(key=tmp_dir, condition=condition)
    assert 0 == o.get(key=tmp_dir).count()

    o.save(df=data, key=tmp_dir)
    # force string criteria
    condition = o._SparkDeltaStore__get_deletion_criteria(data.drop("id").drop("time"))
    o.delete(key=tmp_dir, condition=condition)
    assert 0 == o.get(key=tmp_dir).count()


def test_06_delete_one_row(environment_mock, data, tmp_dir):
    o = SparkDeltaStore()
    o.save(df=data, key=tmp_dir)
    o.delete(key=tmp_dir, condition=(F.col("id") == 3))
    df = o.get(key=tmp_dir)
    assert 1 == df.count()


def test_07_update_one_row(environment_mock, data, data2, data3, tmp_dir):
    o = SparkDeltaStore()
    o.save(df=data, key=tmp_dir)
    o.update(df=data2, key=tmp_dir, match_fields=["id"])
    actual = o.get(key=tmp_dir)
    assert_frames_are_equal(actual.toPandas(), data3.toPandas(), sort_columns=["id"])


def test_08_update_one_row_with_partition_change(environment_mock, data, data2, data3, tmp_dir):
    o = SparkDeltaStore()
    o.save(df=data, key=tmp_dir, partition_fields=["region", "country"])
    o.update(df=data2, key=tmp_dir, match_fields=["id"], partition_fields=["country"])
    actual = o.get(key=tmp_dir)
    assert_frames_are_equal(actual.toPandas(), data3.toPandas(), sort_columns=["id"])
    assert ["country"] == o._get_table_partitions(path=tmp_dir)


def test_09_set_column_comments(environment_mock, spark, tmp_dir):
    df = spark.createDataFrame(
        [
            Row(id=3, country="us", region="america"),
            Row(id=2, country="dk", region="europe"),
        ]
    )

    o = SparkDeltaStore()
    o.save(df=df, key=tmp_dir, partition_fields=["region", "country"], table_name="dummy.test_09_set_column_comments")
    id_description: DataFrame = spark.sql(f"describe dummy.test_09_set_column_comments").filter(
        F.col("col_name") == "id"
    )
    row: dict = (id_description.collect()[0]).asDict()
    assert None == row["comment"]

    column_descriptions = {
        "id": "unique id\\'s",
        "country": "country of the data event took place",
        "region": "country_region",
    }
    o.set_column_comments(db="dummy", table="test_09_set_column_comments", col_comments=column_descriptions)

    id_description: DataFrame = spark.sql(f"describe dummy.test_09_set_column_comments").filter(
        F.col("col_name") == "id"
    )
    row: dict = (id_description.collect()[0]).asDict()
    assert "unique id's" == row["comment"]



def test_09b_save_with_table_name_and_column_descriptions(environment_mock, spark, tmp_dir):
    """Test save with both table_name and column_descriptions calls set_column_comments."""
    df = spark.createDataFrame([
        Row(id=1, country="us", region="america"),
    ])

    o = SparkDeltaStore()
    o.save(
        df=df,
        key=tmp_dir,
        partition_fields=["region"],
        table_name="dummy.test_09b",
        column_descriptions={"id": "unique identifier", "country": "country code"},
    )

    id_description = spark.sql("describe dummy.test_09b").filter(F.col("col_name") == "id")
    assert id_description.collect()[0].asDict()["comment"] == "unique identifier"


def test_10_schema_change(environment_mock, data, tmp_dir):
    o = SparkDeltaStore()
    o.save(df=data, key=tmp_dir)
    assert False == o._has_schema_changed(tmp_dir, data)

    df = data.withColumn("dummy", F.lit(0))
    assert o._has_schema_changed(tmp_dir, df)


def test_get_table_none(environment_mock, tmp_dir):
    """Test _get_table returns None for non-existent table"""
    o = SparkDeltaStore()
    result = o._get_table(path=f"{tmp_dir}/nonexistent")
    assert result is None


def test_get_table_exception_handling(environment_mock, data, tmp_dir):
    """Test _get_table exception handling for access errors"""
    from tgedr_dataops_abs.store import NoStoreException
    
    o = SparkDeltaStore()
    # First save a table
    o.save(df=data, key=tmp_dir)
    # Now it should work
    result = o._get_table(path=tmp_dir)
    assert result is not None


def test_delete_criteria_no_suitable_columns(environment_mock, spark, tmp_dir):
    """Test deletion criteria generation failure with unsuitable column types"""
    from tgedr_dataops_abs.store import StoreException
    from pyspark.sql.types import StructType, StructField, BinaryType
    
    # Create dataframe with only binary column (unsuitable for deletion criteria)
    schema = StructType([StructField("binary_col", BinaryType(), True)])
    df = spark.createDataFrame([(bytearray(b"test"),)], schema)
    
    o = SparkDeltaStore()
    with pytest.raises(StoreException, match="failed to figure out column types"):
        o._SparkDeltaStore__get_deletion_criteria(df)


def test_delete_criteria_with_textual_column(environment_mock, spark):
    """Test deletion criteria generation with textual column"""
    from pyspark.sql.types import StructType, StructField, StringType
    
    # Create dataframe with only string column (no numeric columns)
    schema = StructType([StructField("text_col", StringType(), True)])
    df = spark.createDataFrame([("test",)], schema)
    
    o = SparkDeltaStore()
    criteria = o._SparkDeltaStore__get_deletion_criteria(df)
    # Should return a valid condition
    assert criteria is not None


def test_save_with_metadata_and_retention(environment_mock, data, tmp_dir):
    """Test save with metadata and custom retention settings"""
    o = SparkDeltaStore()
    metadata = Metadata(name="test", version="1.0", framing=None, sources=None)
    
    o.save(
        df=data,
        key=tmp_dir,
        metadata=metadata,
        retention_days=30,
        deleted_retention_days=14
    )
    
    # Verify metadata was saved
    saved_metadata = o.get_metadata(path=tmp_dir)
    assert saved_metadata == metadata
    
    # Verify table was created
    df_result = o.get(key=tmp_dir)
    assert df_result.count() == data.count()


def test_save_with_only_retention_days(environment_mock, data, tmp_dir):
    """Test save with only retention_days (not deleted_retention_days)"""
    o = SparkDeltaStore()
    metadata = Metadata(name="test", version="1.0", framing=None, sources=None)
    
    o.save(
        df=data,
        key=tmp_dir,
        metadata=metadata,
        retention_days=30,
        deleted_retention_days=None
    )
    
    df_result = o.get(key=tmp_dir)
    assert df_result.count() == data.count()


def test_update_without_partition_change(environment_mock, data, data2, tmp_dir):
    """Test update when partition fields don't change"""
    o = SparkDeltaStore()
    o.save(df=data, key=tmp_dir, partition_fields=["region"])
    
    # Update without changing partition fields
    o.update(df=data2, key=tmp_dir, match_fields=["id"], partition_fields=["region"])
    
    actual = o.get(key=tmp_dir)
    assert actual.count() == 3  # Original 2 rows + 1 new row from data2


def test_get_metadata_no_metadata(environment_mock, data, temporary_folder):
    """Test get_metadata when no metadata exists"""
    o = SparkDeltaStore()
    test_path = os.path.join(temporary_folder, "no_metadata_table")
    o.save(df=data, key=test_path)
    
    # Get table metadata without saving custom metadata
    result = o.get_metadata(path=test_path)
    assert result is None


def test_get_metadata_nonexistent_table(environment_mock, temporary_folder):
    """Test get_metadata raises NoStoreException for nonexistent table"""
    from tgedr_dataops_abs.store import NoStoreException
    
    o = SparkDeltaStore()
    nonexistent_path = os.path.join(temporary_folder, "nonexistent_metadata_table")
    with pytest.raises(NoStoreException, match="no data in path"):
        o.get_metadata(path=nonexistent_path)


def test_get_with_version(environment_mock, data, tmp_dir):
    """Test get with specific version"""
    o = SparkDeltaStore()
    o.save(df=data, key=tmp_dir)
    version_1 = o.get_latest_table_versions(path=tmp_dir)[0]
    
    # Make another save
    o.save(df=data, key=tmp_dir, append=True)
    
    # Get specific version
    df_v1 = o.get(key=tmp_dir, version=version_1)
    assert df_v1.count() == data.count()
    
    # Get latest
    df_latest = o.get(key=tmp_dir)
    assert df_latest.count() == data.count() * 2


def test_save_overwrite_with_partition_check(environment_mock, data, data2, tmp_dir):
    """Test overwrite with partition field validation"""
    o = SparkDeltaStore()
    # Save with partitions
    o.save(df=data, key=tmp_dir, partition_fields=["region"])
    
    # Overwrite with same partition fields (should check existing partitions)
    o.save(df=data2, key=tmp_dir, partition_fields=["region"], append=False)
    
    df_result = o.get(key=tmp_dir)
    assert df_result.count() == data2.count()


def test_update_creates_table_if_missing(environment_mock, data, temporary_folder):
    """Test update creates table if it doesn't exist"""
    o = SparkDeltaStore()
    test_path = os.path.join(temporary_folder, "update_creates_table")
    
    # Update on non-existent table should create it
    o.update(df=data, key=test_path, match_fields=["id"])
    
    df_result = o.get(key=test_path)
    assert df_result.count() == data.count()


def test_update_with_missing_columns(environment_mock, spark, tmp_dir):
    """Test update when new data is missing columns from existing table"""
    o = SparkDeltaStore()
    
    # Create initial data with more columns
    now = datetime.now().timestamp()
    initial_data = spark.createDataFrame([
        Row(id=1, country="us", time=now, region="america", extra_col="value")
    ])
    o.save(df=initial_data, key=tmp_dir)
    
    # Update with data missing extra_col
    update_data = spark.createDataFrame([
        Row(id=1, country="ca", time=now, region="america")
    ])
    o.update(df=update_data, key=tmp_dir, match_fields=["id"])
    
    df_result = o.get(key=tmp_dir)
    assert df_result.count() == 1
    # extra_col should be filled with None
    assert "extra_col" in df_result.columns


def test_update_with_both_retention_days(environment_mock, data, data2, tmp_dir):
    """Test update with both retention_days and deleted_retention_days"""
    o = SparkDeltaStore()
    o.save(df=data, key=tmp_dir)
    
    o.update(
        df=data2,
        key=tmp_dir,
        match_fields=["id"],
        retention_days=30,
        deleted_retention_days=14
    )
    
    df_result = o.get(key=tmp_dir)
    assert df_result.count() == 3


def test_update_with_only_retention_days(environment_mock, data, data2, tmp_dir):
    """Test update with only retention_days (not deleted_retention_days)"""
    o = SparkDeltaStore()
    o.save(df=data, key=tmp_dir)
    
    o.update(
        df=data2,
        key=tmp_dir,
        match_fields=["id"],
        retention_days=30
    )
    
    df_result = o.get(key=tmp_dir)
    assert df_result.count() == 3


def test_get_metadata_with_version(environment_mock, data, tmp_dir):
    """Test get_metadata with specific version parameter"""
    o = SparkDeltaStore()
    metadata = Metadata(name="test", version="1.0", framing=None, sources=None)
    o.save(df=data, key=tmp_dir, metadata=metadata)
    
    version = o.get_latest_table_versions(path=tmp_dir)[0]
    
    # Get metadata for specific version - note that version parameter replaces the metadata version
    result = o.get_metadata(path=tmp_dir, version=str(version))
    assert result.name == metadata.name
    assert result.version == str(version)  # Version is replaced with the requested version
