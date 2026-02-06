from typing import List

import pandas as pd
import pytest
from pyspark.sql import Row

from tgedr_dataops.commons.utils_fs import temp_dir
from tgedr_dataops_ext.source.local_delta_table import LocalDeltaTable
from tgedr_dataops_abs.source import NoSourceException


def test_list(spark):
    key = temp_dir()

    df = spark.createDataFrame(
        [
            Row(id=3, region="america"),
            Row(id=2, region="europe"),
        ]
    )

    df.write.format("delta").mode("overwrite").save(key + "/A")
    df.write.format("delta").mode("overwrite").save(key + "/B")

    o = LocalDeltaTable()
    actual: List[str] = o.list(context={"url": key})
    actual.sort()
    assert ["A", "B"] == actual


def test_get(spark):
    key = temp_dir()

    df = spark.createDataFrame(
        [
            Row(id=3, region="america"),
            Row(id=2, region="europe"),
        ]
    )

    df.write.format("delta").mode("overwrite").save(key)

    o = LocalDeltaTable()

    actual: pd.DataFrame = o.get(context={"url": key})
    assert 2 == actual.shape[0]
    assert 2 == actual.shape[1]

    actual: pd.DataFrame = o.get(context={"url": key, "columns": ["region"]})
    assert 2 == actual.shape[0]
    assert 1 == actual.shape[1]

    assert ["america", "europe"] == list((actual.to_dict()["region"].values()))


def test_get_nosourceexception(resources_folder):
    with pytest.raises(NoSourceException):
        LocalDeltaTable().get(context={"url": f"{resources_folder}/dummy/nosource"})


def test_list_missing_url():
    """Test list method raises exception when URL context is missing"""
    from tgedr_dataops_abs.source import SourceException
    
    o = LocalDeltaTable()
    with pytest.raises(SourceException, match="you must provide context for url"):
        o.list(context={})


def test_list_invalid_directory():
    """Test list method raises exception for non-directory URL"""
    from tgedr_dataops_abs.source import SourceException
    
    o = LocalDeltaTable()
    with pytest.raises(SourceException, match="not a delta lake url"):
        o.list(context={"url": "/nonexistent/path/to/delta"})
