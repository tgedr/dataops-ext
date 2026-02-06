from pyspark.sql import Row

from tgedr_dataops_ext.commons.dataset import Dataset
from tgedr_dataops_ext.commons.metadata import FieldFrame, Metadata


def test_dataset(spark):
    df = spark.createDataFrame([Row(id=3, country="us")])
    md = Metadata(name="tableX", version="version", framing=[FieldFrame(field="id", lower=3, upper=3)], sources=None)

    o = Dataset(metadata=md, data=df)
    assert "metadata" in o.as_dict() and "data" in o.as_dict()


def test_dataset_str(spark):
    """Test Dataset __str__ method for JSON serialization"""
    df = spark.createDataFrame([Row(id=5, name="test")])
    md = Metadata(name="testTable", version="v1", framing=[FieldFrame(field="id", lower=5, upper=5)], sources=None)
    
    o = Dataset(metadata=md, data=df)
    str_result = str(o)
    assert "testTable" in str_result
    assert "metadata" in str_result
