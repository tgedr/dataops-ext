from typing import List, Optional

import boto3
import pandas as pd
import pytest
from moto import mock_aws

import tgedr_dataops_ext.source.delta_table_source as dts
from tgedr_dataops.commons.utils_fs import temp_file
from tgedr_dataops.sink.s3_file_sink import S3FileSink
from tgedr_dataops_ext.source.s3_delta_table import S3DeltaTable

BUCKET = "JustABucket"


def create_bucket(name: str):
    conn = boto3.resource("s3", region_name="us-east-1")
    conn.create_bucket(Bucket=name)


def create_dummy_file_in_bucket(bucket: str, key: str, dst_file: Optional[str] = None):
    dummy = temp_file()
    target = f"s3://{bucket}/{key}/{dst_file}"
    o = S3FileSink()
    o.put(context={"source": dummy, "target": target})
    return target


@mock_aws
def test_list():
    create_bucket(BUCKET)
    datasets: List[str] = ["A", "B"]
    for dataset in datasets:
        key = f"ss/datasets/{dataset}/_delta_log"
        create_dummy_file_in_bucket(bucket=BUCKET, key=key, dst_file="001.json")

    url = f"s3://{BUCKET}/ss/datasets"
    o = S3DeltaTable()
    actual: List[str] = o.list(context={"url": url})
    actual.sort()

    assert actual == ["ss/datasets/A", "ss/datasets/B"]


def test_get(monkeypatch):
    key = "dummy"

    class MockS3DatasetDeltaTable:
        def __init__(self, table_uri, storage_options, without_files):
            pass

        def to_pandas(self, columns=None):
            df = pd.DataFrame({"id": [3, 2], "region": ["america", "europe"]})
            if columns is not None:
                df = df[columns]
            return df

    monkeypatch.setattr(dts, "DeltaTable", MockS3DatasetDeltaTable)
    o = S3DeltaTable()

    actual: pd.DataFrame = o.get(context={"url": f"s3://{key}"})
    assert 2 == actual.shape[0]
    assert 2 == actual.shape[1]

    actual: pd.DataFrame = o.get(context={"url": f"s3://{key}", "columns": ["region"]})
    assert 2 == actual.shape[0]
    assert 1 == actual.shape[1]

    assert ["america", "europe"] == list((actual.to_dict()["region"].values()))


@mock_aws
def test_list_missing_url():
    """Test list method raises exception when URL context is missing"""
    from tgedr_dataops_abs.source import SourceException
    
    o = S3DeltaTable()
    with pytest.raises(SourceException, match="you must provide context for url"):
        o.list(context={})


def test_storage_options_with_config():
    """Test _storage_options property with AWS config"""
    config = {
        "AWS_ACCESS_KEY_ID": "test_key",
        "AWS_SECRET_ACCESS_KEY": "test_secret",
        "AWS_SESSION_TOKEN": "test_token",
        "AWS_REGION": "us-west-2",
    }
    o = S3DeltaTable(config=config)
    options = o._storage_options
    
    assert options is not None
    assert options["AWS_ACCESS_KEY_ID"] == "test_key"
    assert options["AWS_SECRET_ACCESS_KEY"] == "test_secret"
    assert options["AWS_SESSION_TOKEN"] == "test_token"
    assert options["AWS_REGION"] == "us-west-2"
