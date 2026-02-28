import pytest
import tgedr_dataops_ext.source.catalog_file_source as cfs
from tgedr_dataops_ext.source.catalog_file_source import CatalogFileSource
from tgedr_dataops_abs.source import SourceException


class FileInfo:
    path: str
    name: str
    size: int
    modificationTime: int

    def __init__(self, path, name, size, modificationTime):
        self.path = path
        self.name = name
        self.size = size
        self.modificationTime = modificationTime


def test_list(monkeypatch):

    class MockDbUtilsFs:

        def ls(self, target: str):
            return [
                FileInfo(
                    path="dbfs:/Volumes/global_safety_ds_bronze/faers/tmp/dummy/part-00001-ae084d9f-df8d-4e4c-bc17-7f0b497aa0cf.c000.snappy.parquet",
                    name="part-00001-ae084d9f-df8d-4e4c-bc17-7f0b497aa0cf.c000.snappy.parquet",
                    size=1200,
                    modificationTime=1738057469000,
                ),
                FileInfo(
                    path="dbfs:/Volumes/global_safety_ds_bronze/faers/tmp/dummy/part-00003-b26f9595-9fef-44c1-bbaf-a4956e9b24a0.c000.snappy.parquetx",
                    name="part-00003-b26f9595-9fef-44c1-bbaf-a4956e9b24a0.c000.snappy.parquetx",
                    size=1194,
                    modificationTime=1738057469000,
                ),
            ]

    class MockDbUtils:
        fs = MockDbUtilsFs()

    class MockUtilsDatabricks:
        @staticmethod
        def get_dbutils():
            return MockDbUtils()

    o = CatalogFileSource()
    monkeypatch.setattr(cfs, "UtilsDatabricks", MockUtilsDatabricks)

    actual = o.list({"source": "dbfs:/dummy", "suffix": ".parquetx"})
    expected = [
        "dbfs:/Volumes/global_safety_ds_bronze/faers/tmp/dummy/part-00003-b26f9595-9fef-44c1-bbaf-a4956e9b24a0.c000.snappy.parquetx"
    ]
    assert actual == expected


def test_get(monkeypatch):

    class MockDbUtilsFs:

        def cp(self, source: str, target: str):
            pass

    class MockDbUtils:
        fs = MockDbUtilsFs()

    class MockUtilsDatabricks:
        @staticmethod
        def get_dbutils():
            return MockDbUtils()

    monkeypatch.setattr(cfs, "UtilsDatabricks", MockUtilsDatabricks)
    o = CatalogFileSource()

    expected = [
        "dbfs:/Volumes/global_safety_ds_bronze/faers/tmp/part-00001-ae084d9f-df8d-4e4c-bc17-7f0b497aa0cf.c000.snappy.parquet",
        "dbfs:/Volumes/global_safety_ds_bronze/faers/tmp/part-00003-b26f9595-9fef-44c1-bbaf-a4956e9b24a0.c000.snappy.parquet",
    ]

    actual = o.get(
        {
            "files": [
                "dbfs:/dummy/part-00001-ae084d9f-df8d-4e4c-bc17-7f0b497aa0cf.c000.snappy.parquet",
                "dbfs:/dummy/part-00003-b26f9595-9fef-44c1-bbaf-a4956e9b24a0.c000.snappy.parquet",
            ],
            "target": "dbfs:/Volumes/global_safety_ds_bronze/faers/tmp/",
        }
    )
    assert actual == expected


def test_get_metadata_a(monkeypatch):

    class MockDbUtilsFs:

        def ls(self, target: str):
            return [
                FileInfo(
                    path="dbfs:/Volumes/global_safety_ds_bronze/faers/tmp/dummy/part-00001-ae084d9f-df8d-4e4c-bc17-7f0b497aa0cf.c000.snappy.parquet",
                    name="part-00001-ae084d9f-df8d-4e4c-bc17-7f0b497aa0cf.c000.snappy.parquet",
                    size=1200,
                    modificationTime=1738057469000,
                )
            ]

    class MockDbUtils:
        fs = MockDbUtilsFs()

    class MockUtilsDatabricks:
        @staticmethod
        def get_dbutils():
            return MockDbUtils()

    monkeypatch.setattr(cfs, "UtilsDatabricks", MockUtilsDatabricks)
    o = CatalogFileSource()

    expected = {"LastModified": 1738057469000, "ContentLength": 1200}

    actual = o.get_metadata({"source": "dbfs:/dummy"})
    assert actual == expected


def test_get_metadata_b(monkeypatch):

    class MockDbUtilsFs:

        def ls(self, target: str):
            return [
                FileInfo(
                    path="dbfs:/Volumes/global_safety_ds_bronze/faers/tmp/dummy/part-00001-ae084d9f-df8d-4e4c-bc17-7f0b497aa0cf.c000.snappy.parquet",
                    name="part-00001-ae084d9f-df8d-4e4c-bc17-7f0b497aa0cf.c000.snappy.parquet",
                    size=1200,
                    modificationTime=1738057469000,
                ),
                FileInfo(
                    path="dbfs:/Volumes/global_safety_ds_bronze/faers/tmp/dummy/part-00003-b26f9595-9fef-44c1-bbaf-a4956e9b24a0.c000.snappy.parquetx",
                    name="part-00003-b26f9595-9fef-44c1-bbaf-a4956e9b24a0.c000.snappy.parquetx",
                    size=1194,
                    modificationTime=1738057469000,
                ),
            ]

    class MockDbUtils:
        fs = MockDbUtilsFs()

    class MockUtilsDatabricks:
        @staticmethod
        def get_dbutils():
            return MockDbUtils()

    monkeypatch.setattr(cfs, "UtilsDatabricks", MockUtilsDatabricks)
    o = CatalogFileSource()

    expected = [
        {"LastModified": 1738057469000, "ContentLength": 1200},
        {"LastModified": 1738057469000, "ContentLength": 1194},
    ]

    actual = o.get_metadata({"source": "dbfs:/dummy"})
    assert actual == expected


# ---------------------------------------------------------------------------
# list — error paths
# ---------------------------------------------------------------------------

def test_list_raises_when_context_is_none():
    o = CatalogFileSource()
    with pytest.raises(SourceException):
        o.list(None)


def test_list_raises_when_source_key_missing():
    o = CatalogFileSource()
    with pytest.raises(SourceException):
        o.list({"suffix": ".parquet"})


def test_list_returns_empty_and_warns_when_fs_raises(monkeypatch):
    class MockDbUtilsFs:
        def ls(self, target: str):
            raise RuntimeError("fs unavailable")

    class MockDbUtils:
        fs = MockDbUtilsFs()

    class MockUtilsDatabricks:
        @staticmethod
        def get_dbutils():
            return MockDbUtils()

    monkeypatch.setattr(cfs, "UtilsDatabricks", MockUtilsDatabricks)
    o = CatalogFileSource()
    result = o.list({"source": "dbfs:/dummy"})
    assert result == []


# ---------------------------------------------------------------------------
# get — error paths and single-file branch
# ---------------------------------------------------------------------------

def test_get_raises_when_context_is_none():
    o = CatalogFileSource()
    with pytest.raises(SourceException):
        o.get(None)


def test_get_raises_when_no_file_key():
    o = CatalogFileSource()
    with pytest.raises(SourceException):
        o.get({"target": "dbfs:/out/"})


def test_get_raises_when_target_missing():
    o = CatalogFileSource()
    with pytest.raises(SourceException):
        o.get({"file": "dbfs:/dummy/file.parquet"})


def test_get_single_file_with_trailing_slash_target(monkeypatch):
    class MockDbUtilsFs:
        def cp(self, source: str, target: str):
            pass

    class MockDbUtils:
        fs = MockDbUtilsFs()

    class MockUtilsDatabricks:
        @staticmethod
        def get_dbutils():
            return MockDbUtils()

    monkeypatch.setattr(cfs, "UtilsDatabricks", MockUtilsDatabricks)
    o = CatalogFileSource()
    result = o.get({"file": "dbfs:/dummy/file.parquet", "target": "dbfs:/out/"})
    assert result == ["dbfs:/out/file.parquet"]


def test_get_single_file_without_trailing_slash_target(monkeypatch):
    class MockDbUtilsFs:
        def cp(self, source: str, target: str):
            pass

    class MockDbUtils:
        fs = MockDbUtilsFs()

    class MockUtilsDatabricks:
        @staticmethod
        def get_dbutils():
            return MockDbUtils()

    monkeypatch.setattr(cfs, "UtilsDatabricks", MockUtilsDatabricks)
    o = CatalogFileSource()
    result = o.get({"file": "dbfs:/dummy/file.parquet", "target": "dbfs:/out/renamed.parquet"})
    assert result == ["dbfs:/out/renamed.parquet"]


# ---------------------------------------------------------------------------
# get_metadata — error paths
# ---------------------------------------------------------------------------

def test_get_metadata_raises_when_context_is_none():
    o = CatalogFileSource()
    with pytest.raises(SourceException):
        o.get_metadata(None)


def test_get_metadata_raises_when_source_key_missing():
    o = CatalogFileSource()
    with pytest.raises(SourceException):
        o.get_metadata({"other": "value"})