import pytest
import tgedr_dataops_ext.sink.catalog_file_sink as catsink
from tgedr_dataops_ext.sink.catalog_file_sink import CatalogFileSink
from tgedr_dataops_abs.sink import SinkException


class MockDbUtilsFs:
    @staticmethod
    def cp(source: str, target: str):
        pass

    @staticmethod
    def rm(target: str, recursive: bool):
        pass

    @staticmethod
    def ls(target: str):
        return [target]


class MockDbUtils:
    fs = MockDbUtilsFs()


class MockUtilsDatabricks:
    @staticmethod
    def get_dbutils():
        return MockDbUtils()


def test_put_file_in_target_file_and_delete(monkeypatch):

    src_file = "source.txt"
    target_file = "target.txt"

    s = CatalogFileSink()
    monkeypatch.setattr(catsink, "UtilsDatabricks", MockUtilsDatabricks)

    s.put(context={"source": src_file, "target": target_file})
    s.delete({"target": target_file})

    assert True


# ---------------------------------------------------------------------------
# put — error paths
# ---------------------------------------------------------------------------

def test_put_raises_when_context_is_none():
    s = CatalogFileSink()
    with pytest.raises(SinkException):
        s.put(None)


def test_put_raises_when_source_key_missing():
    s = CatalogFileSink()
    with pytest.raises(SinkException):
        s.put({"target": "dbfs:/out/file.txt"})


def test_put_raises_when_target_key_missing(monkeypatch):
    monkeypatch.setattr(catsink, "UtilsDatabricks", MockUtilsDatabricks)
    s = CatalogFileSink()
    with pytest.raises(SinkException):
        s.put({"source": "dbfs:/in/file.txt"})


# ---------------------------------------------------------------------------
# delete — error paths
# ---------------------------------------------------------------------------

def test_delete_raises_when_context_is_none():
    s = CatalogFileSink()
    with pytest.raises(SinkException):
        s.delete(None)


def test_delete_raises_when_target_key_missing():
    s = CatalogFileSink()
    with pytest.raises(SinkException):
        s.delete({"other": "value"})


def test_delete_raises_when_ls_returns_empty(monkeypatch):

    class MockDbUtilsFsEmpty:
        @staticmethod
        def ls(target: str):
            return []

    class MockDbUtilsEmpty:
        fs = MockDbUtilsFsEmpty()

    class MockUtilsDatabricksEmpty:
        @staticmethod
        def get_dbutils():
            return MockDbUtilsEmpty()

    monkeypatch.setattr(catsink, "UtilsDatabricks", MockUtilsDatabricksEmpty)
    s = CatalogFileSink()
    with pytest.raises(SinkException):
        s.delete({"target": "dbfs:/out/"})