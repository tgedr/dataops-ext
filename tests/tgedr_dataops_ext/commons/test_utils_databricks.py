"""Unit tests for UtilsDatabricks."""
from unittest.mock import MagicMock, patch

import pytest

from tgedr_dataops_ext.commons.utils_databricks import UtilsDatabricks


@pytest.fixture(autouse=True)
def reset_cache():
    """Reset the UtilsDatabricks singleton cache before each test."""
    UtilsDatabricks._UtilsDatabricks__DATABRICKS_UTILS = None # pyright: ignore[reportAttributeAccessIssue]
    yield
    UtilsDatabricks._UtilsDatabricks__DATABRICKS_UTILS = None # pyright: ignore[reportAttributeAccessIssue]


class TestGetDbutils:
    def test_returns_dbutils_when_dbutils_import_succeeds(self):
        mock_spark = MagicMock()
        mock_dbutils = MagicMock()

        with patch("tgedr_dataops_ext.commons.utils_databricks.UtilsSpark.get_spark_session", return_value=mock_spark), \
             patch.dict("sys.modules", {"pyspark.dbutils": MagicMock(DBUtils=MagicMock(return_value=mock_dbutils))}):
            result = UtilsDatabricks.get_dbutils()

        assert result == mock_dbutils

    def test_returns_none_when_dbutils_import_fails(self):
        mock_spark = MagicMock()

        with patch("tgedr_dataops_ext.commons.utils_databricks.UtilsSpark.get_spark_session", return_value=mock_spark), \
             patch("builtins.__import__", side_effect=ImportError("no module pyspark.dbutils")):
            result = UtilsDatabricks.get_dbutils()

        assert result is None

    def test_caches_dbutils_on_second_call(self):
        mock_spark = MagicMock()
        mock_dbutils = MagicMock()

        with patch("tgedr_dataops_ext.commons.utils_databricks.UtilsSpark.get_spark_session", return_value=mock_spark), \
             patch.dict("sys.modules", {"pyspark.dbutils": MagicMock(DBUtils=MagicMock(return_value=mock_dbutils))}):
            first = UtilsDatabricks.get_dbutils()
            second = UtilsDatabricks.get_dbutils()

        assert first is second

    def test_cache_is_returned_without_reimporting(self):
        mock_spark = MagicMock()
        mock_dbutils = MagicMock()

        # prime the cache
        UtilsDatabricks._UtilsDatabricks__DATABRICKS_UTILS = mock_dbutils # pyright: ignore[reportAttributeAccessIssue]

        with patch("tgedr_dataops_ext.commons.utils_databricks.UtilsSpark.get_spark_session", return_value=mock_spark) as mock_session:
            result = UtilsDatabricks.get_dbutils()

        mock_session.assert_called_once()  # spark session still fetched
        assert result is mock_dbutils
