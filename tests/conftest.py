"""test configurations."""

from pathlib import Path
import sys
import tempfile
import pytest
from pandas import DataFrame
from pandas.testing import assert_frame_equal
from tgedr_dataops_ext.commons.utils_spark import UtilsSpark
from pyspark.sql import SparkSession

sys.path.insert(0, Path(__file__).parent.parent.joinpath("src").absolute())  # isort:skip

def assert_frames_are_equal(actual: DataFrame, expected: DataFrame, sort_columns: list[str], abs_tol: float = None):
    results_sorted = actual.sort_values(by=sort_columns).reset_index(drop=True)
    expected_sorted = expected.sort_values(by=sort_columns).reset_index(drop=True)
    if abs_tol is not None:
        assert_frame_equal(
            results_sorted,
            expected_sorted,
            check_dtype=False,
            check_exact=False,
            check_like=True,
            atol=abs_tol,
        )
    else:
        assert_frame_equal(
            results_sorted,
            expected_sorted,
            check_dtype=False,
            check_exact=False,
            check_like=True,
        )

@pytest.fixture(scope="session")
def resources_folder() -> str:
    """Provides the location of test respource folder.

    Returns:
        test resources folder path

    """
    return Path(__file__).parent.joinpath("resources").absolute()


@pytest.fixture(scope="session")
def temporary_folder() -> str:
    """Provides a temporary folder for testing purposes.

    Returns:
        temporary folder

    """
    # pylint: disable=consider-using-with
    _folder = tempfile.TemporaryDirectory("+wb").name
    _path = Path(_folder)
    if not _path.exists():
        _path.mkdir(parents=True)
    return _folder


@pytest.fixture
def environment_mock(monkeypatch):
    monkeypatch.setenv("PYSPARK_IS_LOCAL", "1")
    monkeypatch.setenv("PYSPARK_PYTHON", sys.executable)
    monkeypatch.setenv("PYSPARK_DRIVER_PYTHON", sys.executable)


@pytest.fixture
def environment_mock_another(monkeypatch):
    monkeypatch.setenv("NOT_AWS_CLOUD", "1")
    monkeypatch.setenv("PYSPARK_PYTHON", sys.executable)
    monkeypatch.setenv("PYSPARK_DRIVER_PYTHON", sys.executable)


@pytest.fixture
def spark(environment_mock) -> SparkSession:
    return UtilsSpark.get_local_spark_session()
