"""Unit tests for EtlDatabricks."""
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from tgedr_dataops_abs.etl import EtlException
from tgedr_dataops_ext.commons.etl_databricks import EtlDatabricks


# ---------------------------------------------------------------------------
# Minimal concrete implementation for testing
# ---------------------------------------------------------------------------

class ConcreteEtl(EtlDatabricks):
    """Minimal concrete subclass used only in tests."""

    def extract(self) -> None:
        pass

    def transform(self) -> None:
        pass

    def load(self) -> dict[str, str] | None:
        return None


# ---------------------------------------------------------------------------
# __init__ / run_id resolution
# ---------------------------------------------------------------------------

class TestInit:
    def test_no_configuration_sets_no_run_id(self):
        etl = ConcreteEtl()
        assert etl._run_id == EtlDatabricks._NO_RUN_ID

    def test_configuration_without_run_id_sets_no_run_id(self):
        etl = ConcreteEtl(configuration={"other_key": "value"})
        assert etl._run_id == EtlDatabricks._NO_RUN_ID

    def test_configuration_with_run_id_is_stored(self):
        etl = ConcreteEtl(configuration={"run_id": "42"})
        assert etl._run_id == "42"

    def test_configuration_is_passed_to_parent(self):
        config = {"run_id": "1", "param": "value"}
        etl = ConcreteEtl(configuration=config)
        assert etl._configuration == config


# ---------------------------------------------------------------------------
# run()
# ---------------------------------------------------------------------------

class TestRun:
    def test_run_calls_etl_steps_in_order(self):
        etl = ConcreteEtl()
        calls = []
        etl.extract = lambda: calls.append("extract")
        etl.validate_extract = lambda: calls.append("validate_extract")
        etl.transform = lambda: calls.append("transform")
        etl.validate_transform = lambda: calls.append("validate_transform")
        etl.load = lambda: calls.append("load") or None

        etl.run()

        assert calls == ["extract", "validate_extract", "transform", "validate_transform", "load"]

    def test_run_returns_load_result(self):
        etl = ConcreteEtl()
        etl.load = lambda: {"key": "value"}

        result = etl.run()

        assert result == {"key": "value"}

    def test_run_without_run_id_does_not_call_dbutils(self):
        etl = ConcreteEtl()
        etl.load = lambda: {"key": "value"}

        with patch("tgedr_dataops_ext.commons.etl_databricks.UtilsDatabricks.get_dbutils") as mock_get:
            etl.run()
            mock_get.assert_not_called()

    def test_run_with_run_id_sets_task_values(self):
        etl = ConcreteEtl(configuration={"run_id": "99"})
        etl.load = lambda: {"out_key": "out_val"}

        mock_task_values = MagicMock()
        mock_dbutils = MagicMock()
        mock_dbutils.jobs.taskValues = mock_task_values

        with patch("tgedr_dataops_ext.commons.etl_databricks.UtilsDatabricks.get_dbutils", return_value=mock_dbutils):
            etl.run()

        mock_task_values.set.assert_called_once_with(key="out_key", value="out_val")

    def test_run_with_run_id_and_none_result_does_not_call_dbutils(self):
        etl = ConcreteEtl(configuration={"run_id": "99"})
        etl.load = lambda: None

        with patch("tgedr_dataops_ext.commons.etl_databricks.UtilsDatabricks.get_dbutils") as mock_get:
            etl.run()
            mock_get.assert_not_called()

    def test_run_with_run_id_warns_when_dbutils_jobs_missing(self, caplog):
        etl = ConcreteEtl(configuration={"run_id": "99"})
        etl.load = lambda: {"k": "v"}

        mock_dbutils = MagicMock(spec=[])  # no 'jobs' attribute

        with patch("tgedr_dataops_ext.commons.etl_databricks.UtilsDatabricks.get_dbutils", return_value=mock_dbutils):
            import logging
            with caplog.at_level(logging.WARNING):
                etl.run()

        assert any("dbutils.jobs.taskValues is not accessible" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# read_task_value()
# ---------------------------------------------------------------------------

class TestReadTaskValue:
    def test_returns_value_when_dbutils_accessible(self):
        mock_task_values = MagicMock()
        mock_task_values.get.return_value = "result_value"
        mock_dbutils = MagicMock()
        mock_dbutils.jobs.taskValues = mock_task_values

        with patch("tgedr_dataops_ext.commons.etl_databricks.UtilsDatabricks.get_dbutils", return_value=mock_dbutils):
            result = EtlDatabricks._read_task_value("task1__param1")

        mock_task_values.get.assert_called_once_with(taskKey="task1", key="param1")
        assert result == "result_value"

    def test_returns_none_when_dbutils_not_accessible(self):
        mock_dbutils = MagicMock(spec=[])  # no 'jobs' attribute

        with patch("tgedr_dataops_ext.commons.etl_databricks.UtilsDatabricks.get_dbutils", return_value=mock_dbutils):
            result = EtlDatabricks._read_task_value("task1__param1")

        assert result is None


# ---------------------------------------------------------------------------
# inject_configuration decorator
# ---------------------------------------------------------------------------

class TestInjectConfiguration:
    def _make_etl(self, config: dict[str, Any] | None = None, run_id: str | None = None) -> ConcreteEtl:
        cfg = dict(config) if config else {}
        if run_id is not None:
            cfg["run_id"] = run_id
        return ConcreteEtl(configuration=cfg or None)

    def test_injects_value_from_configuration(self):
        etl = self._make_etl(config={"param": "injected"})

        @EtlDatabricks.inject_configuration
        def method(self, param: str) -> str:
            return param

        assert method(etl) == "injected"

    def test_uses_default_when_param_not_in_configuration(self):
        etl = self._make_etl()

        @EtlDatabricks.inject_configuration
        def method(self, param: str = "default_val") -> str:
            return param

        assert method(etl) == "default_val"

    def test_raises_etl_exception_for_missing_required_param(self):
        etl = self._make_etl()

        @EtlDatabricks.inject_configuration
        def method(self, required_param: str) -> str:
            return required_param

        with pytest.raises(EtlException, match="missing required configuration parameters"):
            method(etl)

    def test_task_value_param_resolved_when_run_id_set(self):
        etl = self._make_etl(run_id="42")

        mock_task_values = MagicMock()
        mock_task_values.get.return_value = "task_result"
        mock_dbutils = MagicMock()
        mock_dbutils.jobs.taskValues = mock_task_values

        @EtlDatabricks.inject_configuration
        def method(self, upstream__output: str) -> str:
            return upstream__output

        with patch("tgedr_dataops_ext.commons.etl_databricks.UtilsDatabricks.get_dbutils", return_value=mock_dbutils):
            result = method(etl)

        assert result == "task_result"

    def test_task_value_param_not_resolved_without_run_id(self):
        etl = self._make_etl()  # _NO_RUN_ID

        @EtlDatabricks.inject_configuration
        def method(self, upstream__output: str = "fallback") -> str:
            return upstream__output

        result = method(etl)
        assert result == "fallback"

    def test_configuration_overrides_default(self):
        etl = self._make_etl(config={"param": "from_config"})

        @EtlDatabricks.inject_configuration
        def method(self, param: str = "default_val") -> str:
            return param

        assert method(etl) == "from_config"
