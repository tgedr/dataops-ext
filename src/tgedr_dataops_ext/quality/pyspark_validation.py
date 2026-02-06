"""Pyspark DataFrame validation implementation module.

This module provides the Pyspark-specific implementation of Great Expectations validation.
"""

from great_expectations.execution_engine import ExecutionEngine
from great_expectations.execution_engine import SparkDFExecutionEngine
from tgedr_dataops_abs.great_expectations_validation import GreatExpectationsValidation


class PysparkValidation(GreatExpectationsValidation):
    """Pyspark DataFrame validation implementation."""

    def _get_execution_engine(self, batch_data_dict: dict) -> ExecutionEngine:
        """Get the execution engine used by the validation implementation.

        Returns
        -------
        ExecutionEngine
            The execution engine instance.
        """
        return SparkDFExecutionEngine(batch_data_dict=batch_data_dict)
