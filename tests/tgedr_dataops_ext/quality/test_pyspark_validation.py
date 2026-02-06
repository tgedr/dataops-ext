"""Integration tests for PySpark DataFrame validation with Great Expectations.

These tests use Great Expectations 1.9.1 API without mocking.
"""

import pytest

from tgedr_dataops_abs.great_expectations_validation import (
    GreatExpectationsValidation,
    ValidationError,
)
from tgedr_dataops_ext.quality.pyspark_validation import PysparkValidation

def test_pyspark_dataframe_validation_success(spark):
    """Test successful validation of a PySpark DataFrame."""
    # Create test DataFrame
    df = spark.createDataFrame([
        (1, "Alice", 25, 85.5),
        (2, "Bob", 30, 92.0),
        (3, "Charlie", 35, 78.5),
        (4, "Diana", 28, 88.0)
    ], ["id", "name", "age", "score"])
    
    # Define expectations
    expectations = {
        "expectation_suite_name": "user_data_suite",
        "expectations": [
            {
                "expectation_type": "expect_column_to_exist",
                "kwargs": {"column": "name"}
            },
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "name"}
            },
            {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {"column": "age", "min_value": 18, "max_value": 100}
            },
            {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {"column": "score", "min_value": 0.0, "max_value": 100.0}
            }
        ]
    }
    
    # Validate
    impl = PysparkValidation()
    result = impl.validate(df, expectations)
    
    # Verify results
    assert result is not None
    assert isinstance(result, dict)
    assert "success" in result
    assert result["success"] is True, f"Expected validation to succeed, got: {result}"
    assert "results" in result
    assert "statistics" in result
    assert result["statistics"]["successful_expectations"] == 4
    assert result["statistics"]["unsuccessful_expectations"] == 0


def test_pyspark_dataframe_validation_with_failures(spark):
    """Test validation failure when data doesn't meet expectations."""
    # Create DataFrame with issues
    df = spark.createDataFrame([
        (1, "Alice", 25),
        (2, None, 30),  # Has null value
        (3, "Charlie", 150)  # Age 150 exceeds max
    ], ["id", "name", "age"])
    
    expectations = {
        "expectation_suite_name": "strict_user_suite",
        "expectations": [
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "name"}
            },
            {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {"column": "age", "min_value": 18, "max_value": 100}
            }
        ]
    }
    
    # Validate
    impl = PysparkValidation()
    result = impl.validate(df, expectations)
    
    # Verify validation failed
    assert result is not None
    assert "success" in result
    assert result["success"] is False, "Expected validation to fail"
    assert "results" in result
    assert len(result["results"]) > 0, "Should have failure details"
    assert result["statistics"]["unsuccessful_expectations"] > 0


def test_pyspark_dataframe_validation_empty_dataframe(spark):
    """Test validation with empty DataFrame."""
    # Create empty DataFrame with schema
    df = spark.createDataFrame([], "id: int, value: string")
    
    expectations = {
        "expectation_suite_name": "empty_suite",
        "expectations": [
            {
                "expectation_type": "expect_column_to_exist",
                "kwargs": {"column": "id"}
            },
            {
                "expectation_type": "expect_column_to_exist",
                "kwargs": {"column": "value"}
            }
        ]
    }
    
    # Validate
    impl = PysparkValidation()
    result = impl.validate(df, expectations)
    
    # Verify results - column existence checks should pass even for empty DataFrame
    assert result is not None
    assert result["success"] is True


def test_pyspark_dataframe_validation_complex_expectations(spark):
    """Test validation with multiple complex expectations."""
    df = spark.createDataFrame([
        (1, "electronics", 299.99, 10),
        (2, "electronics", 499.99, 5),
        (3, "books", 19.99, 100),
        (4, "books", 29.99, 50),
        (5, "electronics", 399.99, 0)
    ], ["product_id", "category", "price", "stock"])
    
    expectations = {
        "expectation_suite_name": "product_suite",
        "expectations": [
            {
                "expectation_type": "expect_table_row_count_to_equal",
                "kwargs": {"value": 5}
            },
            {
                "expectation_type": "expect_column_values_to_be_in_set",
                "kwargs": {
                    "column": "category",
                    "value_set": ["electronics", "books", "clothing"]
                }
            },
            {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {
                    "column": "price",
                    "min_value": 0.0,
                    "max_value": 10000.0
                }
            },
            {
                "expectation_type": "expect_column_min_to_be_between",
                "kwargs": {
                    "column": "stock",
                    "min_value": 0,
                    "max_value": 0
                }
            }
        ]
    }
    
    # Validate
    impl = PysparkValidation()
    result = impl.validate(df, expectations)
    
    # Verify results
    assert result is not None
    assert result["success"] is True
    assert result["statistics"]["evaluated_expectations"] == 4
    assert result["statistics"]["successful_expectations"] == 4


def test_pyspark_dataframe_validation_single_column(spark):
    """Test validation with a single column."""
    df = spark.createDataFrame([
        (20.5,),
        (21.0,),
        (19.8,),
        (22.3,),
        (20.1,)
    ], ["temperature"])
    
    expectations = {
        "expectation_suite_name": "temperature_suite",
        "expectations": [
            {
                "expectation_type": "expect_column_to_exist",
                "kwargs": {"column": "temperature"}
            },
            {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {
                    "column": "temperature",
                    "min_value": 15.0,
                    "max_value": 25.0
                }
            }
        ]
    }
    
    # Validate
    impl = PysparkValidation()
    result = impl.validate(df, expectations)
    
    # Verify results
    assert result["success"] is True
    assert result["statistics"]["successful_expectations"] == 2


def test_pyspark_dataframe_validation_with_null_handling(spark):
    """Test validation with explicit null value handling."""
    df = spark.createDataFrame([
        (1, "Active", 100.0),
        (2, "Inactive", None),  # Null value in amount
        (3, "Active", 200.0),
        (4, None, 150.0)  # Null value in status
    ], ["id", "status", "amount"])
    
    expectations = {
        "expectation_suite_name": "null_handling_suite",
        "expectations": [
            {
                "expectation_type": "expect_column_to_exist",
                "kwargs": {"column": "id"}
            },
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "id"}
            }
        ]
    }
    
    # Validate - should pass because id column has no nulls
    impl = PysparkValidation()
    result = impl.validate(df, expectations)
    
    # Verify results
    assert result is not None
    assert result["success"] is True
    assert result["statistics"]["successful_expectations"] == 2
