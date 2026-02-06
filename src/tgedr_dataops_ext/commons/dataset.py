"""Dataset module for wrapping dataframes with metadata.

This module provides:
- Dataset: an immutable dataclass that combines a DataFrame with Metadata.
"""

from dataclasses import dataclass
import json
from pyspark.sql import DataFrame
from tgedr_dataops_ext.commons.metadata import Metadata


@dataclass(frozen=True)
class Dataset:
    """Utility immutable class to wrap up a dataframe along with metadata."""

    __slots__ = ["data", "metadata"]
    metadata: Metadata
    data: DataFrame

    def as_dict(self) -> dict:
        """Serialize the dataset as a dictionary."""
        return {"metadata": self.metadata.as_dict(), "data": str(self.data.__repr__)}

    def __str__(self) -> str:
        """Serialize the dataset as a json string."""
        return json.dumps(self.as_dict())
