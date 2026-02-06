"""Delta Lake table source implementation.

This module provides:
- DeltaTableSource: abstract base class for reading delta lake format datasets
  and returning pandas DataFrames with configurable storage options.
"""

from abc import ABC, abstractmethod
import logging
from typing import Any
from pandas import DataFrame
from deltalake import DeltaTable
from deltalake.exceptions import TableNotFoundError

from tgedr_dataops_abs.source import Source, SourceException, NoSourceException


logger = logging.getLogger()


class DeltaTableSource(Source, ABC):
    """abstract class used to read delta lake format datasets returning a pandas dataframe."""

    CONTEXT_KEY_URL: str = "url"
    CONTEXT_KEY_COLUMNS: str = "columns"

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        """Initialize the DeltaTableSource with optional configuration."""
        super().__init__(config=config)

    @property
    @abstractmethod
    def _storage_options(self) -> Any:
        return None  # pragma: no cover

    def get(self, context: dict[str, Any] | None = None) -> DataFrame:
        """Retrieves a delta lake table."""
        logger.info(f"[get|in] ({context})")
        result: DataFrame = None

        if self.CONTEXT_KEY_URL not in context:
            raise SourceException(f"you must provide context for {self.CONTEXT_KEY_URL}")

        columns: list[str] = None
        if self.CONTEXT_KEY_COLUMNS in context:
            columns = context[self.CONTEXT_KEY_COLUMNS]

        try:
            delta_table = DeltaTable(
                table_uri=context[self.CONTEXT_KEY_URL], storage_options=self._storage_options, without_files=True
            )
            result = delta_table.to_pandas(columns=columns)
        except TableNotFoundError as exc:
            raise NoSourceException(f"could not find delta table: {context[self.CONTEXT_KEY_URL]}") from exc

        logger.info(f"[get|out] => {result}")
        return result
