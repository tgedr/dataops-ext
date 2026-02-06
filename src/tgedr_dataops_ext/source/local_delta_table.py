"""Local Delta Table source implementation.

This module provides the LocalDeltaTable class for reading Delta Lake format datasets
from the local filesystem using Python only (PySpark not required), returning pandas DataFrames.
"""

import logging
import re
from typing import Any
import glob
from pathlib import Path

from tgedr_dataops_ext.source.delta_table_source import DeltaTableSource
from tgedr_dataops_abs.source import SourceException


logger = logging.getLogger()


class LocalDeltaTable(DeltaTableSource):
    """class used to read delta lake format datasets from local fs with python only, pyspark not needed, returning a pandas dataframe."""

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        """Initialize LocalDeltaTable with optional configuration.

        Args:
            config: Optional configuration dictionary for the delta table source.
        """
        super().__init__(config=config)

    @property
    def _storage_options(self) -> Any:
        return None

    def list(self, context: dict[str, Any] | None = None) -> list[str]:
        """Lists the available delta lake datasets in the url provided."""
        logger.info(f"[list|in] ({context})")

        result: list[str] = []
        if self.CONTEXT_KEY_URL not in context:
            raise SourceException(f"you must provide context for {self.CONTEXT_KEY_URL}")

        url = context[self.CONTEXT_KEY_URL]
        if not Path(url).is_dir():
            raise SourceException(f"not a delta lake url: {url}")

        matches: set[str] = set()
        pattern: str = f".*{url}/(.*)/_delta_log/.*"
        for entry in glob.iglob(url + "**/**", recursive=True):  # noqa: PTH207
            match = re.search(pattern, entry)
            if match:
                matches.add(match.group(1))

        result = list(matches)

        logger.info(f"[list] result: {result}")
        logger.info(f"[list|out] => result len: {len(result)}")
        return result
