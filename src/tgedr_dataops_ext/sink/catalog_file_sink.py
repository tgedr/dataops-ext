"""The module provides the CatalogFileSink class.

For copying and deleting files or directories using Databricks utilities, as part of the dataops-ext package.
"""

import logging
from typing import Any

from tgedr_dataops_ext.commons.utils_databricks import UtilsDatabricks
from tgedr_dataops_abs.sink import Sink, SinkException


logger = logging.getLogger(__name__)


class CatalogFileSink(Sink):
    """CatalogFileSink provides methods to copy and delete files or directories using Databricks utilities.

    It expects context dictionaries with source and target paths for its operations.
    """

    CONTEXT_SOURCE_PATH = "source"
    CONTEXT_TARGET_PATH = "target"

    def __init__(self, config: dict[str, Any]|None = None) -> None:
        """Initialize CatalogFileSink instance.

        Args:
            config (dict[str, Any] | None): Optional configuration dictionary.
        """
        Sink.__init__(self, config=config)
        self.__dbutils = None

    @property
    def _dbutils(self) -> object:
        """Lazily initialises and returns the Databricks dbutils object."""
        if self.__dbutils is None:
            self.__dbutils = UtilsDatabricks.get_dbutils()
        return self.__dbutils

    def put(self, context: dict[str, Any]|None = None) -> Any:
        """Copies a file or directory from the source path to the target path using Databricks utilities.

        Args:
            context (dict[str, Any] | None): Context dictionary containing source and target paths.

        Raises:
            SinkException: If context is missing or required keys are not provided.
        """
        logger.info(f"[put|in] ({context})")

        if context is None or self.CONTEXT_SOURCE_PATH not in context:
            raise SinkException(f"you must provide context for {self.CONTEXT_SOURCE_PATH}")
        if context is None or self.CONTEXT_TARGET_PATH not in context:
            raise SinkException(f"you must provide context for {self.CONTEXT_TARGET_PATH}")

        source = context[self.CONTEXT_SOURCE_PATH]
        target = context[self.CONTEXT_TARGET_PATH]

        self._dbutils.fs.cp(source, target) # pyright: ignore[reportAttributeAccessIssue]
        logger.info("[put|out]")

    def delete(self, context: dict[str, Any]|None = None) -> None:
        """Deletes a file or directory at the target path using Databricks utilities.

        Args:
            context (dict[str, Any] | None): Context dictionary containing the target path.

        Raises:
            SinkException: If context is missing or required keys are not provided.
        """
        logger.info(f"[delete|in] ({context})")

        if context is None or self.CONTEXT_TARGET_PATH not in context:
            raise SinkException(f"you must provide context for {self.CONTEXT_TARGET_PATH}")

        target = context[self.CONTEXT_TARGET_PATH]
        if 0 < len(self._dbutils.fs.ls(target)): # pyright: ignore[reportAttributeAccessIssue]
            self._dbutils.fs.rm(target, True) # pyright: ignore[reportAttributeAccessIssue]  # noqa: FBT003
        else:
            raise SinkException(f"[delete] is it a dir or a folder? {target}")

        logger.info("[delete|out]")
