"""Databricks catalog file source module.

Provides `CatalogFileSource`, a `Source` implementation that uses
``dbutils.fs`` to interact with files stored in a Databricks-accessible
file system (DBFS, S3, ADLS, etc.).

Typical use cases:
- Listing files in a remote directory, optionally filtered by suffix.
- Copying one or more files to a target location.
- Retrieving size and modification-time metadata for files in a directory.
"""
import logging
from pathlib import Path
from typing import Any
from collections.abc import Iterable
from tgedr_dataops_ext.commons.utils_databricks import UtilsDatabricks
from tgedr_dataops_abs.source import SourceException, Source


logger = logging.getLogger(__name__)


class CatalogFileSource(Source):
  """A `Source` that reads and copies files via Databricks ``dbutils.fs``.

  All methods are driven by a ``context`` dictionary whose keys are defined
  as class-level constants (``CONTEXT_KEY_*``). The ``dbutils`` object is
  obtained lazily on first access via `UtilsDatabricks`.

  Context keys
  ------------
  CONTEXT_KEY_SOURCE : str
      Path to the source directory or file (``"source"``).
  CONTEXT_KEY_TARGET : str
      Destination path for copy operations (``"target"``).
  CONTEXT_KEY_FILES : str
      List of file paths to copy (``"files"``).
  CONTEXT_KEY_FILE : str
      Single file path to copy (``"file"``).
  CONTEXT_KEY_SUFFIX : str
      Optional file-extension filter for ``list`` (``"suffix"``).
  """

  CONTEXT_KEY_SOURCE = "source"
  CONTEXT_KEY_TARGET = "target"
  CONTEXT_KEY_FILES = "files"
  CONTEXT_KEY_FILE = "file"
  CONTEXT_KEY_SUFFIX = "suffix"

  def __init__(self, config: dict[str, Any] | None = None) -> None:
      """Initializes the CatalogFileSource instance.

      Args:
          config (dict[str, Any] | None): Optional configuration dictionary.
      """
      super().__init__(config=config)
      self.__dbutils = None

  @property
  def _dbutils(self) -> object:
      """Lazily initialises and returns the Databricks dbutils object."""
      if self.__dbutils is None:
          self.__dbutils = UtilsDatabricks.get_dbutils()
      return self.__dbutils

  def list(self, context: dict[str, Any] | None = None) -> list[str]:
      """Lists files in the specified source directory from the context.

      Optionally filters files by suffix if provided in the context.

      Args:
          context (dict[str, Any] | None): Context containing source path and optional suffix.

      Returns:
          list[str]: List of file paths.

      Raises:
          SourceException: If required context keys are missing.
      """
      logger.info(f"[list|in] ({context})")

      result: list[str] = []
      if context is None or self.CONTEXT_KEY_SOURCE not in context:
          raise SourceException(f"you must provide context for {self.CONTEXT_KEY_SOURCE}")

      source: str = context[self.CONTEXT_KEY_SOURCE]
      try:
          result = [fi.path for fi in self._dbutils.fs.ls(source)] # pyright: ignore[reportAttributeAccessIssue]
      except Exception as e:  # noqa: BLE001
          logger.warning(f"[list] exception listing files in {source}: {e}")
      else:
        if self.CONTEXT_KEY_SUFFIX in context:
            suffix: str = context[self.CONTEXT_KEY_SUFFIX]
            result = [f for f in result if f.endswith(suffix)]

      logger.debug(f"[list|out] => {result}")
      return result

  def get(self, context: dict[str, Any] | None = None) -> Any:
      """Copies files from the source to the target location specified in the context.

      Accepts either a single file (``file`` key) or a list of files (``files`` key).
      Both keys may be present; each is processed independently.

      Args:
          context (dict[str, Any] | None): Must contain ``target`` and at least one of
              ``file`` (single path) or ``files`` (list of paths).

      Returns:
          list[str]: Paths of the copied files at the target location.

      Raises:
          SourceException: If required context keys are missing.
      """
      logger.info(f"[get|in] ({context})")

      result: list[str] = []
      if context is None or (self.CONTEXT_KEY_FILES not in context and self.CONTEXT_KEY_FILE not in context):
          raise SourceException(
              f"you must provide context for either {self.CONTEXT_KEY_FILES} or {self.CONTEXT_KEY_FILE}"
          )
      if self.CONTEXT_KEY_TARGET not in context:
          raise SourceException(f"you must provide context for {self.CONTEXT_KEY_TARGET}")

      target = context[self.CONTEXT_KEY_TARGET]

      if self.CONTEXT_KEY_FILES in context:
          files = context[self.CONTEXT_KEY_FILES]
          for file in files:
              self._dbutils.fs.cp(file, target) # pyright: ignore[reportAttributeAccessIssue]
              result.append(f"{target}{Path(file).name}")

      if self.CONTEXT_KEY_FILE in context:
          file = context[self.CONTEXT_KEY_FILE]
          self._dbutils.fs.cp(file, target) # pyright: ignore[reportAttributeAccessIssue]
          target_file = f"{target}{Path(file).name}" if target.endswith("/") else target
          result.append(target_file)

      logger.info(f"[get|out] => {result}")
      return result

  def get_metadata(self, context: dict[str, Any] | None = None) -> dict[str, Any] | Iterable[dict[str, Any]]:
      """Retrieves file metadata from the source directory specified in the context.

      Uses ``dbutils.fs.ls`` to inspect the source path and returns modification time
      and size for each entry. If the source contains exactly one file, a single
      ``dict`` is returned; otherwise a list of dicts is returned.

      Args:
          context (dict[str, Any] | None): Must contain ``source`` with the path to inspect.

      Returns:
          dict[str, Any] | Iterable[dict[str, Any]]: A single metadata dict when the
              source contains one file, or a list of metadata dicts for multiple files.
              Each dict has keys ``LastModified`` and ``ContentLength``.

      Raises:
          SourceException: If ``source`` is missing from the context.
      """
      logger.info(f"[get_metadata|in] ({context})")

      if context is None or self.CONTEXT_KEY_SOURCE not in context:
          raise SourceException(f"you must provide context for {self.CONTEXT_KEY_SOURCE}")

      source: str = context[self.CONTEXT_KEY_SOURCE]
      response = self._dbutils.fs.ls(source) # pyright: ignore[reportAttributeAccessIssue]

      metadatas: list[dict[str, Any]] = [
          {"LastModified": resp.modificationTime, "ContentLength": resp.size}
          for resp in response
      ]

      result = metadatas[0] if 1 == len(metadatas) else metadatas

      logger.info(f"[get_metadata|out] => {result}")
      return result
