"""Module providing VolumeFiles for Databricks Volumes as Source and Sink."""

import fnmatch
import os
import shutil
from pathlib import Path
from dataclasses import dataclass
from typing import Any
import logging

from tgedr_dataops_abs.sink import Sink, SinkException
from tgedr_dataops_abs.source import Source, SourceException, NoSourceException
from tgedr_dataops_ext.commons.utils_spark import UtilsSpark


logger = logging.getLogger(__name__)


@dataclass
class _FileInfo:
    """Mimics the Databricks FileInfo object returned by dbutils.fs.ls()."""

    path: str
    name: str
    size: int


class _LocalFs:
    """Local filesystem adapter implementing the same interface as ``dbutils.fs``.

    Enables VolumeFiles to operate on the local filesystem without Databricks.
    """

    def ls(self, path: str) -> list[_FileInfo]:
        path = path.rstrip("/")
        if not Path(path).exists():
            raise FileNotFoundError(f"path does not exist: {path}")
        if Path(path).is_file():
            return [_FileInfo(path=path, name=Path(path).name, size=Path(path).stat().st_size)]
        entries: list[_FileInfo] = []
        for name in os.listdir(path):
            full = str(Path(path) / name)
            if Path(full).is_dir():
                entries.append(_FileInfo(path=full + "/", name=name, size=0))
            else:
                entries.append(_FileInfo(path=full, name=name, size=Path(full).stat().st_size))
        return entries

    def cp(self, src: str, dst: str, *, recurse: bool = False) -> bool:
        src = src.rstrip("/")
        dst = dst.rstrip("/")
        # keep the parameter to maintain signature compatibility; mark as used
        _ = recurse
        if not Path(src).exists():
            raise FileNotFoundError(f"source not found: {src}")
        dst_parent = Path(dst).parent
        dst_parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
        return True

    def rm(self, path: str, *, recurse: bool = False) -> bool:
        path = path.rstrip("/")
        if not Path(path).exists():
            raise FileNotFoundError(f"path not found: {path}")
        if Path(path).is_dir() and recurse:
            shutil.rmtree(path)
        else:
            Path(path).unlink()
        return True

    def mkdirs(self, path: str) -> bool:
        Path(path.rstrip("/")).mkdir(parents=True, exist_ok=True)
        return True


class VolumeFiles(Source, Sink):
    """Source and Sink implementation for Databricks Volumes using dbutils.

    Includes an optional local filesystem fallback.

    As a Source it lists and reads files from a Databricks Volume path.
    As a Sink it copies files into a Databricks Volume path.

    By default uses ``pyspark.dbutils.DBUtils`` to interact with the filesystem
    (works when running on Databricks). Pass ``config = {"use_local_fs": True}``
    to use the local filesystem instead.
    """

    CONFIG_KEY_USE_LOCAL_FS = "use_local_fs"

    CONTEXT_KEY_VOLUME_PATH = "volume_path"
    CONTEXT_KEY_FILE_PATTERN = "file_pattern"
    CONTEXT_KEY_OUTPUT_URL = "output_url"
    CONTEXT_KEY_SOURCE_PATH = "source_path"
    CONTEXT_KEY_TARGET_PATH = "target_path"
    CONTEXT_KEY_OVERWRITE = "overwrite"

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        """Initializes the VolumeFiles instance.

        Args:
            config: Optional configuration dictionary.
        """
        Source.__init__(self, config=config)
        Sink.__init__(self, config=config)

        use_local_fs: bool = (config.get(self.CONFIG_KEY_USE_LOCAL_FS, "false").lower() if config else "false") == "true"

        if use_local_fs:
            self._fs = _LocalFs()
        else:
            from pyspark.dbutils import DBUtils  # type: ignore  # noqa: PGH003
            spark = UtilsSpark.get_spark_session()
            self._fs = DBUtils(spark).fs

    def __path_exists(self, path: str) -> bool:
        try:
            self._fs.ls(path)
            return True  # noqa: TRY300
        except Exception:  # noqa: BLE001
            return False

    def __is_file(self, path: str) -> bool:
        try:
            entries = self._fs.ls(path)
            # if ls returns exactly one entry whose path matches the queried path, it is a file
            if len(entries) == 1:
                entry_path = entries[0].path.rstrip("/")
                return entry_path == path.rstrip("/")
            return False  # noqa: TRY300
        except Exception:  # noqa: BLE001
            return False

    def list(self, context: dict[str, Any] | None = None) -> list[str]:
        """Lists files in a Databricks Volume path, optionally filtered by a glob pattern.

        Context keys:
            - volume_path (required): the volume path to list files from (e.g. /Volumes/catalog/schema/volume_name)
            - file_pattern (optional): glob pattern to filter files (e.g. "*.zip"), defaults to "*"
        """
        logger.info(f"[list|in] ({context})")

        if not context or self.CONTEXT_KEY_VOLUME_PATH not in context:
            raise SourceException(f"[list] you must provide context for {self.CONTEXT_KEY_VOLUME_PATH}")

        volume_path: str = context[self.CONTEXT_KEY_VOLUME_PATH]
        file_pattern: str = context.get(self.CONTEXT_KEY_FILE_PATTERN, "*")

        try:
            entries = self._fs.ls(volume_path)
        except Exception as ex:
            raise NoSourceException(f"[list] volume path does not exist or is not accessible: {volume_path} - {ex}") from ex

        # entries returned by dbutils.fs.ls have .path and .size attributes
        # directories have paths ending with '/', files do not (or have size > 0)
        result: list[str] = sorted(
            [
                entry.path
                for entry in entries
                if not entry.path.endswith("/") and fnmatch.fnmatch(Path(entry.path).name, file_pattern)
            ]
        )

        logger.info(f"[list|out] => {len(result)} file(s)")
        return result

    def get(self, context: dict[str, Any] | None = None) -> str:
        """Downloads a file from a Databricks Volume path to a local output location.

        Uses dbutils.fs.cp() to copy the file.

        Context keys:
            - volume_path (required): the full path to the source file (e.g. /Volumes/catalog/schema/volume/file.zip)
            - output_url (required): the destination path to download the file to
        """
        logger.info(f"[get|in] ({context})")

        if not context or self.CONTEXT_KEY_VOLUME_PATH not in context:
            raise SourceException(f"[get] you must provide context for {self.CONTEXT_KEY_VOLUME_PATH}")
        if not context or self.CONTEXT_KEY_OUTPUT_URL not in context:
            raise SourceException(f"[get] you must provide context for {self.CONTEXT_KEY_OUTPUT_URL}")

        file_path: str = context[self.CONTEXT_KEY_VOLUME_PATH]
        output_url: str = context[self.CONTEXT_KEY_OUTPUT_URL]

        if not self.__path_exists(file_path):
            raise SourceException(f"[get] file does not exist: {file_path}")
        if not self.__is_file(file_path):
            raise SourceException(f"[get] path is not a file: {file_path}")

        try:
            self._fs.cp(file_path, output_url)
        except Exception as ex:
            raise SourceException(f"[get] failed to download file: {file_path} to {output_url}") from ex

        logger.info(f"[get|out] => {output_url}")
        return output_url

    def put(self, context: dict[str, Any] | None = None) -> str:
        """Copies a file into a Databricks Volume path using dbutils.fs.cp().

        Context keys:
            - source_path (required): source path to copy from
            - target_path (required): destination path in the volume (e.g. /Volumes/catalog/schema/volume/file.zip)
            - overwrite (optional): whether to overwrite if target exists, defaults to False
        """
        logger.info(f"[put|in] ({context})")

        if not context or self.CONTEXT_KEY_SOURCE_PATH not in context:
            raise SinkException(f"[put] you must provide context for {self.CONTEXT_KEY_SOURCE_PATH}")
        if not context or self.CONTEXT_KEY_TARGET_PATH not in context:
            raise SinkException(f"[put] you must provide context for {self.CONTEXT_KEY_TARGET_PATH}")

        source_path: str = context[self.CONTEXT_KEY_SOURCE_PATH]
        target_path: str = context[self.CONTEXT_KEY_TARGET_PATH]
        overwrite: bool = context.get(self.CONTEXT_KEY_OVERWRITE, False)

        if not self.__path_exists(source_path):
            raise SinkException(f"[put] source file does not exist: {source_path}")
        if not self.__is_file(source_path):
            raise SinkException(f"[put] source path is not a file: {source_path}")
        if self.__path_exists(target_path) and not overwrite:
            raise SinkException(f"[put] target file already exists: {target_path} (set overwrite=True to replace)")

        try:
            # mkdirs for the parent directory
            target_parent = str(target_path).rsplit("/", 1)[0]
            self._fs.mkdirs(target_parent)
            self._fs.cp(source_path, target_path)
        except Exception as ex:
            raise SinkException(f"[put] failed to copy file to: {target_path}") from ex

        logger.info(f"[put|out] => {target_path}")
        return target_path

    def delete(self, context: dict[str, Any] | None = None) -> None:
        """Deletes a file from a Databricks Volume path.

        Context keys:
            - volume_path (required): the full path to the file to delete
        """
        logger.info(f"[delete|in] ({context})")

        if not context or self.CONTEXT_KEY_VOLUME_PATH not in context:
            raise SinkException(f"[delete] you must provide context for {self.CONTEXT_KEY_VOLUME_PATH}")

        file_path: str = context[self.CONTEXT_KEY_VOLUME_PATH]

        if not self.__path_exists(file_path):
            raise SinkException(f"[delete] file does not exist: {file_path}")
        if not self.__is_file(file_path):
            raise SinkException(f"[delete] path is not a file: {file_path}")

        try:
            self._fs.rm(file_path)
        except Exception as ex:
            raise SinkException(f"[delete] failed to delete file: {file_path}") from ex

        logger.info("[delete|out]")


