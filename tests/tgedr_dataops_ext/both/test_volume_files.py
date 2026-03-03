import os
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from tgedr_dataops_abs.source import Source, SourceException
from tgedr_dataops_abs.sink import Sink, SinkException
from tgedr_dataops_ext.both.volume_files import VolumeFiles, _LocalFs


# --- fixtures ---


@pytest.fixture
def volume():
    return VolumeFiles(config={"use_local_fs": "true"})


@pytest.fixture
def vol_dir(tmp_path):
    """Creates a temporary volume directory with a few test files."""
    d = tmp_path / "volume"
    d.mkdir()
    (d / "alpha.zip").write_text("aaa")
    (d / "bravo.csv").write_text("bbb")
    (d / "charlie.zip").write_text("ccc")
    sub = d / "subdir"
    sub.mkdir()
    return str(d)


# --- constructor tests ---


def test_raises_when_no_config():
    with pytest.raises(Exception):
        VolumeFiles(config=None)


def test_creates_instance_with_use_local_fs():
    v = VolumeFiles(config={"use_local_fs": "true"})
    assert v is not None


def test_config_key_use_local_fs_constant():
    assert VolumeFiles.CONFIG_KEY_USE_LOCAL_FS == "use_local_fs"


# --- context key constants ---


def test_context_key_constants():
    assert VolumeFiles.CONTEXT_KEY_VOLUME_PATH == "volume_path"
    assert VolumeFiles.CONTEXT_KEY_FILE_PATTERN == "file_pattern"
    assert VolumeFiles.CONTEXT_KEY_OUTPUT_URL == "output_url"
    assert VolumeFiles.CONTEXT_KEY_SOURCE_PATH == "source_path"
    assert VolumeFiles.CONTEXT_KEY_TARGET_PATH == "target_path"
    assert VolumeFiles.CONTEXT_KEY_OVERWRITE == "overwrite"


# --- isinstance checks ---


def test_is_source_instance(volume):
    assert isinstance(volume, Source)


def test_is_sink_instance(volume):
    assert isinstance(volume, Sink)


# --- list tests ---


def test_list_raises_when_no_context(volume):
    with pytest.raises(SourceException):
        volume.list(context=None)


def test_list_raises_when_context_missing_volume_path(volume):
    with pytest.raises(SourceException):
        volume.list(context={"some_key": "value"})


def test_list_raises_when_volume_path_does_not_exist(volume):
    with pytest.raises(SourceException):
        volume.list(context={"volume_path": "/nonexistent/path/xyz"})


def test_list_returns_empty_for_empty_directory(volume, tmp_path):
    d = tmp_path / "empty"
    d.mkdir()
    result = volume.list(context={"volume_path": str(d)})
    assert result == []


def test_list_returns_files(volume, vol_dir):
    result = volume.list(context={"volume_path": vol_dir})
    basenames = [os.path.basename(f) for f in result]
    assert "alpha.zip" in basenames
    assert "bravo.csv" in basenames
    assert "charlie.zip" in basenames


def test_list_returns_sorted_files(volume, vol_dir):
    result = volume.list(context={"volume_path": vol_dir})
    assert result == sorted(result)


def test_list_filters_by_pattern(volume, vol_dir):
    result = volume.list(context={"volume_path": vol_dir, "file_pattern": "*.zip"})
    basenames = [os.path.basename(f) for f in result]
    assert "alpha.zip" in basenames
    assert "charlie.zip" in basenames
    assert "bravo.csv" not in basenames


def test_list_excludes_directories(volume, vol_dir):
    result = volume.list(context={"volume_path": vol_dir})
    for f in result:
        assert not f.endswith("/")


def test_list_defaults_to_wildcard_pattern(volume, vol_dir):
    result = volume.list(context={"volume_path": vol_dir})
    assert len(result) == 3


# --- get tests ---


def test_get_raises_when_no_context(volume):
    with pytest.raises(SourceException):
        volume.get(context=None)


def test_get_raises_when_context_missing_volume_path(volume):
    with pytest.raises(SourceException):
        volume.get(context={"output_url": "/tmp/out"})


def test_get_raises_when_context_missing_output_url(volume, vol_dir):
    with pytest.raises(SourceException):
        volume.get(context={"volume_path": os.path.join(vol_dir, "alpha.zip")})


def test_get_raises_when_file_does_not_exist(volume, tmp_path):
    out = str(tmp_path / "out.zip")
    with pytest.raises(SourceException):
        volume.get(context={"volume_path": "/nonexistent/file.zip", "output_url": out})


def test_get_raises_when_path_is_directory(volume, vol_dir, tmp_path):
    out = str(tmp_path / "out")
    with pytest.raises(SourceException):
        volume.get(context={"volume_path": os.path.join(vol_dir, "subdir"), "output_url": out})


def test_get_copies_file_to_output_url(volume, vol_dir, tmp_path):
    src = os.path.join(vol_dir, "alpha.zip")
    out = str(tmp_path / "downloaded.zip")
    volume.get(context={"volume_path": src, "output_url": out})
    assert os.path.exists(out)
    assert open(out).read() == "aaa"


def test_get_returns_output_url(volume, vol_dir, tmp_path):
    src = os.path.join(vol_dir, "alpha.zip")
    out = str(tmp_path / "downloaded.zip")
    result = volume.get(context={"volume_path": src, "output_url": out})
    assert result == out


# --- put tests ---


def test_put_raises_when_no_context(volume):
    with pytest.raises(SinkException):
        volume.put(context=None)


def test_put_raises_when_context_missing_source_path(volume):
    with pytest.raises(SinkException):
        volume.put(context={"target_path": "/tmp/target"})


def test_put_raises_when_context_missing_target_path(volume):
    with pytest.raises(SinkException):
        volume.put(context={"source_path": "/tmp/source"})


def test_put_raises_when_source_does_not_exist(volume, tmp_path):
    target = str(tmp_path / "target.zip")
    with pytest.raises(SinkException):
        volume.put(context={"source_path": "/nonexistent/file.zip", "target_path": target})


def test_put_raises_when_source_is_directory(volume, vol_dir, tmp_path):
    target = str(tmp_path / "target.zip")
    with pytest.raises(SinkException):
        volume.put(context={"source_path": os.path.join(vol_dir, "subdir"), "target_path": target})


def test_put_copies_file(volume, vol_dir, tmp_path):
    src = os.path.join(vol_dir, "alpha.zip")
    target = str(tmp_path / "dest" / "copied.zip")
    volume.put(context={"source_path": src, "target_path": target})
    assert os.path.exists(target)
    assert open(target).read() == "aaa"


def test_put_creates_parent_directories(volume, vol_dir, tmp_path):
    src = os.path.join(vol_dir, "alpha.zip")
    target = str(tmp_path / "deep" / "nested" / "dir" / "copied.zip")
    volume.put(context={"source_path": src, "target_path": target})
    assert os.path.exists(target)


def test_put_raises_when_target_exists_and_overwrite_false(volume, vol_dir):
    src = os.path.join(vol_dir, "alpha.zip")
    target = os.path.join(vol_dir, "bravo.csv")
    with pytest.raises(SinkException):
        volume.put(context={"source_path": src, "target_path": target, "overwrite": False})


def test_put_overwrites_when_overwrite_true(volume, vol_dir):
    src = os.path.join(vol_dir, "alpha.zip")
    target = os.path.join(vol_dir, "bravo.csv")
    volume.put(context={"source_path": src, "target_path": target, "overwrite": True})
    assert open(target).read() == "aaa"


def test_put_overwrite_defaults_to_false(volume, vol_dir):
    src = os.path.join(vol_dir, "alpha.zip")
    target = os.path.join(vol_dir, "charlie.zip")
    with pytest.raises(SinkException):
        volume.put(context={"source_path": src, "target_path": target})


# --- delete tests ---


def test_delete_raises_when_no_context(volume):
    with pytest.raises(SinkException):
        volume.delete(context=None)


def test_delete_raises_when_context_missing_volume_path(volume):
    with pytest.raises(SinkException):
        volume.delete(context={"some_key": "value"})


def test_delete_raises_when_file_does_not_exist(volume):
    with pytest.raises(SinkException):
        volume.delete(context={"volume_path": "/nonexistent/file.zip"})


def test_delete_raises_when_path_is_directory(volume, vol_dir):
    with pytest.raises(SinkException):
        volume.delete(context={"volume_path": os.path.join(vol_dir, "subdir")})


def test_delete_removes_file(volume, vol_dir):
    target = os.path.join(vol_dir, "alpha.zip")
    assert os.path.exists(target)
    volume.delete(context={"volume_path": target})
    assert not os.path.exists(target)


# --- _LocalFs edge cases ---


def test_local_fs_cp_raises_when_source_not_found():
    fs = _LocalFs()
    with pytest.raises(FileNotFoundError):
        fs.cp("/nonexistent/source.txt", "/tmp/dst.txt")


def test_local_fs_rm_raises_when_path_not_found():
    fs = _LocalFs()
    with pytest.raises(FileNotFoundError):
        fs.rm("/nonexistent/file.txt")


def test_local_fs_rm_recurse_removes_directory(tmp_path):
    d = tmp_path / "to_remove"
    d.mkdir()
    (d / "file.txt").write_text("data")
    fs = _LocalFs()
    fs.rm(str(d), recurse=True)
    assert not d.exists()


# --- constructor DBUtils branch ---


def test_init_uses_dbutils_when_use_local_fs_not_set():
    mock_spark = MagicMock()
    mock_fs = MagicMock()
    mock_dbutils_instance = MagicMock()
    mock_dbutils_instance.fs = mock_fs

    with patch("tgedr_dataops_ext.both.volume_files.UtilsSpark.get_spark_session", return_value=mock_spark), \
         patch.dict("sys.modules", {"pyspark.dbutils": MagicMock(DBUtils=MagicMock(return_value=mock_dbutils_instance))}):
        v = VolumeFiles(config={})
        assert v._fs is mock_fs


# --- __is_file exception path ---


def test_is_file_returns_false_on_exception(volume):
    """When __is_file encounters an exception from ls, it returns False.

    We trigger this via the get method by pointing to a nonexistent path.
    """
    # __path_exists returns False for nonexistent path, so get raises before __is_file.
    # To hit the __is_file exception branch, we need ls to succeed for __path_exists
    # but raise for __is_file. We mock _fs.ls to raise on the second call.
    call_count = [0]

    def ls_side_effect(path):
        call_count[0] += 1
        if call_count[0] == 1:
            return [MagicMock(path=path, name="file.txt", size=100)]
        raise OSError("simulated error")

    volume._fs.ls = ls_side_effect
    with pytest.raises(SourceException, match="path is not a file"):
        volume.get(context={"volume_path": "/some/file.txt", "output_url": "/tmp/out"})


# --- get/put/delete exception wrapping ---


def test_get_wraps_cp_exception(volume, vol_dir):
    src = os.path.join(vol_dir, "alpha.zip")
    volume._fs.cp = MagicMock(side_effect=OSError("disk error"))
    with pytest.raises(SourceException, match="failed to download file"):
        volume.get(context={"volume_path": src, "output_url": "/tmp/out"})


def test_put_wraps_cp_exception(volume, vol_dir, tmp_path):
    src = os.path.join(vol_dir, "alpha.zip")
    target = str(tmp_path / "new_dir" / "target.zip")
    volume._fs.mkdirs = MagicMock(side_effect=OSError("disk error"))
    with pytest.raises(SinkException, match="failed to copy file to"):
        volume.put(context={"source_path": src, "target_path": target})


def test_delete_wraps_rm_exception(volume, vol_dir):
    target = os.path.join(vol_dir, "alpha.zip")
    volume._fs.rm = MagicMock(side_effect=OSError("disk error"))
    with pytest.raises(SinkException, match="failed to delete file"):
        volume.delete(context={"volume_path": target})
