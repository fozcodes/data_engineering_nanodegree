import glob
import os
from typing import AnyStr, Iterator

from pydash import flat_map, map_


def join_json_paths_and_filenames(
    walk_iterator: Iterator[tuple[AnyStr, list[AnyStr], list[AnyStr]]]
):
    """
    Takes an Iterator (usually a call to os.walk) and returns a list of relative
    filepaths for all files found.
    """
    root, _, _ = walk_iterator
    return glob.glob(os.path.join(root, "*.json"))


def absolute_path(filepath):
    """
    Takes a list of filepaths and makes them all absolute.
    """
    return os.path.abspath(filepath)


def get_all_json_files_in_path(filepath):
    """Get all JSON files in a a given filepath

    :param filepath: str - path to walk for possible files
    """
    files_with_paths = flat_map(os.walk(filepath), join_json_paths_and_filenames)
    with_absolute_paths = map_(files_with_paths, absolute_path)
    return with_absolute_paths
