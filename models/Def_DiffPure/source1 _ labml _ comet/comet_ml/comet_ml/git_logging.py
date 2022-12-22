# -*- coding: utf-8 -*-
# *******************************************************
#   ____                     _               _
#  / ___|___  _ __ ___   ___| |_   _ __ ___ | |
# | |   / _ \| '_ ` _ \ / _ \ __| | '_ ` _ \| |
# | |__| (_) | | | | | |  __/ |_ _| | | | | | |
#  \____\___/|_| |_| |_|\___|\__(_)_| |_| |_|_|
#
#  Sign up for free at http://www.comet.ml
#  Copyright (C) 2015-2021 Comet ML INC
#  This file can not be copied and/or distributed without the express
#  permission of Comet ML Inc.
# *******************************************************

"""
This module contains git related functions

"""

import logging
import os
import os.path
import sys
from io import BytesIO

import dulwich.errors
import dulwich.objects
import dulwich.patch
import dulwich.porcelain
import dulwich.repo
from six.moves.urllib.parse import urlparse

from ._typing import Optional

LOGGER = logging.getLogger(__name__)


def _patched_path_to_tree_path(repopath, path):
    """Convert a path to a path usable in e.g. an index.
    :param repopath: Repository
    :param path: A path
    :return: A path formatted for use in e.g. an index
    """
    if os.path.isabs(path):
        path = os.path.relpath(path, repopath)
    if os.path.sep != "/":
        path = path.replace(os.path.sep, "/")
    return path.encode(sys.getfilesystemencoding())


def to_utf8(str_or_bytes):
    if hasattr(str_or_bytes, "decode"):
        return str_or_bytes.decode("utf-8", errors="replace")

    return str_or_bytes


def get_user(repo):
    """Retrieve the configured user from a dulwich git repository"""
    try:
        # The user name might not be valid UTF-8
        return to_utf8(repo.get_config_stack().get("user", "name"))

    except KeyError:
        LOGGER.debug("No Git User configured")
        return None
    except Exception:
        LOGGER.debug("Unexpected error retrieving git user", exc_info=True)
        return None


def get_root(repo):
    """Retrieve the hash of the repo root to uniquely identify the git
    repository
    """

    # Check if the repository is empty
    if len(repo.get_refs()) == 0:
        LOGGER.debug("Repository is empty, cannot get root")
        return None

    # Get walker needs at least the HEAD ref to be present
    walker = repo.get_walker()

    entry = None

    # Iterate on the lazy iterator to get to the last one
    for entry in walker:
        pass

    assert entry is not None

    # SHA should always be valid utf-8
    return to_utf8(entry.commit.id)


def get_branch(repo):
    """Retrieve the current branch of a dulwich repository"""
    refnames, sha = repo.refs.follow(b"HEAD")

    if len(refnames) != 2:
        LOGGER.debug("Got more than two refnames for HEAD!")

    for ref in refnames:
        if ref != b"HEAD":
            return to_utf8(ref)
    else:
        # This shouldn't happens, a repository without a HEAD reference is not recognized by Git
        # itself as a repository
        LOGGER.debug("Didn't find the HEAD ref")


def get_git_commit(repo):
    try:
        # SHA should always be valid utf-8
        return to_utf8(repo.head())

    except KeyError:
        LOGGER.debug("Failed to get current git commit", exc_info=True)
        return None


def git_status(repo):
    # Monkey-patch a dulwich method, see
    # https://github.com/dulwich/dulwich/pull/601 for an explanation why
    original = dulwich.porcelain.path_to_tree_path
    try:
        dulwich.porcelain.path_to_tree_path = _patched_path_to_tree_path

        status = dulwich.porcelain.status(repo)

        staged = {
            key: [to_utf8(path) for path in items]
            for (key, items) in status.staged.items()
        }
        unstaged = [to_utf8(path) for path in status.unstaged]
        untracked = [to_utf8(path) for path in status.untracked]

        return {"staged": staged, "unstaged": unstaged, "untracked": untracked}

    finally:
        dulwich.porcelain.path_to_tree_path = original


def get_origin_url(repo):
    repo_config = repo.get_config()
    try:
        # The origin url might not be valid UTF-8
        return to_utf8(repo_config.get((b"remote", b"origin"), "url"))

    except KeyError:
        LOGGER.debug("Failed to get GIT origin url", exc_info=True)
        return None


def get_repo_name(origin_url):
    if origin_url is None:
        return None

    # First parse the url to get rid of possible HTTP comments or arguments
    parsed_url = urlparse(origin_url)
    # Remove potential leading /
    path = parsed_url.path.rstrip("/")
    repo_name = path.split("/")[-1]

    # Remove potential leading .git
    if repo_name.endswith(".git"):
        repo_name = repo_name[:-4]
    return repo_name


class OverrideObjectStore(object):
    def __init__(self, repo_object_store):
        self.repo_object_store = repo_object_store
        self.override = {}

    def __getitem__(self, sha):
        blob = self.override.get(sha)

        if blob is None:
            blob = self.repo_object_store[sha]

        return blob

    def __setitem__(self, sha, blob):
        self.override[sha] = blob


def get_git_patch(repo, unstaged=True):
    final_changes = {}
    store = OverrideObjectStore(repo.object_store)

    try:
        head = repo.head()
    except KeyError:
        return None

    tree_id = repo[head].tree
    tree = repo[tree_id]

    index = repo.open_index()

    normalizer = repo.get_blob_normalizer()
    filter_callback = normalizer.checkin_normalize
    object_store = repo.object_store
    blob_from_path_and_stat = dulwich.index.blob_from_path_and_stat
    cleanup_mode = dulwich.index.cleanup_mode
    lookup_path = tree.lookup_path
    repo_path = repo.path.encode(sys.getfilesystemencoding())

    def lookup_entry(path):
        absolute_path = os.path.join(repo_path, path)
        if os.path.isfile(absolute_path):
            st = os.lstat(absolute_path)
            # TODO: Building the blob means that we need to load the whole
            # file content in memory. We should be able to compute the sha
            # without needed to load the whole blob in memory.
            blob = blob_from_path_and_stat(absolute_path, st)
            blob = filter_callback(blob, path)
            blob_id = blob.id

            mode = cleanup_mode(st.st_mode)

            # Check if on-disk blob differs from the one in tree
            try:
                tree_blob = lookup_path(object_store.__getitem__, path)
            except KeyError:
                # Lookup path will fails for added files
                store[blob_id] = blob
            else:
                # If the blob for path in index differs from the one on disk,
                # store the on-disk one
                if tree_blob[1] != blob_id:
                    store[blob_id] = blob

            return blob_id, mode
        elif os.path.isdir(absolute_path):
            try:
                tree_blob = lookup_path(object_store.__getitem__, path)
            except KeyError:
                # If the submodule is not in the store, it must be in index
                # and should be added
                index_entry = index[path]
                return index_entry.sha, index_entry.mode

            tree_mode = tree_blob[0]

            if dulwich.objects.S_ISGITLINK(tree_mode):
                return tree_blob[1], tree_mode
            else:
                # We shouldn't be here?
                raise KeyError(path)
        else:
            # Indicate that the files has been removed
            raise KeyError(path)

    # Merges names from the index and from the store as some files can be only
    # on index or only on the store
    names = set()
    for (name, _, _) in object_store.iter_tree_contents(tree_id):
        names.add(name)

    names.update(index._byname.keys())

    final_changes = dulwich.index.changes_from_tree(
        names, lookup_entry, repo.object_store, tree_id, want_unchanged=False
    )

    # Generate the diff

    diff = BytesIO()

    def key(x):
        # Generate a comparable sorting key
        paths = tuple(p for p in x[0] if p)
        return paths

    for (oldpath, newpath), (oldmode, newmode), (oldsha, newsha) in sorted(
        final_changes, key=key
    ):
        dulwich.patch.write_object_diff(
            diff, store, (oldpath, oldmode, oldsha), (newpath, newmode, newsha)
        )

    diff.seek(0)
    diff_result = diff.getvalue()

    # Detect empty diff
    if not diff_result:
        return None

    return diff_result


def find_git_repo(repo_path):
    # type: (str) -> Optional[dulwich.repo.Repo]
    # Early-exit if repo_path is repo root
    try:
        return dulwich.repo.Repo(repo_path)

    except dulwich.errors.NotGitRepository:
        pass

    path = repo_path
    while path:
        parent_path = os.path.dirname(path)
        # Avoid infinite loop
        if parent_path == path:
            break

        path = parent_path
        try:
            return dulwich.repo.Repo(path)

        except dulwich.errors.NotGitRepository:
            pass


def get_git_metadata(repo):
    origin = get_origin_url(repo)
    repo_name = get_repo_name(origin)

    data = {
        "user": get_user(repo),
        "root": get_root(repo),
        "branch": get_branch(repo),
        "parent": get_git_commit(repo),
        "status": None,
        "origin": origin,
        "repo_name": repo_name,
    }

    return data
