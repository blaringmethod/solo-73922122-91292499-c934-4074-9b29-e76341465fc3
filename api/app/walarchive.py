"""Safe WAL archive publishing, validation and repair.

Contract enforced here (and mirrored by ``db/archiver/safe_archive.sh`` used
by the primary's ``archive_command``):

* a WAL segment is *published* under its final name only after a full,
  size-verified copy (write to a temp file, then atomic rename);
* a segment already published as complete is never overwritten;
* a leftover short file (an interrupted copy) is replaced and re-archived
  while the source WAL still exists;
* readiness and recovery acceptance consider a segment usable only when it
  is present **and** has the full 16 MiB segment size — a truncated file is
  treated as missing, not as a valid archive.

The module is import-safe (no psycopg dependency) so it can also run as a
standalone CLI, including as the replica's ``restore_command``:

    python -m app.walarchive archive <src> <name>
    python -m app.walarchive restore <name> <dest>
    python -m app.walarchive check
"""

from __future__ import annotations

import logging
import os
import re
import shutil
import sys
import tempfile

from . import config

log = logging.getLogger("walarchive")

WAL_SEG_SIZE = 16 * 1024 * 1024
_TMP_SUFFIX = ".tmp"
# 24 hex digits, e.g. 000000010000000000000003; plus optional timeline history
_SEGMENT_RE = re.compile(r"^[0-9A-F]{24}$")
_HISTORY_RE = re.compile(r"^[0-9A-F]{8}\.history$")
_TMP_RE = re.compile(r"^[0-9A-F]{24}\..*\.tmp$")


class ArchiveError(RuntimeError):
    """Raised when a segment cannot be archived or restored safely."""


# --------------------------------------------------------------------------
# validation
# --------------------------------------------------------------------------

def is_wal_name(name: str) -> bool:
    return bool(_SEGMENT_RE.match(name) or _HISTORY_RE.match(name))


def archive_path(name: str) -> str:
    if not is_wal_name(name):
        # never let a caller escape the archive directory
        raise ArchiveError(f"refusing unsafe WAL file name: {name!r}")
    return os.path.join(config.WAL_ARCHIVE_DIR, name)


def segment_complete(path: str) -> bool:
    """A published segment is valid iff it is exactly one WAL segment.

    A closed WAL segment copied completely is always 16 MiB. Any other size
    means the copy was interrupted; PostgreSQL would reject the file during
    replay anyway, so the API must not treat it as an available archive.
    """
    try:
        return os.path.getsize(path) == WAL_SEG_SIZE
    except OSError:
        return False


def is_published_complete(name: str) -> bool:
    """True only when the segment exists in the archive at full size."""
    if _HISTORY_RE.match(name):
        return os.path.exists(archive_path(name))
    path = archive_path(name)
    return os.path.exists(path) and segment_complete(path)


def list_archive() -> list[dict]:
    """Inventory of the archive directory (for /archive and diagnostics)."""
    items: list[dict] = []
    if not os.path.isdir(config.WAL_ARCHIVE_DIR):
        return items
    for entry in sorted(os.listdir(config.WAL_ARCHIVE_DIR)):
        full = os.path.join(config.WAL_ARCHIVE_DIR, entry)
        if not os.path.isfile(full):
            continue
        try:
            size = os.path.getsize(full)
        except OSError:
            continue
        if _SEGMENT_RE.match(entry):
            kind, complete = "segment", size == WAL_SEG_SIZE
        elif _HISTORY_RE.match(entry):
            kind, complete = "history", True
        elif _TMP_RE.match(entry) or entry.endswith(_TMP_SUFFIX):
            kind, complete = "temp", False
        else:
            kind, complete = "other", False
        items.append(
            {"name": entry, "size": size, "kind": kind, "complete": complete}
        )
    return items


def cleanup_temp_files() -> list[str]:
    """Remove interrupted *.tmp copies left by crashed archive attempts."""
    removed = []
    for item in list_archive():
        if item["kind"] == "temp":
            path = os.path.join(config.WAL_ARCHIVE_DIR, item["name"])
            try:
                os.remove(path)
                removed.append(item["name"])
            except OSError:
                log.exception("could not remove temp archive file %s", path)
    return removed


# --------------------------------------------------------------------------
# atomic publishing (shared by the CLI and in-process repair)
# --------------------------------------------------------------------------

def _fsync_dir(path: str) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except OSError:
        # best effort: atomic rename already guarantees publication semantics
        log.debug("fsync of %s failed (ignored)", path)


def publish_copy(src: str, name: str) -> str:
    """Copy ``src`` into the archive as ``name`` atomically.

    Returns "archived" for a newly published segment or "exists" when a
    complete same-named segment is already present (it is never touched).
    Raises ArchiveError if the source itself is not a full segment or the
    copy cannot be completed.
    """
    if not _SEGMENT_RE.match(name):
        raise ArchiveError(f"refusing to publish non-segment name: {name!r}")
    os.makedirs(config.WAL_ARCHIVE_DIR, exist_ok=True)
    final = archive_path(name)

    if os.path.exists(final):
        if segment_complete(final):
            # complete archive is immutable: never overwrite it
            log.info("segment %s already archived complete; keeping existing copy", name)
            return "exists"
        log.warning(
            "replacing truncated archive %s (%d bytes) with a fresh copy",
            name, os.path.getsize(final),
        )
        os.remove(final)

    try:
        src_size = os.path.getsize(src)
    except OSError as exc:
        raise ArchiveError(f"source WAL {src} is not readable: {exc}") from exc
    if src_size != WAL_SEG_SIZE:
        raise ArchiveError(
            f"source WAL {src} has size {src_size}, expected {WAL_SEG_SIZE}; "
            "not publishing an incomplete file"
        )

    fd, tmp = tempfile.mkstemp(
        prefix=f"{name}.", suffix=_TMP_SUFFIX, dir=config.WAL_ARCHIVE_DIR
    )
    try:
        with os.fdopen(fd, "wb") as out, open(src, "rb") as inp:
            shutil.copyfileobj(inp, out, length=1024 * 1024)
            out.flush()
            os.fsync(out.fileno())
        if os.path.getsize(tmp) != WAL_SEG_SIZE:
            raise ArchiveError(f"interrupted copy of {name}; not publishing")
        os.rename(tmp, final)
        _fsync_dir(config.WAL_ARCHIVE_DIR)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
    log.info("published WAL segment %s (%d bytes)", name, WAL_SEG_SIZE)
    return "archived"


def restore_segment(name: str, dest: str) -> None:
    """restore_command implementation: refuse anything short of a full file."""
    src = archive_path(name)
    if not os.path.exists(src):
        # PostgreSQL retries, then fails recovery loudly — never hand over a
        # partial file behind its back.
        raise ArchiveError(f"archive segment {name} is missing")
    if _SEGMENT_RE.match(name) and not segment_complete(src):
        raise ArchiveError(
            f"archive segment {name} is truncated "
            f"({os.path.getsize(src)} != {WAL_SEG_SIZE} bytes); refusing to restore"
        )
    dest_dir = os.path.dirname(os.path.abspath(dest)) or "."
    os.makedirs(dest_dir, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".restore.", suffix=".tmp", dir=dest_dir)
    try:
        with os.fdopen(fd, "wb") as out, open(src, "rb") as inp:
            shutil.copyfileobj(inp, out, length=1024 * 1024)
            out.flush()
            os.fsync(out.fileno())
        os.rename(tmp, dest)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


# --------------------------------------------------------------------------
# CLI: python -m app.walarchive {archive,restore,check}
# --------------------------------------------------------------------------

def _main(argv: list[str]) -> int:
    logging.basicConfig(level=logging.INFO, format="walarchive: %(message)s")
    if len(argv) < 2:
        print("usage: python -m app.walarchive {archive <src> <name>|"
              "restore <name> <dest>|check}", file=sys.stderr)
        return 2
    cmd = argv[1]
    try:
        if cmd == "archive":
            result = publish_copy(argv[2], argv[3])
            print(result)
            return 0
        if cmd == "restore":
            restore_segment(argv[2], argv[3])
            return 0
        if cmd == "check":
            bad = [i for i in list_archive() if i["kind"] == "segment"
                   and not i["complete"]]
            for item in bad:
                print(f"TRUNCATED {item['name']} {item['size']}")
            return 1 if bad else 0
    except ArchiveError as exc:
        print(f"walarchive: {exc}", file=sys.stderr)
        return 1
    print(f"unknown command: {cmd}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(_main(sys.argv))
