#!/usr/bin/env bash
# Safe WAL archive_command for the primary database.
#
# Usage (archive_command): /archiver/safe_archive.sh %p %f
#
# Guarantees (kept in lockstep with api/app/walarchive.py):
#   1. a WAL segment is published under its final name only after a complete
#      16 MiB copy via a temp file + atomic rename;
#   2. an already complete segment is NEVER overwritten (idempotent success,
#      so PostgreSQL retries do not clobber a good archive);
#   3. a leftover short file from an interrupted copy is removed and the
#      segment is re-archived from the source WAL while it still exists;
#   4. a source file that is itself short is rejected, so an incomplete WAL
#      is never published as a valid archive.
#
# Exit non-zero on failure: PostgreSQL keeps the segment and retries later.
set -euo pipefail

WAL_ARCHIVE_DIR="${WAL_ARCHIVE_DIR:-/wal_archive}"
SRC="$1"
NAME="$2"
SEG_SIZE=$((16 * 1024 * 1024))

mkdir -p "$WAL_ARCHIVE_DIR"

case "$NAME" in
  *.history)
    # timeline history files are small; archive once, ignore repeats.
    FINAL="$WAL_ARCHIVE_DIR/$NAME"
    if [ -e "$FINAL" ]; then
      exit 0
    fi
    cp "$SRC" "$FINAL"
    exit 0
    ;;
  [0-9A-F]*)
    ;;
  *)
    echo "safe_archive: refusing unsafe WAL name: $NAME" >&2
    exit 1
    ;;
esac

FINAL="$WAL_ARCHIVE_DIR/$NAME"

if [ -e "$FINAL" ]; then
  CUR_SIZE=$(stat -c%s "$FINAL")
  if [ "$CUR_SIZE" -eq "$SEG_SIZE" ]; then
    # complete archive is immutable: treat the retry as success
    exit 0
  fi
  echo "safe_archive: removing truncated archive $NAME ($CUR_SIZE bytes) for re-archive" >&2
  rm -f "$FINAL"
fi

SRC_SIZE=$(stat -c%s "$SRC")
if [ "$SRC_SIZE" -ne "$SEG_SIZE" ]; then
  echo "safe_archive: source $NAME is only $SRC_SIZE bytes (expected $SEG_SIZE); not publishing" >&2
  exit 1
fi

TMP=$(mktemp "$WAL_ARCHIVE_DIR/$NAME.XXXXXX.tmp")
trap 'rm -f "$TMP"' EXIT
cp "$SRC" "$TMP"

NEW_SIZE=$(stat -c%s "$TMP")
if [ "$NEW_SIZE" -ne "$SEG_SIZE" ]; then
  echo "safe_archive: copy of $NAME interrupted at $NEW_SIZE bytes; not publishing" >&2
  exit 1
fi

mv -f "$TMP" "$FINAL"
trap - EXIT
exit 0
