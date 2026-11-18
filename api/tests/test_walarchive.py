#!/usr/bin/env python3
"""Unit + CLI tests for app.walarchive (no PostgreSQL needed).

Run: python3 tests/test_walarchive.py
"""
import os
import shutil
import subprocess
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

SEG = 16 * 1024 * 1024
NAME = "000000010000000000000003"
NAME2 = "000000010000000000000004"

# configure paths before importing app modules
tmp = tempfile.mkdtemp(prefix="waltest-")
arch = os.path.join(tmp, "archive")
srcdir = os.path.join(tmp, "source")
os.makedirs(arch)
os.makedirs(srcdir)
os.environ["WAL_ARCHIVE_DIR"] = arch

from app import config, walarchive  # noqa: E402

assert config.WAL_ARCHIVE_DIR == arch
fails = []


def check(cond, label):
    print(("PASS" if cond else "FAIL"), "-", label)
    if not cond:
        fails.append(label)


def make_src(name, size=SEG):
    p = os.path.join(srcdir, name)
    with open(p, "wb") as fh:
        fh.truncate(size)
    return p


# 1. publish a complete segment atomically
src = make_src(NAME)
r = walarchive.publish_copy(src, NAME)
check(r == "archived", "fresh segment is archived")
check(walarchive.is_published_complete(NAME), "published segment is full size")
check(not [i for i in walarchive.list_archive() if i["kind"] == "temp"],
      "no temp files left after publish")

# 2. complete file is never overwritten (idempotent retry)
with open(os.path.join(arch, NAME), "r+b") as fh:
    fh.write(b"ORIGINAL")
r = walarchive.publish_copy(src, NAME)
check(r == "exists", "retry against complete file returns exists, no overwrite")
with open(os.path.join(arch, NAME), "rb") as fh:
    check(fh.read(8) == b"ORIGINAL", "complete archive content is untouched")

# 3. truncated archive gets re-archived from still-present source WAL
short = os.path.join(arch, NAME2)
with open(short, "wb") as fh:
    fh.write(b"x" * 4096)
check(not walarchive.is_published_complete(NAME2), "short file reported incomplete")
src2 = make_src(NAME2)
r = walarchive.publish_copy(src2, NAME2)
check(r == "archived", "truncated file replaced and re-archived")
check(walarchive.is_published_complete(NAME2), "re-archived file is full size")

# 4. short source must not be published
bad_name = "000000010000000000000005"
bad_src = make_src(bad_name, size=123)
try:
    walarchive.publish_copy(bad_src, bad_name)
    check(False, "short source rejected")
except walarchive.ArchiveError:
    check(True, "short source rejected")
check(not os.path.exists(os.path.join(arch, bad_name)),
      "nothing published for short source")

# 5. path traversal names refused
for evil in ("../x", "000000010000000000000003/../../etc", "abc"):
    try:
        walarchive.archive_path(evil)
        check(False, f"unsafe name refused: {evil}")
    except walarchive.ArchiveError:
        check(True, f"unsafe name refused: {evil}")

# 6. restore CLI: complete works, truncated/missing refused
dest = os.path.join(tmp, "restore_dest")
rc = subprocess.run(
    [sys.executable, "-m", "app.walarchive", "restore", NAME, dest],
    cwd=ROOT, env={**os.environ},
).returncode
check(rc == 0 and os.path.getsize(dest) == SEG, "CLI restore of full segment ok")

os.truncate(os.path.join(arch, NAME), 100)
rc = subprocess.run(
    [sys.executable, "-m", "app.walarchive", "restore", NAME,
     os.path.join(tmp, "d2")],
    cwd=ROOT, capture_output=True, text=True,
)
check(rc != 0 and "truncated" in rc.stderr, "CLI restore refuses truncated segment")

rc = subprocess.run(
    [sys.executable, "-m", "app.walarchive", "restore",
     "000000010000000000000099", os.path.join(tmp, "d3")],
    cwd=ROOT, capture_output=True, text=True,
)
check(rc != 0 and "missing" in rc.stderr, "CLI restore refuses missing segment")

# repair back to full for check command
walarchive.publish_copy(src, NAME)
rc = subprocess.run(
    [sys.executable, "-m", "app.walarchive", "check"], cwd=ROOT,
).returncode
check(rc == 0, "check exits 0 when all segments complete")

os.truncate(os.path.join(arch, NAME), 50)
cp = subprocess.run(
    [sys.executable, "-m", "app.walarchive", "check"], cwd=ROOT,
    capture_output=True, text=True,
)
check(rc == 0 and cp.returncode == 1 and "TRUNCATED" in cp.stdout,
      "check exits 1 and names truncated segment")
walarchive.publish_copy(src, NAME)

# 7. leftover temp cleanup
with open(os.path.join(arch, NAME + ".abc.tmp"), "wb") as fh:
    fh.write(b"partial")
removed = walarchive.cleanup_temp_files()
check(removed == [NAME + ".abc.tmp"], "leftover temp file cleaned up")

# 8. shell archiver parity (safe_archive.sh)
shell = os.path.join(ROOT, "..", "db", "archiver", "safe_archive.sh")
shell = os.path.abspath(shell)
sharch = os.path.join(tmp, "shell_archive")
os.makedirs(sharch)
env = {**os.environ, "WAL_ARCHIVE_DIR": sharch}

def sh_archive(s, n):
    return subprocess.run([shell, s, n], env=env, capture_output=True, text=True)

r = sh_archive(src, NAME)
check(r.returncode == 0, "shell: fresh archive ok")
with open(os.path.join(sharch, NAME), "r+b") as fh:
    fh.write(b"KEEP")
r = sh_archive(src, NAME)
check(r.returncode == 0, "shell: retry on complete file succeeds")
with open(os.path.join(sharch, NAME), "rb") as fh:
    check(fh.read(4) == b"KEEP", "shell: complete file not overwritten")

shortf = os.path.join(sharch, NAME2)
with open(shortf, "wb") as fh:
    fh.write(b"z" * 10)
r = sh_archive(src2, NAME2)
check(r.returncode == 0 and os.path.getsize(shortf) == SEG,
      "shell: truncated file re-archived from source")

bad = os.path.join(srcdir, "000000010000000000000007")
with open(bad, "wb") as fh:
    fh.truncate(7)
r = sh_archive(bad, "000000010000000000000007")
check(r.returncode != 0 and not os.path.exists(
    os.path.join(sharch, "000000010000000000000007")),
    "shell: short source not published")

shutil.rmtree(tmp)
print()
if fails:
    print(f"{len(fails)} FAILURE(S): {fails}")
    sys.exit(1)
print("ALL WAL ARCHIVE TESTS PASSED")
