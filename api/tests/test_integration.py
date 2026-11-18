#!/usr/bin/env python3
"""Integration tests against a real PostgreSQL 16 primary.

Exercises the fixed archive/readiness/admission logic through the FastAPI
app. The replica postmaster itself cannot accept connections in this
sandbox (cross-process signals are EPERM); recovery *mechanics* are
verified separately in test_recovery_mechanics.sh.

Env requirements:
  primary on 127.0.0.1:55432, archiving into $TE/archive
"""
import os
import sys
import time

TE = "/workspace/.testenv"
os.environ.update({
    "PGHOST": "127.0.0.1",
    "PGPORT": "55432",
    "PGUSER": "postgres",
    "PGPASSWORD": "postgres",
    "PGDATABASE": "traffic",
    "WAL_ARCHIVE_DIR": f"{TE}/archive",
    "BACKUP_DIR": f"{TE}/backups/base",
    "RECOVERY_ROOT": f"{TE}/recovery_work",
    "STATE_DB": f"{TE}/state/api_test.db",
    "SOURCE_PG_WAL": f"{TE}/primary/data/pg_wal",
    "BACKUP_ON_STARTUP": "0",
    "AUTO_REPAIR": "0",
    "REPLICA_RUN_AS": "",
    "REPLICA_PORT": "55435",
    "RECOVERY_TIMEOUT_SEC": "10",
})
for p in ("/workspace/.testenv/pylibs", "/workspace/api"):
    sys.path.insert(0, p)

# fresh state db
try:
    os.remove(os.environ["STATE_DB"])
except OSError:
    pass

from fastapi.testclient import TestClient  # noqa: E402

from app import config, lsn, recovery, state, walarchive  # noqa: E402
from app.main import app  # noqa: E402

fails = []


def check(cond, label):
    print(("PASS" if cond else "FAIL"), "-", label)
    if not cond:
        fails.append(label)


state.init()
client = TestClient(app)
client.__enter__()  # run lifespan (background loops; backup disabled)

# --- seed base backup state (pg_basebackup is blocked by sandbox signals) ---
with __import__("psycopg").connect(
    host="127.0.0.1", port=55432, user="postgres", dbname="traffic",
    autocommit=True,
) as conn:
    end_lsn = conn.execute("SELECT pg_current_wal_insert_lsn()::text").fetchone()[0]
state.kv_set("backup_status", "ready")
state.kv_set("backup_end_lsn", end_lsn)
state.kv_set("backup_completed_at", "2026-11-17T00:00:00+00:00")
print(f"seeded backup_end_lsn={end_lsn}")

# --- helpers ----------------------------------------------------------------
def wait_status(name, wanted, timeout=60):
    deadline = time.time() + timeout
    while time.time() < deadline:
        rows = client.get("/restore-points").json()["items"]
        for r in rows:
            if r["name"] == name and r["status"] == wanted:
                return r
        time.sleep(1)
    rows = client.get("/restore-points").json()["items"]
    return next((r for r in rows if r["name"] == name), None)


def get_point(name):
    return next(r for r in client.get("/restore-points").json()["items"]
                if r["name"] == name)


# --- 1. health, docs, archive inventory -------------------------------------
check(client.get("/health").json()["primary_reachable"] is True, "primary reachable")
check(client.get("/docs").status_code == 200, "/docs serves Swagger UI")
spec = client.get("/openapi.json").json()
check("/archive/repair" in spec["paths"] and "/archive" in spec["paths"],
      "archive endpoints documented in OpenAPI")
inv = client.get("/archive").json()
check(inv["complete"] >= 1 and not inv["truncated"],
      f"archive inventory shows complete segments ({inv['complete']})")

# --- 2. restore point becomes ready only when full-size WAL is present ------
r = client.post("/restore-points", params={"name": "rp_one"})
check(r.status_code == 201, "restore point created")
rp = wait_status("rp_one", "ready")
check(rp is not None, "rp_one becomes ready once WAL is fully archived")
check(rp["missing_wal"] == "" and rp["truncated_wal"] == "",
      "ready point carries no WAL problems")

# --- 3. truncate a required segment: ready -> pending, recovery refused -----
needed = recovery._required_wal(end_lsn, rp["lsn"])
victim = needed[len(needed) // 2]
check(os.path.getsize(os.path.join(config.WAL_ARCHIVE_DIR, victim)) ==
      walarchive.WAL_SEG_SIZE, "victim segment is complete before fault")
r = client.post("/demo/fault/truncate-archive",
                json={"name": victim, "bytes": 4096})
check(r.status_code == 200 and r.json()["size"] == 4096, "truncation fault injected")
rp = get_point("rp_one")
check(rp["status"] == "pending", "ready point flips back to pending after truncation")
check(victim in rp["truncated_wal"].split(","),
      "truncated segment name recorded on the restore point")
r = client.post("/recoveries", json={"restore_point": "rp_one"})
check(r.status_code == 409 and "truncated" in r.json()["detail"],
      "recovery rejected with 409 naming the truncated WAL")

# --- 4. healthy old replica is NOT retired when admission rejects -----------
state.kv_set("current_recovery_id", "999")
retire_calls = []
orig_retire = recovery._retire_current_replica
recovery._retire_current_replica = lambda: retire_calls.append(1)
try:
    r = client.post("/recoveries", json={"restore_point": "rp_one"})
    check(r.status_code == 409, "rejected recovery still 409")
    check(retire_calls == [], "old replica retirement is NOT called on rejection")
    check(state.kv_get("current_recovery_id") == "999",
          "current replica pointer untouched after rejection")
finally:
    recovery._retire_current_replica = orig_retire
    state.exec("DELETE FROM kv WHERE key='current_recovery_id'")

# --- 5. repair from source pg_wal while source segment still exists ---------
r = client.post("/archive/repair")
rep = r.json()
check(victim in rep["repaired"], "repair re-archives the short segment from source")
check(os.path.getsize(os.path.join(config.WAL_ARCHIVE_DIR, victim)) ==
      walarchive.WAL_SEG_SIZE, "repaired segment is full size again")
rp = wait_status("rp_one", "ready")
check(rp is not None, "point becomes ready again after repair")

# repair must be idempotent and never overwrite complete files
with open(os.path.join(config.WAL_ARCHIVE_DIR, victim), "r+b") as fh:
    fh.write(b"MARKER")
r = client.post("/archive/repair")
check(r.json()["repaired"] == [], "second repair touches no complete segment")
with open(os.path.join(config.WAL_ARCHIVE_DIR, victim), "rb") as fh:
    check(fh.read(6) == b"MARKER", "complete archive is never overwritten by repair")
# restore the marker bytes back to a valid full file for later scenarios
with open(os.path.join(config.WAL_ARCHIVE_DIR, victim), "r+b") as fh:
    fh.seek(0)
    fh.write(b"\x00" * 6)

# --- 6. missing (deleted) segment: pending with missing_wal, 409 ------------
r = client.post("/demo/fault/remove-archive", params={"name": victim})
check(r.status_code == 200, "removal fault injected")
rp = get_point("rp_one")
check(rp["status"] == "pending" and victim in rp["missing_wal"].split(","),
      "point pending with missing segment recorded")
r = client.post("/recoveries", json={"restore_point": "rp_one"})
check(r.status_code == 409 and "missing" in r.json()["detail"],
      "recovery rejected with 409 naming the missing WAL")

# re-publish from source via the repair endpoint: a missing segment that is
# still retained in the source pg_wal is re-archivable too
r = client.post("/archive/repair")
check(victim in r.json()["repaired"],
      "repair re-publishes a missing segment still present in source pg_wal")
check(os.path.getsize(os.path.join(config.WAL_ARCHIVE_DIR, victim)) ==
      walarchive.WAL_SEG_SIZE, "re-published segment is full size")
src = os.path.join(config.SOURCE_PG_WAL, victim)
check(os.path.exists(src), "source WAL segment still retained")
rp = wait_status("rp_one", "ready")
check(rp is not None, "same restore point selectable again (retry path)")

# --- 7. unknown / in-flight / expired handling ------------------------------
r = client.post("/recoveries", json={"restore_point": "nope"})
check(r.status_code == 404, "unknown restore point -> 404")

# expired point: one at/before backup redo point
import psycopg  # noqa: E402
with psycopg.connect(host="127.0.0.1", port=55432, user="postgres",
                     dbname="traffic", autocommit=True) as conn:
    old_lsn = conn.execute("SELECT pg_create_restore_point('rp_old')::text").fetchone()[0]
    conn.execute("SELECT pg_switch_wal()")
wal_name = lsn.wal_file(lsn.parse(old_lsn))
state.exec(
    "INSERT INTO restore_points (name, lsn, wal_file, status) VALUES (?, ?, ?, 'pending')",
    ("rp_old", old_lsn, wal_name),
)
state.kv_set("backup_start_lsn", old_lsn)  # point at redo point -> expired
client.get("/restore-points")
rp_old = get_point("rp_old")
check(rp_old["status"] == "expired", "point at/before backup redo -> expired")
r = client.post("/recoveries", json={"restore_point": "rp_old"})
check(r.status_code == 409, "expired point recovery -> 409")
state.exec("DELETE FROM kv WHERE key='backup_start_lsn'")

# --- 8. duplicate restore point ----------------------------------------------
r = client.post("/restore-points", params={"name": "rp_one"})
check(r.status_code == 409, "duplicate restore point -> 409")

# --- 9. state DB migration on an old-schema database -------------------------
import sqlite3  # noqa: E402
legacy = f"{TE}/state/api_legacy.db"
try:
    os.remove(legacy)
except OSError:
    pass
with sqlite3.connect(legacy) as c:
    c.execute("CREATE TABLE restore_points (id INTEGER PRIMARY KEY, name TEXT, "
              "lsn TEXT, wal_file TEXT, status TEXT, created_at TEXT)")
    c.execute("INSERT INTO restore_points (name, lsn, wal_file, status, created_at) "
              "VALUES ('x','0/1','f','ready','now')")
old_db = config.STATE_DB
config.STATE_DB = legacy
state.init()
cols = {r[1] for r in sqlite3.connect(legacy).execute(
    "PRAGMA table_info(restore_points)")}
check({"missing_wal", "truncated_wal"} <= cols,
      "legacy state DB gains missing_wal/truncated_wal columns")
config.STATE_DB = old_db

print()
if fails:
    print(f"{len(fails)} FAILURE(S):")
    for f in fails:
        print("  -", f)
    sys.exit(1)
print("ALL INTEGRATION TESTS PASSED")
