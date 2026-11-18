"""Point-in-time recovery executor.

Every recovery:
  1. retires (stops and deletes) any previously served replica, so data from
     different restore points can never be mixed or served stale;
  2. rebuilds a fresh, isolated data directory from the physical base backup;
  3. replays archived WAL up to the named restore point
     (recovery_target_action=pause keeps the replica a read-only standby);
  4. only reports success after the target LSN has actually been replayed AND
     a real query against the traffic table succeeds.

Missing WAL or a dying recovery process => the job is marked failed, the
broken directory is removed, and nothing (not even the previous replica) is
served. The caller may simply retry.
"""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
import threading
import time

from . import config, db, lsn, state, walarchive

log = logging.getLogger("recovery")

_start_lock = threading.Lock()


class Conflict(Exception):
    """Maps to HTTP 409."""


class NotFound(Exception):
    """Maps to HTTP 404."""


# --------------------------------------------------------------------------
# subprocess helpers
# --------------------------------------------------------------------------

def _run(cmd, **kw):
    log.info("exec: %s", " ".join(cmd))
    return subprocess.run(cmd, capture_output=True, text=True, **kw)


def _as_replica_user(cmd):
    if config.REPLICA_RUN_AS:
        return ["gosu", config.REPLICA_RUN_AS] + cmd
    return cmd


def _pg_ctl(args, timeout=120):
    return _run(_as_replica_user(["pg_ctl"] + args), timeout=timeout)


def _tail(path, n=4000):
    try:
        with open(path, "r", errors="replace") as fh:
            return fh.read()[-n:]
    except OSError:
        return ""


# --------------------------------------------------------------------------
# replica process helpers
# --------------------------------------------------------------------------

def _data_dir(work_dir):
    return os.path.join(work_dir, "data")


def _replica_running(work_dir):
    data = _data_dir(work_dir)
    if not os.path.isdir(data):
        return False
    try:
        return _pg_ctl(["-D", data, "status"], timeout=20).returncode == 0
    except Exception:
        return False


def _stop_replica(work_dir):
    data = _data_dir(work_dir)
    if os.path.isdir(data):
        try:
            _pg_ctl(["-D", data, "-m", "fast", "-w", "-t", "60", "stop"], timeout=90)
        except Exception:
            log.exception("failed to stop replica at %s", work_dir)
    shutil.rmtree(work_dir, ignore_errors=True)


# --------------------------------------------------------------------------
# "currently served replica" pointer
# --------------------------------------------------------------------------

def current():
    row = state.one("SELECT value FROM kv WHERE key = 'current_recovery_id'")
    if not row:
        return None
    return state.one(
        "SELECT * FROM recoveries WHERE id = ? AND status = 'success'", (row["value"],)
    )


def _fail_current(message):
    row = state.one("SELECT value FROM kv WHERE key = 'current_recovery_id'")
    if not row:
        return
    rec_id = row["value"]
    state.exec(
        "UPDATE recoveries SET status='failed', error=?, finished_at=datetime('now') "
        "WHERE id = ? AND status = 'success'",
        (message, rec_id),
    )
    state.exec("DELETE FROM kv WHERE key = 'current_recovery_id'")
    rec = state.one("SELECT work_dir FROM recoveries WHERE id = ?", (rec_id,))
    if rec and rec["work_dir"]:
        _stop_replica(rec["work_dir"])


def require_current():
    rec = current()
    if not rec:
        raise Conflict("no recovered replica is currently served; start a recovery first")
    if not _replica_running(rec["work_dir"]):
        _fail_current("replica postgres is not running anymore")
        raise Conflict("the recovered replica exited; its recovery has been marked failed")
    return rec


def monitor_current():
    """Background watchdog: a replica that exits must become a failure."""
    rec = current()
    if rec and not _replica_running(rec["work_dir"]):
        log.error("replica for recovery %s exited unexpectedly", rec["id"])
        _fail_current("replica postgres exited unexpectedly")


# --------------------------------------------------------------------------
# restore point readiness (WAL archive completeness)
# --------------------------------------------------------------------------

def _required_wal(backup_end_lsn: str, point_lsn: str) -> list[str]:
    """WAL segments that must be fully archived to reach a restore point."""
    return lsn.wal_files_between(lsn.parse(backup_end_lsn), lsn.parse(point_lsn))


def _classify_archive(names: list[str]) -> tuple[list[str], list[str]]:
    """Split required segment names into (missing, truncated).

    A segment counts as usable only when it exists *and* has the full
    16 MiB size. A truncated file is a broken archive, not a valid one.
    """
    missing, truncated = [], []
    for name in names:
        full = os.path.join(config.WAL_ARCHIVE_DIR, name)
        if not os.path.exists(full):
            missing.append(name)
        elif not walarchive.is_published_complete(name):
            truncated.append(name)
    return missing, truncated


def repair_archive_from_source() -> dict:
    """Re-publish broken/missing WAL while the source segment still exists.

    Two repair paths, both safe (a complete archive is never overwritten):
      * leftover truncated/temp segments in the archive directory;
      * segments required by a restore point that are absent from the
        archive but still retained in the source cluster's pg_wal.
    The primary's archive_command normally retries this itself; this is the
    API-side repair for cases where a segment is stuck or was lost out of
    band. Returns a small report; safe to call repeatedly.
    """
    report = {"repaired": [], "still_truncated": [], "source_gone": [],
              "temp_removed": walarchive.cleanup_temp_files()}
    source_dir = config.SOURCE_PG_WAL

    def republish(name: str) -> None:
        src = os.path.join(source_dir, name)
        if not os.path.exists(src):
            if name not in report["source_gone"]:
                report["source_gone"].append(name)
            if name not in report["still_truncated"]:
                report["still_truncated"].append(name)
            return
        try:
            walarchive.publish_copy(src, name)
            report["repaired"].append(name)
            log.info("re-archived segment %s from source pg_wal", name)
        except walarchive.ArchiveError as exc:
            log.warning("could not re-archive %s: %s", name, exc)
            report["still_truncated"].append(name)

    # 1. truncated segments sitting in the archive directory
    for item in walarchive.list_archive():
        if item["kind"] == "segment" and not item["complete"]:
            republish(item["name"])

    # 2. segments required by restore points but absent from the archive
    backup_end = state.kv_get("backup_end_lsn")
    if backup_end:
        wanted = set()
        for point in state.q("SELECT * FROM restore_points"):
            if point["status"] == "expired":
                continue
            try:
                wanted.update(_required_wal(backup_end, point["lsn"]))
            except ValueError:
                continue
        for name in sorted(wanted):
            if not walarchive.is_published_complete(name):
                republish(name)
    return report


def refresh_restore_points():
    """Recompute readiness of every restore point from the live archive.

    Selectability is never sticky: a point marked ready moves back to
    pending as soon as one of its required segments disappears or is
    truncated, and it carries the offending segment names with it.
    """
    backup_end = state.kv_get("backup_end_lsn")
    if not backup_end:
        return
    backup_start = state.kv_get("backup_start_lsn")
    for point in state.q("SELECT * FROM restore_points"):
        if point["status"] == "expired":
            # an expired point can never become usable again for this backup
            continue
        # a point at/before the backup's own redo point can never be
        # recovered from this backup (it already contains newer state)
        if backup_start and lsn.parse(point["lsn"]) <= lsn.parse(backup_start):
            state.exec(
                "UPDATE restore_points SET status='expired', "
                "missing_wal='', truncated_wal='' WHERE id=?",
                (point["id"],),
            )
            log.info("restore point %s predates the base backup -> expired", point["name"])
            continue
        try:
            needed = _required_wal(backup_end, point["lsn"])
        except ValueError:
            continue
        missing, truncated = _classify_archive(needed)
        if missing or truncated:
            if point["status"] == "ready":
                log.warning(
                    "restore point %s was ready but its archive is now broken "
                    "(missing=%d truncated=%d) -> pending",
                    point["name"], len(missing), len(truncated),
                )
            else:
                log.info(
                    "restore point %s: waiting (missing=%d truncated=%d)",
                    point["name"], len(missing), len(truncated),
                )
            state.exec(
                "UPDATE restore_points SET status='pending', missing_wal=?, "
                "truncated_wal=? WHERE id=?",
                (",".join(missing), ",".join(truncated), point["id"]),
            )
        else:
            if point["status"] != "ready":
                log.info("restore point %s is ready (WAL archive complete)", point["name"])
            state.exec(
                "UPDATE restore_points SET status='ready', "
                "missing_wal='', truncated_wal='' WHERE id=?",
                (point["id"],),
            )


# --------------------------------------------------------------------------
# recovery lifecycle
# --------------------------------------------------------------------------

def _retire_current_replica():
    """Stop serving any previous replica and delete its data directory."""
    state.exec("DELETE FROM kv WHERE key = 'current_recovery_id'")
    for rec in state.q("SELECT * FROM recoveries WHERE status = 'success'"):
        state.exec(
            "UPDATE recoveries SET status='superseded', "
            "finished_at=COALESCE(finished_at, datetime('now')) WHERE id=?",
            (rec["id"],),
        )
        if rec["work_dir"]:
            _stop_replica(rec["work_dir"])


def _acceptance_check(point) -> None:
    """Final gate at recovery submission.

    A restore point that was marked ready earlier must still have every
    required segment present *and* full-sized now. Raises Conflict (HTTP
    409) otherwise; this runs before any healthy replica is retired.
    """
    backup_end = state.kv_get("backup_end_lsn")
    if not backup_end:
        raise Conflict("the startup base backup is not ready yet")
    needed = _required_wal(backup_end, point["lsn"])
    missing, truncated = _classify_archive(needed)
    problems = []
    if missing:
        problems.append(f"missing archived WAL: {', '.join(missing[:5])}"
                        + (" ..." if len(missing) > 5 else ""))
    if truncated:
        problems.append(f"truncated archived WAL: {', '.join(truncated[:5])}"
                        + (" ..." if len(truncated) > 5 else ""))
    if problems:
        # keep the stored point status consistent with the archive
        refresh_restore_points()
        raise Conflict(
            f"restore point {point['name']!r} cannot be recovered: "
            + "; ".join(problems)
            + ". Repair the archive (POST /archive/repair) and retry the "
              "same restore point."
        )


def start_recovery(point_name):
    with _start_lock:
        if state.one("SELECT id FROM recoveries WHERE status = 'running'"):
            raise Conflict("another recovery is already in flight")
        point = state.one("SELECT * FROM restore_points WHERE name=?", (point_name,))
        if not point:
            raise NotFound(f"restore point {point_name!r} does not exist")
        if state.kv_get("backup_status") != "ready":
            raise Conflict("the startup base backup is not ready yet")
        if point["status"] == "expired":
            raise Conflict(
                f"restore point {point_name!r} predates the base backup "
                "and cannot be recovered from it (status=expired)"
            )

        # Re-validate against the live archive BEFORE touching the current
        # replica: a ready point whose WAL later went missing/short must be
        # rejected while the healthy old replica keeps serving.
        refresh_restore_points()
        point = state.one("SELECT * FROM restore_points WHERE name=?", (point_name,))
        # The acceptance check is authoritative: it names exactly which
        # segments are missing or truncated.
        _acceptance_check(point)
        if point["status"] != "ready":
            raise Conflict(
                f"restore point {point_name!r} is not selectable: "
                f"its WAL archive is incomplete (status={point['status']})"
            )

        _retire_current_replica()

        rec_id = state.exec(
            "INSERT INTO recoveries (restore_point, target_lsn) VALUES (?, ?)",
            (point_name, point["lsn"]),
        )
        work_dir = os.path.join(config.RECOVERY_ROOT, f"r{rec_id}")
        state.exec("UPDATE recoveries SET work_dir=? WHERE id=?", (work_dir, rec_id))
        threading.Thread(
            target=_run_recovery, args=(rec_id,), daemon=True, name=f"recovery-{rec_id}"
        ).start()
        return state.one("SELECT * FROM recoveries WHERE id=?", (rec_id,))


def _run_recovery(rec_id):
    rec = state.one("SELECT * FROM recoveries WHERE id=?", (rec_id,))
    work_dir = rec["work_dir"]
    data = _data_dir(work_dir)
    log_file = os.path.join(work_dir, "replica.log")
    try:
        # 1. fresh isolated copy of the physical base backup
        os.makedirs(data)
        result = _run(["cp", "-a", config.BACKUP_DIR + "/.", data], timeout=900)
        if result.returncode != 0:
            raise RuntimeError(f"copying base backup failed: {result.stderr[-1000:]}")
        os.chmod(data, 0o700)

        # 2. PITR configuration: replay archived WAL up to the named point.
        # restore_command goes through the same size-gated helper: a segment
        # that gets truncated after admission is refused instead of feeding
        # a partial file to replay (PostgreSQL then fails recovery loudly).
        restore_cmd = (
            f"{config.PYTHON_BIN} -m app.walarchive restore %f %p"
        )
        with open(os.path.join(data, "postgresql.auto.conf"), "a") as fh:
            fh.write(
                "\n# ---- recovery-api ----\n"
                f"port = {config.REPLICA_PORT}\n"
                f"listen_addresses = '{config.REPLICA_HOST}'\n"
                "unix_socket_directories = '/tmp'\n"
                "archive_mode = off\n"
                "hot_standby = on\n"
                f"restore_command = '{restore_cmd}'\n"
                f"recovery_target_name = '{rec['restore_point']}'\n"
                "recovery_target_action = 'pause'\n"
            )
        open(os.path.join(data, "recovery.signal"), "w").close()
        if config.REPLICA_RUN_AS:
            user = config.REPLICA_RUN_AS
            _run(["chown", "-R", f"{user}:{user}", work_dir])

        # 3. start the isolated replica
        result = _pg_ctl(["-D", data, "-l", log_file, "-w", "-t", "90", "start"], timeout=120)
        if result.returncode != 0:
            raise RuntimeError(
                "replica postgres failed to start (missing WAL?): "
                + (_tail(log_file, 1500) or (result.stdout + result.stderr)[-500:])
            )

        # 4. wait until the target is really replayed and the data queryable
        target = lsn.parse(rec["target_lsn"])
        deadline = time.time() + config.RECOVERY_TIMEOUT_SEC
        while time.time() < deadline:
            if not _replica_running(work_dir):
                raise RuntimeError(
                    "replica postgres exited before reaching the recovery target: "
                    + _tail(log_file, 1500)
                )
            replay = None
            try:
                with db.replica() as conn:
                    row = conn.execute(
                        "SELECT pg_last_wal_replay_lsn()::text AS replay"
                    ).fetchone()
                    replay = row["replay"] if row else None
                    if replay and lsn.parse(replay) >= target:
                        # prove the replica is actually queryable before success
                        conn.execute(
                            f"SELECT count(*) FROM {config.TRAFFIC_TABLE}"
                        ).fetchone()
            except Exception:
                replay = None
            if replay and lsn.parse(replay) >= target:
                state.exec(
                    "UPDATE recoveries SET status='success', replay_lsn=?, "
                    "finished_at=datetime('now') WHERE id=?",
                    (replay, rec_id),
                )
                state.kv_set("current_recovery_id", str(rec_id))
                log.info(
                    "recovery %s reached target %s (replay %s)",
                    rec_id, rec["target_lsn"], replay,
                )
                return
            time.sleep(1)
        raise RuntimeError(
            f"timed out after {config.RECOVERY_TIMEOUT_SEC}s waiting to reach "
            f"recovery target {rec['target_lsn']}: " + _tail(log_file, 1500)
        )
    except Exception as exc:  # noqa: BLE001 - recorded on the job
        message = str(exc)[:3000]
        log.error("recovery %s failed: %s", rec_id, message)
        state.exec(
            "UPDATE recoveries SET status='failed', error=?, finished_at=datetime('now') "
            "WHERE id=?",
            (message, rec_id),
        )
        _stop_replica(work_dir)


def cleanup_on_startup():
    """Never serve leftovers from a previous api lifetime."""
    for rec in state.q("SELECT * FROM recoveries WHERE status IN ('running','success')"):
        state.exec(
            "UPDATE recoveries SET status='failed', "
            "error=COALESCE(error, 'interrupted by api restart'), "
            "finished_at=datetime('now') WHERE id=?",
            (rec["id"],),
        )
    state.exec("DELETE FROM kv WHERE key = 'current_recovery_id'")
    if os.path.isdir(config.RECOVERY_ROOT):
        for entry in os.listdir(config.RECOVERY_ROOT):
            _stop_replica(os.path.join(config.RECOVERY_ROOT, entry))
    # interrupted archive copies from a previous lifetime must not linger
    removed = walarchive.cleanup_temp_files()
    if removed:
        log.info("removed leftover temp archive files: %s", ", ".join(removed))
    # readiness is recomputed on the first refresh; truncated archives stay
    # visible there until the primary's retry (or /archive/repair) fixes them
    refresh_restore_points()
