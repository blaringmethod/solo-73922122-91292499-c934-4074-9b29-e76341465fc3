import os
import sys

# --- primary (source) database ---------------------------------------------
PGHOST = os.environ.get("PGHOST", "db")
PGPORT = os.environ.get("PGPORT", "5432")
PGUSER = os.environ.get("PGUSER", "postgres")
PGPASSWORD = os.environ.get("PGPASSWORD", "postgres")
PGDATABASE = os.environ.get("PGDATABASE", "traffic")

PRIMARY_DSN = (
    f"host={PGHOST} port={PGPORT} dbname={PGDATABASE} "
    f"user={PGUSER} password={PGPASSWORD}"
)
# Physical-replication connections must not carry a dbname.
BACKUP_DSN = f"host={PGHOST} port={PGPORT} user={PGUSER} password={PGPASSWORD}"

# --- recovered replica (spawned by this service) ----------------------------
REPLICA_HOST = os.environ.get("REPLICA_HOST", "127.0.0.1")
REPLICA_PORT = int(os.environ.get("REPLICA_PORT", "55433"))
REPLICA_DSN = (
    f"host={REPLICA_HOST} port={REPLICA_PORT} dbname={PGDATABASE} "
    f"user={PGUSER} password={PGPASSWORD} "
    f"options='-c default_transaction_read_only=on'"
)

# --- storage -----------------------------------------------------------------
WAL_ARCHIVE_DIR = os.environ.get("WAL_ARCHIVE_DIR", "/wal_archive")
BACKUP_DIR = os.environ.get("BACKUP_DIR", "/backups/base")
RECOVERY_ROOT = os.environ.get("RECOVERY_ROOT", "/recovery_work")
STATE_DB = os.environ.get("STATE_DB", "/state/api.db")
# Read-only view of the source cluster's pg_wal directory. Used to re-archive
# leftover truncated/missing segments while the source WAL file is still
# retained (wal_keep_size). The compose stack mounts the primary's data dir
# read-only at /source_pgdata; an empty/missing path disables out-of-band
# repair, leaving the primary's own archive_command retries to do the work.
SOURCE_PG_WAL = os.environ.get("SOURCE_PG_WAL", "/source_pgdata/pg_wal")
# Background sweep that re-archives short/missing segments from SOURCE_PG_WAL.
# The primary's archive_command already retries pending segments itself, and
# POST /archive/repair heals externally caused damage on demand; the sweep is
# therefore opt-in ("1") so demo fault windows stay deterministic.
AUTO_REPAIR = os.environ.get("AUTO_REPAIR", "0") == "1"
# Interpreter used for the replica restore_command helper. sys.executable
# keeps it working both in the container venv and bare-metal test runs.
PYTHON_BIN = os.environ.get("PYTHON_BIN") or os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "bin", "python")
)
# Fallback when the venv-relative path does not exist (e.g. system python).
if not os.path.exists(PYTHON_BIN):
    PYTHON_BIN = sys.executable

# --- recovery behaviour ------------------------------------------------------
RECOVERY_TIMEOUT_SEC = int(os.environ.get("RECOVERY_TIMEOUT_SEC", "180"))
# Take the physical base backup at startup (default). Set to "0" only when a
# pre-staged backup is provided out of band (e.g. tests); recoveries are then
# gated on whatever /backup reports.
BACKUP_ON_STARTUP = os.environ.get("BACKUP_ON_STARTUP", "1")
# The replica postmaster cannot run as root. Inside the docker image the api
# runs as root and drops privileges via `gosu <REPLICA_RUN_AS>`; set to "" to
# run the replica as the current user (local development).
REPLICA_RUN_AS = os.environ.get("REPLICA_RUN_AS", "postgres")

TRAFFIC_TABLE = "traffic_minute_counts"
