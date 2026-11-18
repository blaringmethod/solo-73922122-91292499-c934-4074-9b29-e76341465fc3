#!/usr/bin/env bash
# Physical recovery mechanics test (no network connections needed: works even
# when the kernel blocks cross-process signals, by verifying via single-user
# mode on a postmaster.pid-free copy).
set -uo pipefail

TE=/workspace/.testenv
PGBIN=$TE/pg/usr/lib/postgresql/16/bin
export LD_LIBRARY_PATH=$TE/pg/usr/lib/x86_64-linux-gnu
export PYTHONPATH=/workspace/api
export WAL_ARCHIVE_DIR=$TE/archive
PY=/usr/bin/python3
fails=0
ok(){ echo "PASS - $1"; }
no(){ echo "FAIL - $1"; fails=$((fails+1)); }

prep_copy(){
  local dest=$1
  rm -rf "$dest"
  mkdir -p "$dest/data" "$dest/sock"
  cp -a "$TE/backups/base/." "$dest/data"
  rm -f "$dest/data/postmaster.pid"
  cat >> "$dest/data/postgresql.auto.conf" <<EOF
# no TCP needed for the mechanics check (and avoids port clashes with
# sandbox postmasters that cannot be signalled to stop); give every copy
# its own unix socket directory so copied-in socket locks never collide.
listen_addresses = ''
unix_socket_directories = '$dest/sock'
archive_mode = off
hot_standby = on
restore_command = '$PY -m app.walarchive restore %f %p'
recovery_target_name = 'before_delete'
recovery_target_action = pause
EOF
  touch "$dest/data/recovery.signal"
}

wait_log(){ # dir pattern timeout
  local d=$1 pat=$2 t=${3:-60}
  for _ in $(seq 1 $t); do
    grep -q "$pat" "$d/replica.log" 2>/dev/null && return 0
    sleep 1
  done
  return 1
}

single_query(){ # datadir sql -> scalar value from  col = "value"  output
  echo "$2" | "$PGBIN/postgres" --single -D "$1" traffic 2>/dev/null \
    | sed -n 's/.*= "\(.*\)".*/\1/p' | head -1
}

echo "== scenario A: full archive -> recovery pauses at restore point =="
A=$TE/recovery_work/mechA
prep_copy "$A"
"$PGBIN/pg_ctl" -D "$A/data" -l "$A/replica.log" start >/dev/null 2>&1 || true
if wait_log "$A" "pausing at the end of recovery" 60; then
  ok "recovery reached restore point and paused (python restore_command)"
else
  no "recovery did not reach target"; tail -5 "$A/replica.log"
fi
grep -q "recovery stopping at restore point \"before_delete\"" "$A/replica.log" \
  && ok "log names the before_delete restore point" || no "restore point stop not logged"

# verify data on a pid-free copy via single-user mode
AC=$TE/recovery_work/mechA_check
rm -rf "$AC"; cp -a "$A/data" "$AC"; rm -f "$AC/postmaster.pid"
total=$(single_query "$AC" "SELECT count(*) FROM traffic_minute_counts;" | tr -dc '0-9')
[ "$total" = "60" ] && ok "replica holds exactly the 60 pre-delete rows (got $total)" \
  || no "replica row count wrong: $total"
ixb=$(single_query "$AC" "SELECT count(*) FROM traffic_minute_counts WHERE intersection_id LIKE 'IXB-%';" | tr -dc '0-9')
[ "$ixb" = "0" ] && ok "replica contains zero post-point IXB rows" \
  || no "replica leaked post-point rows: $ixb"
ix=$(single_query "$AC" "SELECT count(DISTINCT intersection_id) FROM traffic_minute_counts WHERE intersection_id LIKE 'IX-%';" | tr -dc '0-9')
[ "$ix" = "3" ] && ok "all 3 deleted intersections recovered" || no "intersection count: $ix"

echo "== scenario B: truncated required WAL -> restore_command refuses, FATAL =="
# truncate a segment the recovery needs (segment holding target LSN: 0B)
victim=00000001000000000000000B
cp -a "$TE/archive/$victim" "$TE/archive/$victim.bak"
truncate -s 4096 "$TE/archive/$victim"
B=$TE/recovery_work/mechB
prep_copy "$B"
"$PGBIN/pg_ctl" -D "$B/data" -l "$B/replica.log" start >/dev/null 2>&1 || true
# either the python helper's refusal message, or postgres FATAL before target
sleep 8
grep -Eq "truncated|FATAL" "$B/replica.log" \
  && ok "truncated segment is refused during recovery (helper FATAL logged)" \
  || no "truncated segment was not rejected"
if wait_log "$B" "pausing at the end of recovery" 15; then
  no "recovery must NOT reach target with truncated WAL"
else
  ok "recovery does not reach target with truncated WAL"
fi
# restore the good archive
mv -f "$TE/archive/$victim.bak" "$TE/archive/$victim"

echo "== scenario C: retry after repair reaches the same restore point =="
C=$TE/recovery_work/mechC
prep_copy "$C"
"$PGBIN/pg_ctl" -D "$C/data" -l "$C/replica.log" start >/dev/null 2>&1 || true
if wait_log "$C" "pausing at the end of recovery" 60; then
  ok "retry with repaired archive reaches target"
else
  no "retry failed"; tail -5 "$C/replica.log"
fi
CC=$TE/recovery_work/mechC_check
rm -rf "$CC"; cp -a "$C/data" "$CC"; rm -f "$CC/postmaster.pid"
total=$(single_query "$CC" "SELECT count(*) FROM traffic_minute_counts;" | tr -dc '0-9')
[ "$total" = "60" ] && ok "retry replica again shows 60 rows" || no "retry rows: $total"

echo
if [ "$fails" = "0" ]; then echo "ALL RECOVERY MECHANICS TESTS PASSED"; else echo "$fails FAILURE(S)"; exit 1; fi
