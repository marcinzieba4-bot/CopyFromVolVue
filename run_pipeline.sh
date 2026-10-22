#!/usr/bin/env bash
# Unattended VolVue -> S3 pipeline. Safe to re-run at any time: finished work is skipped.
#   1. waits for an already running volvue_export.py, then runs the export again until
#      every trading day is in S3 (up to 3 attempts; each attempt resumes)
#   2. builds the monthly Parquet copy
#   3. writes s3://$BUCKET/volvue/_status/STATUS.json and keeps the log at
#      s3://$BUCKET/volvue/_status/pipeline.log (refreshed every 5 minutes)
# Env: VOLVUE_API_KEY, AWS_Key, AWS_Pass. Optional: BUCKET, WORKERS, LOG, START, and
# EXTRA_LOG (log of an export started outside this script, also copied to _status/).
set -u
BUCKET=${BUCKET:-volvue-archive-905418356298}
WORKERS=${WORKERS:-16}
START=${START:-2004-01-01}
DIR=$(cd "$(dirname "$0")" && pwd)
LOG=${LOG:-$DIR/pipeline.log}

s3_put() {  # s3_put <local file> <key>
  python3 -I - "$BUCKET" "$1" "$2" <<'EOF' >/dev/null 2>&1
import os, sys, boto3
bucket, path, key = sys.argv[1:]
kw = {"aws_access_key_id": os.environ["AWS_Key"], "aws_secret_access_key": os.environ["AWS_Pass"]} if os.environ.get("AWS_Key") else {}
boto3.Session(region_name="eu-north-1", **kw).client("s3").upload_file(path, bucket, key)
EOF
}
log() { echo "$(date -u '+%F %T') PIPELINE $*" >>"$LOG"; }

upload_logs() {
  s3_put "$LOG" volvue/_status/pipeline.log
  [ -n "${EXTRA_LOG:-}" ] && [ -f "$EXTRA_LOG" ] && s3_put "$EXTRA_LOG" volvue/_status/export_first_run.log
}
( while sleep 300; do upload_logs; done ) &
UPLOADER=$!
trap 'kill $UPLOADER 2>/dev/null; upload_logs' EXIT

log "started (bucket=$BUCKET workers=$WORKERS start=$START)"
while pgrep -f "volvue_export.py --bucket" >/dev/null; do sleep 30; done

export_rc=1
for attempt in 1 2 3; do
  log "export attempt $attempt"
  python3 -I "$DIR/volvue_export.py" --bucket "$BUCKET" --workers "$WORKERS" --start "$START" >>"$LOG" 2>&1
  export_rc=$?
  [ $export_rc -eq 0 ] && break
  sleep 60
done
log "export finished rc=$export_rc"

parquet_rc=1
for attempt in 1 2; do
  log "parquet attempt $attempt"
  python3 -I "$DIR/volvue_to_parquet.py" --bucket "$BUCKET" --workers 2 >>"$LOG" 2>&1
  parquet_rc=$?
  [ $parquet_rc -eq 0 ] && break
done
log "parquet finished rc=$parquet_rc"

STATUS=$(mktemp)
printf '{"finished_at": "%s", "export_ok": %s, "parquet_ok": %s, "last_export_line": "%s"}\n' \
  "$(date -u +%FT%TZ)" "$([ $export_rc -eq 0 ] && echo true || echo false)" \
  "$([ $parquet_rc -eq 0 ] && echo true || echo false)" \
  "$(grep 'INFO finished' "$LOG" | tail -1 | sed 's/"/\\"/g')" >"$STATUS"
s3_put "$STATUS" volvue/_status/STATUS.json
log "done"
