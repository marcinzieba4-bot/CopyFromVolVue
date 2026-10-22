#!/usr/bin/env python3
"""Export the full VolVue dataset (table `data`) to S3, one gzipped CSV per trading day.

Uses the VolVue SQL API (https://api.volvue.com/query, format=csv). The API rejects
queries whose scope is too large, so the export works day by day:

  1. For every month since --start, ask VolVue for the row count of each trading day
     (SELECT date, COUNT(*) ... GROUP BY date). Saved to manifest/date_counts.csv.
  2. For every trading day, download SELECT * for that day, check that the row count
     matches step 1 and that the header has every column, gzip it and upload it to
     s3://BUCKET/PREFIX/raw/csv/year=YYYY/month=MM/volvue_YYYY-MM-DD.csv.gz

Re-running skips days already in S3 with the expected row count, so it can be
resumed at any time. Newest days are fetched first.

Env: VOLVUE_API_KEY, AWS_Key, AWS_Pass (falls back to the default boto3 chain).
"""
import argparse
import csv
import datetime as dt
import gzip
import hashlib
import io
import json
import logging
import os
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
from botocore.config import Config

API_URL = "https://api.volvue.com/query"
EXPECTED_COLUMNS = 332
log = logging.getLogger("volvue")


class FatalApiError(Exception):
    """The API refuses the key (e.g. subscription ended); retrying will not help."""


def api_query(sql, retries=6, timeout=900):
    url = API_URL + "?" + urllib.parse.urlencode(
        {"apiKey": os.environ["VOLVUE_API_KEY"], "format": "csv", "data": sql})
    delay = 5
    for attempt in range(1, retries + 1):
        try:
            with urllib.request.urlopen(url, timeout=timeout) as r:
                return r.read()
        except urllib.error.HTTPError as e:
            body = e.read().decode(errors="replace")[:500]
            if e.code in (401, 402, 403):
                raise FatalApiError(f"HTTP {e.code}: {body}")
            if e.code == 400 and any(w in body.lower() for w in ("api key", "apikey", "subscri", "expired")):
                raise FatalApiError(f"HTTP 400: {body}")
            if e.code == 400 and attempt >= 2:
                raise RuntimeError(f"HTTP 400: {body}")
            log.warning("HTTP %s (attempt %d/%d) for %r: %s", e.code, attempt, retries, sql[:120], body)
        except Exception as e:  # network errors, timeouts, truncated reads
            log.warning("%s (attempt %d/%d) for %r: %s", type(e).__name__, attempt, retries, sql[:120], e)
        time.sleep(delay)
        delay = min(delay * 2, 120)
    raise RuntimeError(f"giving up after {retries} attempts: {sql[:120]}")


def month_starts(start, end):
    d = dt.date(start.year, start.month, 1)
    while d <= end:
        nxt = dt.date(d.year + (d.month == 12), d.month % 12 + 1, 1)
        yield d, nxt
        d = nxt


def day_counts_for_month(m_start, m_end):
    sql = (f"SELECT date, COUNT(*) AS n FROM data WHERE date >= '{m_start}' AND date < '{m_end}' "
           f"GROUP BY date ORDER BY date")
    rows = list(csv.reader(io.StringIO(api_query(sql).decode())))
    return [(r[0], int(r[1])) for r in rows[1:] if r]


class Exporter:
    def __init__(self, s3, bucket, prefix):
        self.s3, self.bucket, self.prefix = s3, bucket, prefix.strip("/")
        self.lock = threading.Lock()
        self.stats = {"done": 0, "skipped": 0, "failed": 0, "rows": 0, "raw_bytes": 0, "gz_bytes": 0}
        self.stop = threading.Event()

    def key(self, rel):
        return f"{self.prefix}/{rel}" if self.prefix else rel

    def day_key(self, day):
        y, m, _ = day.split("-")
        return self.key(f"raw/csv/year={y}/month={m}/volvue_{day}.csv.gz")

    def existing(self):
        """Map S3 key -> row count for daily files already uploaded."""
        out = {}
        pager = self.s3.get_paginator("list_objects_v2")
        for page in pager.paginate(Bucket=self.bucket, Prefix=self.key("raw/csv/")):
            for o in page.get("Contents", []):
                out[o["Key"]] = None
        return out

    def rows_in_s3(self, key):
        try:
            return int(self.s3.head_object(Bucket=self.bucket, Key=key)["Metadata"].get("rows", -1))
        except Exception:
            return None

    def export_day(self, day, expected):
        if self.stop.is_set():
            return
        key = self.day_key(day)
        raw = api_query(f"SELECT * FROM data WHERE date = '{day}' ORDER BY ticker")
        text = raw.decode()
        lines = text.count("\n") - (0 if text.endswith("\n") else -1)
        header = text.split("\n", 1)[0].split(",")
        rows = lines - 1
        if len(header) != EXPECTED_COLUMNS or rows != expected:
            raise RuntimeError(f"{day}: got {rows} rows / {len(header)} cols, expected {expected} / {EXPECTED_COLUMNS}")
        gz = gzip.compress(raw, compresslevel=6)
        self.s3.put_object(
            Bucket=self.bucket, Key=key, Body=gz, ContentType="text/csv", ContentEncoding="gzip",
            Metadata={"rows": str(rows), "columns": str(len(header)), "raw-bytes": str(len(raw)),
                      "raw-sha256": hashlib.sha256(raw).hexdigest(), "source": "api.volvue.com/query",
                      "exported-at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")})
        with self.lock:
            self.stats["done"] += 1
            self.stats["rows"] += rows
            self.stats["raw_bytes"] += len(raw)
            self.stats["gz_bytes"] += len(gz)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--prefix", default="volvue")
    ap.add_argument("--region", default="eu-north-1")
    ap.add_argument("--start", default="2004-01-01")
    ap.add_argument("--end", default=dt.date.today().isoformat())
    ap.add_argument("--workers", type=int, default=12)
    ap.add_argument("--recount", action="store_true", help="re-query day counts even if a manifest exists")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        handlers=[logging.StreamHandler(sys.stdout)])
    if os.environ.get("AWS_Key") and os.environ.get("AWS_Pass"):
        session = boto3.Session(aws_access_key_id=os.environ["AWS_Key"],
                                aws_secret_access_key=os.environ["AWS_Pass"], region_name=args.region)
    else:
        session = boto3.Session(region_name=args.region)
    s3 = session.client("s3", config=Config(max_pool_connections=args.workers * 2,
                                                          retries={"max_attempts": 10, "mode": "adaptive"}))
    ex = Exporter(s3, args.bucket, args.prefix)
    start, end = dt.date.fromisoformat(args.start), dt.date.fromisoformat(args.end)

    # 1. Trading days and expected row counts.
    manifest_key = ex.key("manifest/date_counts.csv")
    counts = {}
    if not args.recount:
        try:
            body = s3.get_object(Bucket=args.bucket, Key=manifest_key)["Body"].read().decode()
            counts = {r[0]: int(r[1]) for r in list(csv.reader(io.StringIO(body)))[1:] if r}
            log.info("loaded %d day counts from s3://%s/%s", len(counts), args.bucket, manifest_key)
        except s3.exceptions.NoSuchKey:
            pass
    months = [m for m in month_starts(start, end)
              if args.recount or not any(m[0].isoformat() <= d < m[1].isoformat() for d in counts)
              or m[1] > end - dt.timedelta(days=40)]
    if months:
        log.info("counting rows per day for %d months", len(months))
        with ThreadPoolExecutor(args.workers) as pool:
            for fut in as_completed([pool.submit(day_counts_for_month, a, b) for a, b in months]):
                for d, n in fut.result():
                    counts[d] = n
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(["date", "rows"])
        for d in sorted(counts):
            w.writerow([d, counts[d]])
        s3.put_object(Bucket=args.bucket, Key=manifest_key, Body=buf.getvalue().encode(), ContentType="text/csv")
    days = sorted((d for d in counts if args.start <= d <= args.end), reverse=True)
    log.info("%d trading days, %s rows expected (%s .. %s)", len(days), f"{sum(counts[d] for d in days):,}",
             days[-1] if days else "-", days[0] if days else "-")

    # 2. Daily files (skip ones already uploaded with the right row count).
    present = ex.existing()
    todo = []
    for d in days:
        k = ex.day_key(d)
        if k in present and ex.rows_in_s3(k) == counts[d]:
            ex.stats["skipped"] += 1
        else:
            todo.append(d)
    log.info("%d days already in S3, %d to download with %d workers", ex.stats["skipped"], len(todo), args.workers)

    t0, failed = time.time(), []
    with ThreadPoolExecutor(args.workers) as pool:
        futs = {pool.submit(ex.export_day, d, counts[d]): d for d in todo}
        for i, fut in enumerate(as_completed(futs), 1):
            d = futs[fut]
            try:
                fut.result()
            except FatalApiError as e:
                log.error("API refused the key, stopping: %s", e)
                ex.stop.set()
                failed.append(d)
            except Exception as e:
                log.error("%s failed: %s", d, e)
                ex.stats["failed"] += 1
                failed.append(d)
            if i % 25 == 0 or i == len(todo):
                el = time.time() - t0
                s = ex.stats
                log.info("progress %d/%d days | %s rows | %.2f GB raw -> %.2f GB gz | %.1f MB/s | ETA %.0f min",
                         i, len(todo), f"{s['rows']:,}", s["raw_bytes"] / 1e9, s["gz_bytes"] / 1e9,
                         s["raw_bytes"] / 1e6 / max(el, 1), (len(todo) - i) * el / i / 60)
    log.info("finished: %s; failed days: %s", json.dumps(ex.stats), ",".join(sorted(failed)) or "none")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
