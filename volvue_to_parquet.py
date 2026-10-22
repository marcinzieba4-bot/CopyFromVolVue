#!/usr/bin/env python3
"""Build a Parquet copy of the VolVue export, one file per month.

Reads the daily CSVs written by volvue_export.py from
  s3://BUCKET/PREFIX/raw/csv/year=YYYY/month=MM/volvue_YYYY-MM-DD.csv.gz
and writes
  s3://BUCKET/PREFIX/parquet/year=YYYY/month=MM/volvue_YYYY-MM.parquet
(ticker: string, date: date32, every other column: float64, zstd-compressed,
sorted by ticker then date).
Only reads S3, so it works after the VolVue subscription has ended. Months whose
Parquet file already holds the same number of rows as the CSVs are skipped.

Env: AWS_Key, AWS_Pass (falls back to the default boto3 chain).
"""
import argparse
import csv
import gzip
import io
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
from botocore.config import Config
from botocore.exceptions import ClientError

log = logging.getLogger("volvue-parquet")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--prefix", default="volvue")
    ap.add_argument("--region", default="eu-north-1")
    ap.add_argument("--months", nargs="*", help="only these months, e.g. 2026-10 2026-09")
    ap.add_argument("--workers", type=int, default=2, help="months converted in parallel (~2 GB RAM each)")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        handlers=[logging.StreamHandler(sys.stdout)])
    if os.environ.get("AWS_Key") and os.environ.get("AWS_Pass"):
        session = boto3.Session(aws_access_key_id=os.environ["AWS_Key"],
                                aws_secret_access_key=os.environ["AWS_Pass"], region_name=args.region)
    else:
        session = boto3.Session(region_name=args.region)
    s3 = session.client("s3", config=Config(max_pool_connections=32, retries={"max_attempts": 10, "mode": "adaptive"}))
    prefix = args.prefix.strip("/")

    fields = s3.get_object(Bucket=args.bucket, Key=f"{prefix}/metadata/fields.csv")["Body"].read().decode()
    columns = [r[0] for r in list(csv.reader(io.StringIO(fields)))[1:]]
    types = {c: pa.float64() for c in columns}
    types.update(ticker=pa.string(), date=pa.date32())
    convert = pacsv.ConvertOptions(column_types=types, strings_can_be_null=False)

    months = {}
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=args.bucket, Prefix=f"{prefix}/raw/csv/"):
        for o in page.get("Contents", []):
            parts = o["Key"].split("/")
            ym = f"{parts[-3].split('=')[1]}-{parts[-2].split('=')[1]}"
            months.setdefault(ym, []).append(o["Key"])
    if args.months:
        months = {m: k for m, k in months.items() if m in args.months}

    def convert_month(ym, keys):
        y, m = ym.split("-")
        out_key = f"{prefix}/parquet/year={y}/month={m}/volvue_{ym}.parquet"
        expected = sum(int(s3.head_object(Bucket=args.bucket, Key=k)["Metadata"]["rows"]) for k in keys)
        try:
            done = int(s3.head_object(Bucket=args.bucket, Key=out_key)["Metadata"].get("rows", -1))
            if done == expected:
                return ym, expected, "skipped"
        except ClientError:
            pass
        tables = []
        for k in sorted(keys):
            raw = gzip.decompress(s3.get_object(Bucket=args.bucket, Key=k)["Body"].read())
            tables.append(pacsv.read_csv(io.BytesIO(raw), convert_options=convert))
        table = pa.concat_tables(tables).sort_by([("ticker", "ascending"), ("date", "ascending")])
        if table.num_rows != expected or table.column_names != columns:
            raise RuntimeError(f"{ym}: {table.num_rows} rows / {table.num_columns} cols, expected {expected}")
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="zstd", compression_level=6, row_group_size=50_000)
        s3.put_object(Bucket=args.bucket, Key=out_key, Body=buf.getvalue(),
                      Metadata={"rows": str(table.num_rows), "days": str(len(keys))})
        return ym, table.num_rows, f"{buf.tell() / 1e6:.1f} MB"

    failed = []
    with ThreadPoolExecutor(args.workers) as pool:
        futs = {pool.submit(convert_month, ym, keys): ym for ym, keys in sorted(months.items(), reverse=True)}
        for fut in as_completed(futs):
            try:
                ym, rows, info = fut.result()
                log.info("%s: %s rows, %s", ym, f"{rows:,}", info)
            except Exception as e:
                log.error("%s failed: %s", futs[fut], e)
                failed.append(futs[fut])
    log.info("done; failed months: %s", ",".join(sorted(failed)) or "none")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
