# VolVue → S3 export: status and how to resume

Everything is resumable: re-running a step skips work already in S3 (each day file is
checked against VolVue's own row count).

## Where the data is
`s3://volvue-archive-905418356298/volvue/` (eu-north-1, private, encrypted)

| Path | Contents |
|---|---|
| `raw/csv/year=YYYY/month=MM/volvue_YYYY-MM-DD.csv.gz` | one file per trading day, all 332 columns, as returned by VolVue |
| `parquet/year=YYYY/month=MM/volvue_YYYY-MM.parquet` | same data, one file per month, sorted by ticker+date |
| `manifest/date_counts.csv` | expected rows per trading day (from VolVue) |
| `metadata/fields.csv`, `metadata/tickers.csv` | column descriptions, ticker → company name |
| `_status/pipeline.log`, `_status/STATUS.json` | progress log (refreshed every 5 min) and final result |

Full history: 5,737 trading days, 49.3M rows (2004-01-02 .. 2026-10-21).

## Check progress without Claude
Open `_status/pipeline.log` in the S3 console. `STATUS.json` appears when the pipeline is done
(`export_ok` and `parquet_ok` both `true` = complete).

## Resume (if the run was interrupted)
Needs env vars `VOLVUE_API_KEY`, `AWS_Key`, `AWS_Pass`.

**On your own computer (no Claude tokens):**
```bash
git clone -b claude/peaceful-lovelace-u05qid https://github.com/marcinzieba4-bot/CopyFromVolVue
cd CopyFromVolVue && pip install boto3 pyarrow
bash run_pipeline.sh          # export (resumes) + Parquet + STATUS.json
```

**In a new Claude Code session (cheapest: pick Sonnet or Haiku), paste:**
> On branch claude/peaceful-lovelace-u05qid run `pip install -q boto3 pyarrow` then
> `nohup setsid bash run_pipeline.sh > /dev/null 2>&1 &`. Wait for it with one background
> `while pgrep -f run_pipeline.sh; do sleep 60; done` and then report the contents of
> s3://volvue-archive-905418356298/volvue/_status/STATUS.json. Do nothing else.

## Fetch the newest day later (while the subscription still works)
VolVue publishes a day's data after the US close. To add e.g. 2026-10-22:
```bash
python3 volvue_export.py --bucket volvue-archive-905418356298 --start 2026-10-22
python3 volvue_to_parquet.py --bucket volvue-archive-905418356298 --months 2026-10
```

## Cost
~23 GB csv.gz + ~25 GB Parquet ≈ 50 GB → about $1.10/month in S3 Standard. Delete one copy,
or move `raw/` to Glacier Instant Retrieval, to roughly halve that.

## Security
The AWS keys used here are root-account keys. Afterwards create an IAM user limited to this
bucket and delete/rotate the root keys.
