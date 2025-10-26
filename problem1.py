#!/usr/bin/env python3

import argparse
from pathlib import Path
from datetime import datetime
from pyspark.sql import SparkSession, functions as F


def parse_args():
    # parse CLI commands - spark master url, netid, output dir
    ap = argparse.ArgumentParser()
    ap.add_argument("master", help="spark://<PRIVATE_IP>:7077")
    ap.add_argument("--net-id", required=True)
    ap.add_argument("--outdir", default=str(Path.cwd()))
    return ap.parse_args()

# create a spark session 
def new_session(master_url: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName("A06_Problem1_nk988")
        .master(master_url)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider")
        .getOrCreate()
    )


def write_csv(path: Path, header, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        f.write(",".join(header) + "\n")
        for r in rows:
            out = []
            for v in r:
                s = "" if v is None else str(v)
                if any(c in s for c in [",", '"', "\n"]):
                    s = '"' + s.replace('"', '""') + '"'
                out.append(s)
            f.write(",".join(out) + "\n")


def main():
    args = parse_args()
    outdir = Path(args.outdir)

    # build a spark session connected to the master
    spark = new_session(args.master)

    # read logs recursively 
    root = f"s3a://{args.net_id}-assignment-spark-cluster-logs/data/"
    lines = (
        spark.read
        .option("recursiveFileLookup", "true")
        # each log line is a row 
        .text(root) 
    )
    # total raw lines
    total = lines.count()

    # fiter out the rows were it doesnt match a level
    logs = (
        lines
        .select(
            F.regexp_extract("value", r"(INFO|WARN|ERROR|DEBUG)", 1).alias("level"),
            F.col("value").alias("message")
        )
        .where(F.col("level") != "")
        .cache()
    )
    n_with_level = logs.count()

    # aggregate counts by level
    counts = (
        logs.groupBy("level")
            .agg(F.count("*").alias("cnt"))
            .collect()
    )
    order = ["INFO", "WARN", "ERROR", "DEBUG"]
    level_to_count = {k: 0 for k in order}
    level_to_count.update({row["level"]: int(row["cnt"]) for row in counts})

    # write to output file
    counts_rows = [(lvl, level_to_count[lvl]) for lvl in order]
    write_csv(outdir / "problem1_counts.csv", ["log_level", "count"], counts_rows)

    # random sample
    sample = logs.orderBy(F.rand()).limit(10).select("message", "level").collect()
    write_csv(outdir / "problem1_sample.csv", ["log_entry", "log_level"],
              [(r["message"], r["level"]) for r in sample])

    # summary text 
    def pct(n, d): return (100.0 * n / d) if d else 0.0
    dist_lines = [
        f"  {lvl:<5}: {level_to_count[lvl]:>10,} ({pct(level_to_count[lvl], n_with_level):6.2f}%)"
        for lvl in order
    ]
    summary_txt = "\n".join([
        f"Total log lines processed: {total:,}",
        f"Total lines with log levels: {n_with_level:,}",
        f"Unique log levels found: {sum(1 for v in level_to_count.values() if v > 0)}",
        "",
        "Log level distribution:",
        *dist_lines,
        ""
    ])
    (outdir / "problem1_summary.txt").write_text(summary_txt, encoding="utf-8")

    spark.stop()


if __name__ == "__main__":
    main()
