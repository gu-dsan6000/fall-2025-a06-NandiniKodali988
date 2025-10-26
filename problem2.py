#!/usr/bin/env python3

import argparse
from pathlib import Path
from datetime import datetime
# for degugging purpose: 
_HAS_MPL = False
_HAS_SNS = False
try:
    import matplotlib
    matplotlib.use("Agg")           # headless backend for servers/CI
    import matplotlib.pyplot as plt
    _HAS_MPL = True
    try:
        import seaborn as sns
        _HAS_SNS = True
    except Exception:
        pass
except Exception:
    pass

# write to csv files 
def _write_csv(path: Path, header, rows):
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

# format a python datetime else none 
def _fmt(ts):
    return ts.strftime("%Y-%m-%d %H:%M:%S") if ts else ""


def _args():
    # parse CLI commands - spark master url, netid, output dir, no spark fast path
    ap = argparse.ArgumentParser()
    ap.add_argument("master", nargs="?", help="spark://<PRIVATE_IP>:7077 (omit with --skip-spark)")
    ap.add_argument("--net-id", help="Your NetID (bucket prefix), e.g. nk988")
    ap.add_argument("--outdir", default=str(Path("data") / "output"))
    ap.add_argument("--skip-spark", action="store_true", help="Skip Spark; rebuild figs/stats from CSVs")
    return ap.parse_args()


# plot the bar chart 
def _bar_chart(outdir: Path, cluster_summary):
    out = outdir / "problem2_bar_chart.png"
    if not cluster_summary:
        out.write_text("no data", encoding="utf-8"); return
    if not _HAS_MPL:
        out.write_text("matplotlib not installed", encoding="utf-8"); return

    clusters = [c for (c, n, _, _) in cluster_summary]
    counts = [n for (_, n, _, _) in cluster_summary]

    plt.figure(figsize=(10, 5))
    if _HAS_SNS:
        sns.barplot(x=clusters, y=counts)
    else:
        plt.bar(clusters, counts)

    for i, v in enumerate(counts):
        plt.text(i, v, str(v), ha="center", va="bottom", fontsize=9)

    plt.title("Applications per Cluster")
    plt.xlabel("Cluster ID")
    plt.ylabel("Number of applications")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig(out, dpi=150)
    plt.close()

# plot the density plot 
def _density_plot(outdir: Path, timeline):
    out = outdir / "problem2_density_plot.png"
    if not timeline:
        out.write_text("no data", encoding="utf-8"); return
    if not _HAS_MPL:
        out.write_text("matplotlib not installed", encoding="utf-8"); return

    # choose cluster with most apps
    from collections import Counter
    cnt = Counter([c for (c, _, _, _, _) in timeline])
    if not cnt:
        out.write_text("no data", encoding="utf-8"); return
    top_cluster, _ = cnt.most_common(1)[0]

    # durations for that cluster
    durations = []
    for (c, _, _, s, e) in timeline:
        if c == top_cluster and s and e and e >= s:
            durations.append((e - s).total_seconds())
    if not durations:
        out.write_text("no durations", encoding="utf-8"); return

    plt.figure(figsize=(10, 5))
    if _HAS_SNS:
        sns.histplot(durations, kde=True)
    else:
        plt.hist(durations, bins=30)

    plt.xscale("log")
    plt.xlabel("Application duration (seconds, log scale)")
    plt.ylabel("Count")
    plt.title(f"Duration distribution — Cluster {top_cluster} (n={len(durations)})")
    plt.tight_layout()
    plt.savefig(out, dpi=150)
    plt.close()


def _spark(master_url):
    from pyspark.sql import SparkSession
    return (
        SparkSession.builder
        .appName("A06_Problem2_nk988")
        .master(master_url)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )


def run_spark(master_url: str, net_id: str, outdir: Path):
    from pyspark.sql import functions as F

    spark = _spark(master_url)
    root = f"s3a://{net_id}-assignment-spark-cluster-logs/data/"

    # read all log lines recursively
    lines = (spark.read.option("recursiveFileLookup", "true").text(root)
             .withColumn("path", F.input_file_name()))

    # extract identifiers and timestamp string 
    ts_text        = F.regexp_extract("value", r"^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})", 1).alias("ts_text")
    application_id = F.regexp_extract("path",  r"(application_\d+_\d+)", 1).alias("application_id")
    cluster_id     = F.regexp_extract(application_id, r"application_(\d+)_\d+", 1).alias("cluster_id")
    app_number     = F.regexp_extract(application_id, r"application_\d+_(\d+)$", 1).alias("app_number")

    base = lines.select(application_id, cluster_id, app_number, ts_text)

    # robust timestamp parse: treat format as literal 
    with_ts = base.withColumn("ts", F.expr("try_to_timestamp(ts_text, 'yy/MM/dd HH:mm:ss')"))

    parsed = (with_ts.where(F.col("application_id") != "")
                     .where(F.col("ts").isNotNull())
                     .select("cluster_id", "application_id", "app_number", "ts")
                     .cache())

    # per app min/max
    timeline_df = (parsed.groupBy("cluster_id", "application_id", "app_number")
                          .agg(F.min("ts").alias("start_time"),
                               F.max("ts").alias("end_time")))

    # to driver, sorted
    def _to_int(s):
        try: return int(s)
        except: return None

    rows = timeline_df.select("cluster_id","application_id","app_number","start_time","end_time").collect()
    timeline = [(r["cluster_id"], r["application_id"], r["app_number"], r["start_time"], r["end_time"]) for r in rows]
    timeline.sort(key=lambda x: (x[0], _to_int(x[2]) if _to_int(x[2]) is not None else 0))

    # write timeline
    _write_csv(outdir / "problem2_timeline.csv",
               ["cluster_id","application_id","app_number","start_time","end_time"],
               [(c,a,n,_fmt(s),_fmt(e)) for (c,a,n,s,e) in timeline])

    # cluster summary
    from collections import defaultdict
    per_cluster = defaultdict(lambda: {"count":0,"first":None,"last":None})
    for c, a, n, s, e in timeline:
        d = per_cluster[c]; d["count"] += 1
        if s and (d["first"] is None or s < d["first"]): d["first"] = s
        if e and (d["last"]  is None or e > d["last"]):  d["last"]  = e

    cluster_summary = [(c, d["count"], d["first"], d["last"]) for c, d in per_cluster.items()]
    cluster_summary.sort(key=lambda x: (-x[1], x[0]))

    _write_csv(outdir / "problem2_cluster_summary.csv",
               ["cluster_id","num_applications","cluster_first_app","cluster_last_app"],
               [(c, n, _fmt(f), _fmt(l)) for (c,n,f,l) in cluster_summary])

    # stats
    total_clusters = len(per_cluster)
    total_apps = len(timeline)
    avg_apps = (total_apps / total_clusters) if total_clusters else 0.0
    top_lines = [f"  {i}. Cluster {c}: {n} applications" for i,(c,n,_,_) in enumerate(cluster_summary[:10],1)]

    (outdir / "problem2_stats.txt").write_text(
        "\n".join([
            f"Total unique clusters: {total_clusters}",
            f"Total applications: {total_apps}",
            f"Average applications per cluster: {avg_apps:.2f}",
            "",
            "Most heavily used clusters:",
            *top_lines,
            ""
        ]), encoding="utf-8"
    )

    # figures
    _bar_chart(outdir, cluster_summary)
    _density_plot(outdir, timeline)

    spark.stop()


def run_skip(outdir: Path):
    import csv
    t_path = outdir / "problem2_timeline.csv"
    s_path = outdir / "problem2_cluster_summary.csv"
    if not t_path.exists() or not s_path.exists():
        raise SystemExit("Missing CSVs in --outdir. Run the Spark pipeline first or place CSVs there.")

    # load
    cluster_summary = []
    with s_path.open() as f:
        for row in csv.DictReader(f):
            cluster_summary.append((
                row["cluster_id"],
                int(row["num_applications"]),
                datetime.strptime(row["cluster_first_app"], "%Y-%m-%d %H:%M:%S") if row["cluster_first_app"] else None,
                datetime.strptime(row["cluster_last_app"],  "%Y-%m-%d %H:%M:%S") if row["cluster_last_app"]  else None,
            ))

    timeline = []
    with t_path.open() as f:
        for row in csv.DictReader(f):
            s = datetime.strptime(row["start_time"], "%Y-%m-%d %H:%M:%S") if row["start_time"] else None
            e = datetime.strptime(row["end_time"],   "%Y-%m-%d %H:%M:%S") if row["end_time"]   else None
            timeline.append((row["cluster_id"], row["application_id"], row["app_number"], s, e))

    # rewrite stats to keep fresh
    total_clusters = len({c for (c,_,_,_,_) in timeline})
    total_apps     = len({a for (_,a,_,_,_) in timeline})
    avg_apps       = (total_apps / total_clusters) if total_clusters else 0.0
    cluster_summary_sorted = sorted(cluster_summary, key=lambda x: (-x[1], x[0]))
    top_lines = [f"  {i}. Cluster {c}: {n} applications" for i,(c,n,_,_) in enumerate(cluster_summary_sorted[:10],1)]
    (outdir / "problem2_stats.txt").write_text(
        "\n".join([
            f"Total unique clusters: {total_clusters}",
            f"Total applications: {total_apps}",
            f"Average applications per cluster: {avg_apps:.2f}",
            "",
            "Most heavily used clusters:",
            *top_lines,
            ""
        ]), encoding="utf-8"
    )

    # rebuild figures
    _bar_chart(outdir, cluster_summary_sorted)
    _density_plot(outdir, timeline)

def main():
    a = _args()
    outdir = Path(a.outdir)
    if a.skip_spark:
        run_skip(outdir); return 0
    if not a.master or not a.net_id:
        print("Provide the Spark master and --net-id, or use --skip-spark.")
        return 1
    run_spark(a.master, a.net_id, outdir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
