"""Розбір журналу подій Spark (spark-events/<app-id>) без History Server.

    python3 scripts/eventlog.py spark-events/local-123            # markdown-звіт
    python3 scripts/eventlog.py spark-events/local-123 --json m.json

Звідки числа: кожна задача пише подію SparkListenerTaskEnd з "Task Metrics"; саме з них
Spark UI рахує Input / Shuffle Read / Shuffle Write на сторінці stage-а.
Shuffle Bytes Written - байти ПІСЛЯ стиснення (spark.shuffle.compress=true, lz4).
"""
import argparse
import json
import statistics
from collections import defaultdict


def _pct(vals, q):
    if not vals:
        return 0
    vals = sorted(vals)
    k = (len(vals) - 1) * q
    lo, hi = int(k), min(int(k) + 1, len(vals) - 1)
    return vals[lo] + (vals[hi] - vals[lo]) * (k - lo)


def parse(path):
    app = {"stages": {}, "jobs": {}, "sql": [], "env": {}}
    tasks = defaultdict(list)
    with open(path, encoding="utf-8") as f:
        for line in f:
            e = json.loads(line)
            ev = e.get("Event")
            if ev == "SparkListenerApplicationStart":
                app.update(name=e["App Name"], app_id=e.get("App ID"), start=e["Timestamp"])
            elif ev == "SparkListenerApplicationEnd":
                app["end"] = e["Timestamp"]
            elif ev == "SparkListenerEnvironmentUpdate":
                sp = e.get("Spark Properties", {})
                sp = sp if isinstance(sp, dict) else dict(sp)
                app["env"] = {k: sp.get(k) for k in (
                    "spark.sql.shuffle.partitions", "spark.sql.adaptive.enabled",
                    "spark.master", "spark.driver.memory", "spark.sql.session.timeZone")}
            elif ev == "SparkListenerJobStart":
                props = e.get("Properties", {}) or {}
                app["jobs"][e["Job ID"]] = {
                    "group": props.get("spark.jobGroup.id"),
                    "description": props.get("spark.job.description"),
                    "stage_ids": e["Stage IDs"], "submit": e["Submission Time"]}
            elif ev == "SparkListenerJobEnd":
                j = app["jobs"].setdefault(e["Job ID"], {})
                j["end"] = e["Completion Time"]
                j["result"] = e["Job Result"]["Result"]
            elif ev == "SparkListenerStageCompleted":
                si = e["Stage Info"]
                app["stages"][si["Stage ID"]] = {
                    "name": si["Stage Name"], "num_tasks": si["Number of Tasks"],
                    "submit": si.get("Submission Time"), "complete": si.get("Completion Time"),
                    "attempt": si.get("Stage Attempt ID", 0),
                    "rdds": [r["Name"] for r in si.get("RDD Info", [])]}
            elif ev == "SparkListenerTaskEnd":
                m = e.get("Task Metrics") or {}
                ti = e["Task Info"]
                sr = m.get("Shuffle Read Metrics", {})
                sw = m.get("Shuffle Write Metrics", {})
                im = m.get("Input Metrics", {})
                tasks[e["Stage ID"]].append({
                    "duration_ms": ti["Finish Time"] - ti["Launch Time"],
                    "run_ms": m.get("Executor Run Time", 0),
                    "gc_ms": m.get("JVM GC Time", 0),
                    "input_bytes": im.get("Bytes Read", 0),
                    "input_records": im.get("Records Read", 0),
                    "sr_bytes": sr.get("Remote Bytes Read", 0) + sr.get("Local Bytes Read", 0),
                    "sr_records": sr.get("Total Records Read", 0),
                    "sw_bytes": sw.get("Shuffle Bytes Written", 0),
                    "sw_records": sw.get("Shuffle Records Written", 0),
                    "spill_disk": m.get("Disk Bytes Spilled", 0),
                })
            elif ev == "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart":
                app["sql"].append({"id": e["executionId"], "description": e.get("description"),
                                   "plan": e.get("physicalPlanDescription", "")})

    stage_job = {}
    for jid, j in app["jobs"].items():
        for sid in j.get("stage_ids", []):
            stage_job.setdefault(sid, jid)

    for sid, st in app["stages"].items():
        ts = tasks.get(sid, [])
        st["job"] = stage_job.get(sid)
        st["group"] = app["jobs"].get(st["job"], {}).get("group")
        st["duration_ms"] = (st["complete"] or 0) - (st["submit"] or 0)
        for k in ("input_bytes", "input_records", "sr_bytes", "sr_records", "sw_bytes",
                  "sw_records", "spill_disk", "gc_ms"):
            st[k] = sum(t[k] for t in ts)
        for k in ("duration_ms", "sr_bytes", "sr_records", "sw_records", "input_bytes"):
            vals = [t[k] for t in ts]
            st[f"task_{k}"] = {"min": min(vals) if vals else 0,
                               "median": statistics.median(vals) if vals else 0,
                               "p90": _pct(vals, 0.9), "max": max(vals) if vals else 0}
        # Роль stage-а в нашій схемі (див. docs/algorithm.md):
        #   A: читає CSV і пише shuffle; B: читає і пише shuffle; C: лише читає shuffle.
        reads = st["sr_records"] > 0
        writes = st["sw_records"] > 0
        st["role"] = ("A (scan + partial agg -> shuffle)" if st["input_bytes"] and writes else
                      "B (shuffle read -> shuffle write)" if reads and writes else
                      "C (shuffle read -> result)" if reads else
                      "scan only" if st["input_bytes"] else "other")
    jobs_done = [j for j in app["jobs"].values() if "end" in j and "submit" in j]
    for j in jobs_done:
        j["duration_ms"] = j["end"] - j["submit"]
    app["duration_ms"] = (app.get("end") or 0) - (app.get("start") or 0)
    return app


def group_summary(app, group):
    """Сумарні метрики всіх job-ів однієї групи (напр. 'analysis')."""
    jobs = [j for j in app["jobs"].values() if j.get("group") == group and "duration_ms" in j]
    stages = [s for s in app["stages"].values() if s.get("group") == group]
    return {
        "jobs": len(jobs),
        "wall_ms": sum(j["duration_ms"] for j in jobs),
        "input_bytes": sum(s["input_bytes"] for s in stages),
        "input_records": sum(s["input_records"] for s in stages),
        "shuffle_write_bytes": sum(s["sw_bytes"] for s in stages),
        "shuffle_write_records": sum(s["sw_records"] for s in stages),
        "shuffle_read_bytes": sum(s["sr_bytes"] for s in stages),
        "stages": {sid: {k: s[k] for k in ("role", "num_tasks", "duration_ms", "input_bytes",
                                           "sw_records", "sw_bytes", "sr_records", "sr_bytes")}
                   for sid, s in sorted(app["stages"].items()) if s.get("group") == group},
    }


def fmt_bytes(n):
    for unit in ("B", "KiB", "MiB", "GiB"):
        if abs(n) < 1024 or unit == "GiB":
            return f"{n:.1f} {unit}" if unit != "B" else f"{n} B"
        n /= 1024


def to_markdown(app):
    out = [f"# Журнал подій: {app.get('name')} ({app.get('app_id')})", "",
           f"Тривалість застосунку: {app['duration_ms'] / 1000:.1f} с. Налаштування: `{app['env']}`", "",
           "## Job-и", "", "| Job | Група | Stage-і | Час, с | Результат |", "|---|---|---|---|---|"]
    for jid, j in sorted(app["jobs"].items()):
        out.append(f"| {jid} | {j.get('group')} | {j.get('stage_ids')} | "
                   f"{j.get('duration_ms', 0) / 1000:.2f} | {j.get('result')} |")
    out += ["", "## Stage-і (лише виконані; пропущені stage-і не мають подій)", "",
            "| Stage | Job | Роль | Задач | Час, с | Input | Shuffle write (записи / байти) | "
            "Shuffle read (записи / байти) | Задача: час min/med/max, мс | Задача: shuffle read med/max |",
            "|---|---|---|---|---|---|---|---|---|---|"]
    for sid, s in sorted(app["stages"].items()):
        td, tr = s["task_duration_ms"], s["task_sr_bytes"]
        out.append(
            f"| {sid} | {s['job']} ({s['group']}) | {s['role']} | {s['num_tasks']} | "
            f"{s['duration_ms'] / 1000:.2f} | {fmt_bytes(s['input_bytes'])} / {s['input_records']:,} | "
            f"{s['sw_records']:,} / {fmt_bytes(s['sw_bytes'])} | "
            f"{s['sr_records']:,} / {fmt_bytes(s['sr_bytes'])} | "
            f"{td['min']}/{td['median']:.0f}/{td['max']} | "
            f"{fmt_bytes(tr['median'])}/{fmt_bytes(tr['max'])} |")
    for q in app["sql"]:
        if "Exchange" in q["plan"]:
            out += ["", f"## Фізичний план SQL-виконання {q['id']}: {q['description']}", "",
                    "```", q["plan"].strip(), "```"]
    return "\n".join(out)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("eventlog")
    ap.add_argument("--json")
    ap.add_argument("--md")
    a = ap.parse_args()
    app = parse(a.eventlog)
    md = to_markdown(app)
    if a.md:
        open(a.md, "w", encoding="utf-8").write(md)
    else:
        print(md)
    if a.json:
        json.dump(app, open(a.json, "w", encoding="utf-8"), ensure_ascii=False, indent=1, default=str)
