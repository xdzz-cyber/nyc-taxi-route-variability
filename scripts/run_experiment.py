"""Експеримент (крок 5): spark.sql.shuffle.partitions = 8 проти 32, решта однакове.

    docker compose run --rm tools scripts/run_experiment.py --input full --runs 3

Протокол:
  * по одному прогріву кожного варіанта (не враховується): прогрів JVM-кешу класів не
    переноситься між запусками, але CSV потрапляє в кеш сторінок ОС - далі всі заміри
    в однакових умовах "теплого" кешу;
  * далі варіанти по черзі 8, 32, 8, 32, ... (щоб повільний дрейф стану машини не ліг
    на один варіант);
  * кожен запуск - окремий spark-submit з --no-quality (лише job analysis);
  * з журналу подій беремо час job-а analysis, прочитані байти, shuffle 1 і 2 окремо;
  * перевіряємо, що результат усіх запусків однаковий (n_days точно, mean/std/cv з допуском).
Результати: out/experiment/runs.csv і docs/experiment.md.
"""
import argparse
import csv
import json
import math
import os
import platform
import statistics
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from eventlog import fmt_bytes, parse  # noqa: E402
from rules_sql import C  # noqa: E402
from submit import SPARK_CONF, spark_submit_cmd  # noqa: E402

HYPOTHESIS = ("32 розділи зменшать медіанний час щонайменше на 15% порівняно з 8, "
              "а обсяг записаних shuffle-даних зміниться не більш ніж на 10%.")


def run_one(inp, p, label):
    cmd = spark_submit_cmd(["--input", inp, "--partitions", str(p), "--label", label, "--no-quality"])
    t = time.time()
    rc = subprocess.call(cmd, stdout=subprocess.DEVNULL)
    wall = time.time() - t
    if rc != 0:
        sys.exit(f"Запуск {label} завершився з кодом {rc}")
    summ = json.load(open(os.path.join(C.OUT_DIR, label, "summary.json"), encoding="utf-8"))
    app = parse(os.path.join(C.EVENT_DIR, summ["app_id"]))
    st = {s["role"][0]: s for s in app["stages"].values() if s["group"] == "analysis"}
    job = next(j for j in app["jobs"].values() if j.get("group") == "analysis")
    A, B, Cs = st.get("A", {}), st.get("B", {}), st.get("C", {})
    return {
        "label": label, "partitions": p, "app_id": summ["app_id"],
        "submit_wall_s": round(wall, 2), "analysis_job_s": job["duration_ms"] / 1000,
        "stageA_s": A.get("duration_ms", 0) / 1000, "stageB_s": B.get("duration_ms", 0) / 1000,
        "stageC_s": Cs.get("duration_ms", 0) / 1000,
        "input_bytes": A.get("input_bytes", 0),
        "s1_records": A.get("sw_records", 0), "s1_bytes": A.get("sw_bytes", 0),
        "s2_records": B.get("sw_records", 0), "s2_bytes": B.get("sw_bytes", 0),
        "shuffle_bytes_total": A.get("sw_bytes", 0) + B.get("sw_bytes", 0),
        "tasks_A": A.get("num_tasks", 0), "tasks_B": B.get("num_tasks", 0), "tasks_C": Cs.get("num_tasks", 0),
        "gc_ms": sum(s.get("gc_ms", 0) for s in st.values()),
    }


def load_result(label):
    with open(os.path.join(C.OUT_DIR, label, "result.csv"), encoding="utf-8") as f:
        return {(r["PULocationID"], r["DOLocationID"]): (int(r["n_days"]), float(r["mean_daily_min"]),
                                                          float(r["std_daily_min"]), float(r["cv"]))
                for r in csv.DictReader(f)}


def same_result(a, b, tol=1e-9):
    if a.keys() != b.keys():
        return False
    return all(x[0] == y[0] and all(math.isclose(u, v, rel_tol=tol, abs_tol=1e-12) for u, v in zip(x[1:], y[1:]))
               for x, y in ((a[k], b[k]) for k in a))


def machine_info():
    mem = ""
    try:
        mem = next(line for line in open("/proc/meminfo") if line.startswith("MemTotal")).split(":")[1].strip()
    except OSError:
        pass
    cpu = ""
    try:
        cpu = next(line for line in open("/proc/cpuinfo") if line.startswith("model name")).split(":")[1].strip()
    except (OSError, StopIteration):
        pass
    return {"cpu": cpu, "logical_cores_in_container": os.cpu_count(), "mem_in_container": mem,
            "kernel": platform.release(), "driver_memory": os.environ.get("DRIVER_MEMORY", "4g"),
            "spark_conf": SPARK_CONF}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="full")
    ap.add_argument("--variants", type=int, nargs="+", default=[8, 32])
    ap.add_argument("--runs", type=int, default=3)
    a = ap.parse_args()
    exp_dir = os.path.join(C.OUT_DIR, "experiment")
    os.makedirs(exp_dir, exist_ok=True)

    rows = []
    for p in a.variants:
        print(f"Прогрів P={p} ...", flush=True)
        r = run_one(a.input, p, f"exp-p{p}-warmup")
        r["warmup"] = True
        rows.append(r)
    for i in range(1, a.runs + 1):
        for p in a.variants:
            print(f"Замір {i}/{a.runs}, P={p} ...", flush=True)
            r = run_one(a.input, p, f"exp-p{p}-r{i}")
            r["warmup"] = False
            rows.append(r)
            print(f"   job analysis: {r['analysis_job_s']:.2f} с, shuffle 1: {fmt_bytes(r['s1_bytes'])}, "
                  f"shuffle 2: {r['s2_records']:,} записів / {fmt_bytes(r['s2_bytes'])}", flush=True)

    ref = load_result(rows[0]["label"])
    for r in rows:
        r["same_result"] = same_result(ref, load_result(r["label"]))

    keys = list(rows[0].keys())
    with open(os.path.join(exp_dir, "runs.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        w.writerows(rows)

    measured = [r for r in rows if not r["warmup"]]
    med = {p: {k: statistics.median(r[k] for r in measured if r["partitions"] == p)
               for k in ("analysis_job_s", "submit_wall_s", "stageA_s", "stageB_s", "stageC_s", "input_bytes",
                         "s1_bytes", "s1_records", "s2_bytes", "s2_records", "shuffle_bytes_total")}
           for p in a.variants}
    p0, p1 = a.variants[0], a.variants[-1]
    dt = (med[p1]["analysis_job_s"] - med[p0]["analysis_job_s"]) / med[p0]["analysis_job_s"]
    ds = (med[p1]["shuffle_bytes_total"] - med[p0]["shuffle_bytes_total"]) / med[p0]["shuffle_bytes_total"]

    L = ["# Експеримент: spark.sql.shuffle.partitions", "",
         f"**Гіпотеза (зафіксована в заявці):** {HYPOTHESIS}", "",
         "## Умови", "", "```", json.dumps(machine_info(), ensure_ascii=False, indent=2), "```",
         f"Вхід: `{C.resolve_input(a.input)}`. Прогрів: 1 запуск кожного варіанта (у таблиці позначено). "
         "Порядок замірів чергується. Після прогріву CSV лежить у кеші сторінок ОС (тепла файлова система) "
         "для всіх замірів однаково.", "",
         "## Усі запуски", "",
         "| Запуск | P | Прогрів | Job analysis, с | spark-submit, с | Stage A / B / C, с | Задач A / B / C | Прочитано | Shuffle 1 | Shuffle 2 (записи / байти) | Результат той самий |",
         "|---|---|---|---|---|---|---|---|---|---|---|"]
    for r in rows:
        L.append(f"| {r['label']} | {r['partitions']} | {'так' if r['warmup'] else ''} | {r['analysis_job_s']:.2f} | "
                 f"{r['submit_wall_s']:.1f} | {r['stageA_s']:.2f} / {r['stageB_s']:.2f} / {r['stageC_s']:.2f} | "
                 f"{r['tasks_A']} / {r['tasks_B']} / {r['tasks_C']} | {fmt_bytes(r['input_bytes'])} | "
                 f"{fmt_bytes(r['s1_bytes'])} | {r['s2_records']:,} / {fmt_bytes(r['s2_bytes'])} | {'так' if r['same_result'] else 'НІ'} |")
    L += ["", "## Медіани (без прогріву)", "",
          "| P | Job analysis, с | Stage A, с | Stage B, с | Stage C, с | Shuffle 1 | Shuffle 2 | Shuffle разом | Прочитано |",
          "|---|---|---|---|---|---|---|---|---|"]
    for p in a.variants:
        m = med[p]
        L.append(f"| {p} | {m['analysis_job_s']:.2f} | {m['stageA_s']:.2f} | {m['stageB_s']:.2f} | {m['stageC_s']:.2f} | "
                 f"{fmt_bytes(m['s1_bytes'])} | {m['s2_records']:,.0f} / {fmt_bytes(m['s2_bytes'])} | "
                 f"{fmt_bytes(m['shuffle_bytes_total'])} | {fmt_bytes(m['input_bytes'])} |")
    L += ["", "## Перевірка гіпотези", "",
          f"* Зміна медіанного часу job-а analysis при P={p1} відносно P={p0}: **{dt:+.1%}** "
          f"(гіпотеза: ≤ −15%) → {'підтверджено' if dt <= -0.15 else 'НЕ підтверджено'}.",
          f"* Зміна сумарного обсягу shuffle: **{ds:+.1%}** (гіпотеза: |зміна| ≤ 10%) → "
          f"{'підтверджено' if abs(ds) <= 0.10 else 'НЕ підтверджено'}.",
          f"* Частка stage A в часі job-а: {med[p0]['stageA_s'] / med[p0]['analysis_job_s']:.0%} (P={p0}), "
          f"{med[p1]['stageA_s'] / med[p1]['analysis_job_s']:.0%} (P={p1}). P змінює лише stage B і C.", "",
          "_Висновок і пояснення (коли доречний кожен варіант) - дописати вручну._"]
    path = os.path.join(C.APP_ROOT, "docs", "experiment.md")
    open(path, "w", encoding="utf-8").write("\n".join(L))
    print("\n".join(L))


if __name__ == "__main__":
    main()
