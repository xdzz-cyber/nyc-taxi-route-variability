"""Перекіс ключів на кожному shuffle і скільки він коштує в задачах (крок 3).

    docker compose run --rm tools scripts/skew.py --label full

Потрібен запуск конвеєра з --save-daily (out/<label>/daily_all.parquet = вихід етапу 1
до відбору днів) і його журнал подій. Пише docs/skew_<label>.md.

Два рівні:
  * ключі: скільки записів даних припадає на ключ (найчастіший, медіана, частка топ-10);
  * задачі: скільки записів/байтів реально прочитала кожна reduce-задача і скільки
    вона працювала (саме тут видно, чи перекіс щось коштує).
"""
import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from eventlog import fmt_bytes, parse  # noqa: E402
from rules_sql import C, connect  # noqa: E402


def key_stats(con, table, weight, keys):
    r = con.execute(f"""SELECT count(*), sum({weight}), max({weight}), median({weight}),
                               quantile_cont({weight}, 0.99) FROM {table}""").fetchone()
    top = con.execute(f"SELECT {keys}, {weight} FROM {table} ORDER BY {weight} DESC, {keys} LIMIT 10").fetchall()
    top10 = sum(t[-1] for t in top)
    return {"keys": r[0], "total": r[1], "max": r[2], "median": r[3], "p99": r[4],
            "top10_share": top10 / r[1] if r[1] else 0, "top_key": top[0] if top else None,
            "max_over_median": r[2] / r[3] if r[3] else None}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--label", default="full")
    a = ap.parse_args()
    out_dir = os.path.join(C.OUT_DIR, a.label)
    summ = json.load(open(os.path.join(out_dir, "summary.json"), encoding="utf-8"))
    con = connect()
    con.execute(f"CREATE VIEW daily_all AS SELECT * FROM read_parquet('{out_dir}/daily_all.parquet/*.parquet')")
    con.execute(f"""CREATE VIEW days AS SELECT PULocationID, DOLocationID, count(*) AS n_days FROM daily_all
                    WHERE n_trips >= {C.MIN_TRIPS_PER_DAY} GROUP BY ALL""")
    s1 = key_stats(con, "daily_all", "n_trips", "trip_date, PULocationID, DOLocationID")
    s2 = key_stats(con, "days", "n_days", "PULocationID, DOLocationID")

    app = parse(os.path.join(C.EVENT_DIR, summ["app_id"]))
    st = {s["role"][0]: (sid, s) for sid, s in app["stages"].items() if s["group"] == "analysis"}

    def task_line(name, pair):
        if not pair:
            return f"| {name} | — | — | — | — |"
        sid, s = pair
        r, b, d = s["task_sr_records"], s["task_sr_bytes"], s["task_duration_ms"]
        return (f"| {name} (stage {sid}, {s['num_tasks']} задач) | {r['min']:,} / {r['median']:,.0f} / {r['max']:,} "
                f"(max/med = {r['max'] / r['median'] if r['median'] else 0:.2f}) | {fmt_bytes(b['median'])} / {fmt_bytes(b['max'])} | "
                f"{d['min']} / {d['median']:.0f} / {d['max']} (max/med = {d['max'] / d['median'] if d['median'] else 0:.2f}) | "
                f"{s['duration_ms'] / 1000:.2f} |")

    L = [f"# Перекіс: запуск `{a.label}` (P = {summ['partitions']})", "",
         "## Рівень ключів", "",
         "| Shuffle | Ключ | Різних ключів | Найчастіший ключ | Максимум | Медіана | p99 | max / медіана | Частка топ-10 |",
         "|---|---|---|---|---|---|---|---|---|",
         f"| 1 | (trip_date, PU, DO); вага = поїздок | {s1['keys']:,} | {', '.join(map(str, s1['top_key'][:3]))} | {s1['max']:,} | {s1['median']:,.0f} | "
         f"{s1['p99']:,.0f} | {s1['max_over_median']:,.0f} | {s1['top10_share']:.2%} |",
         f"| 2 | (PU, DO); вага = відібраних днів | {s2['keys']:,} | {', '.join(map(str, s2['top_key'][:2]))} | {s2['max']:,} | {s2['median']:,.0f} | "
         f"{s2['p99']:,.0f} | {s2['max_over_median']:,.1f} | {s2['top10_share']:.2%} |", "",
         "Часткова агрегація: у shuffle 1 ключ дає не більше записів, ніж map-задач, у яких він трапився, "
         "незалежно від того, 1 у нього поїздка чи 1000. У shuffle 2 - не більше P. Тому перекіс у даних "
         "(стовпець «Максимум») майже не переходить у перекіс обсягу shuffle.", "",
         "## Рівень задач (що перекіс коштує)", "",
         "| Stage | Записів shuffle read на задачу: min / медіана / max | Байтів: медіана / max | Час задачі, мс: min / медіана / max | Час stage-а, с |",
         "|---|---|---|---|---|",
         task_line("B: читає shuffle 1", st.get("B")),
         task_line("C: читає shuffle 2", st.get("C")), ""]
    if st.get("A"):
        sid, s = st["A"]
        d = s["task_duration_ms"]
        L.append(f"Для порівняння, stage A ({s['num_tasks']} задач, читання CSV): час задачі {d['min']} / {d['median']:.0f} / {d['max']} мс, "
                 f"stage {s['duration_ms'] / 1000:.2f} с.")
    path = os.path.join(C.APP_ROOT, "docs", f"skew_{a.label}.md")
    open(path, "w", encoding="utf-8").write("\n".join(L))
    print("\n".join(L))
    json.dump({"stage1": s1, "stage2": s2}, open(os.path.join(out_dir, "skew.json"), "w"), default=str, indent=2)


if __name__ == "__main__":
    main()
