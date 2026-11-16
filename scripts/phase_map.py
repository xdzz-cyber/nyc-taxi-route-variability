"""Карта фаз: оцінка проти факту для конкретного запуску.

    docker compose run --rm tools scripts/phase_map.py --label full

Читає out/<label>/summary.json (app_id, P), журнал подій spark-events/<app_id>,
out/<label>/explain.txt і out/explore/explore.json. Пише docs/phase_map_<label>.md:
таблицю етапів із заповненими стовпцями "Факт" і "Stage", відношення факт/оцінка,
рядки коду, що породили кожен Exchange, і фізичний план.
"""
import argparse
import json
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import estimate  # noqa: E402
from eventlog import fmt_bytes, parse  # noqa: E402
from rules_sql import C  # noqa: E402


def code_lines(pattern):
    path = os.path.join(C.APP_ROOT, "pipeline", "taxi_pipeline.py")
    return [i for i, line in enumerate(open(path, encoding="utf-8"), 1) if re.search(pattern, line)]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--label", default="full")
    ap.add_argument("--explore", default=os.path.join(C.APP_ROOT, "evidence", "exploration", "explore.json"))
    a = ap.parse_args()
    out_dir = os.path.join(C.OUT_DIR, a.label)
    summ = json.load(open(os.path.join(out_dir, "summary.json"), encoding="utf-8"))
    app = parse(os.path.join(C.EVENT_DIR, summ["app_id"]))
    P = summ["partitions"]
    est = estimate.numbers(json.load(open(a.explore, encoding="utf-8")), P) if os.path.exists(a.explore) else None

    stages = sorted((sid, s) for sid, s in app["stages"].items() if s["group"] == "analysis")
    by_role = {s["role"][0]: (sid, s) for sid, s in stages}
    job = next(j for j, v in app["jobs"].items() if v.get("group") == "analysis")
    A, B, Cc = by_role.get("A"), by_role.get("B"), by_role.get("C")

    def ratio(f, e):
        return f"{f / e:.2f}×" if e else "—"

    gb = code_lines(r"\.groupBy\(")
    L = [f"# Карта фаз запуску `{a.label}` (app {summ['app_id']}, P = {P})", "",
         f"Job `analysis` = job {job}; stage-і {[sid for sid, _ in stages]}. "
         f"Час job-а: {app['jobs'][job]['duration_ms'] / 1000:.1f} с; усього застосунку: {app['duration_ms'] / 1000:.1f} с.", "",
         "| Етап | Оцінка | Факт | Факт / оцінка | Stage / job | Рядок коду |", "|---|---|---|---|---|---|"]
    if A:
        s = A[1]
        L.append(f"| 0. Очищення (читання) | {est['input_records']:,} записів, {est['input_bytes']:,} Б, {est['map_tasks']} задач | "
                 f"{s['input_records']:,} записів, {s['input_bytes']:,} Б, {s['num_tasks']} задач | "
                 f"{ratio(s['input_bytes'], est['input_bytes'])} | stage {A[0]} / job {job} | `read_raw`, `classify` |" if est else "")
        L.append(f"| 1. Shuffle 1: (trip_date, PU, DO) | {est['s1_records']:,} записів; {fmt_bytes(est['s1_bytes_raw'])} без стиснення | "
                 f"{s['sw_records']:,} записів; {fmt_bytes(s['sw_bytes'])} (lz4) | записи {ratio(s['sw_records'], est['s1_records'])}, "
                 f"байти {ratio(s['sw_bytes'], est['s1_bytes_raw'])} | write: stage {A[0]}; read: stage {B[0] if B else '?'} | "
                 f"taxi_pipeline.py:{gb[0] if gb else '?'} `groupBy(trip_date, PU, DO)` |")
    if B:
        s = B[1]
        L.append(f"| 2. Shuffle 2: (PU, DO) | ≈ {est['s2_records']:,.0f} записів; {fmt_bytes(est['s2_bytes_raw'])} без стиснення | "
                 f"{s['sw_records']:,} записів; {fmt_bytes(s['sw_bytes'])} (lz4) | записи {ratio(s['sw_records'], est['s2_records'])}, "
                 f"байти {ratio(s['sw_bytes'], est['s2_bytes_raw'])} | write: stage {B[0]}; read: stage {Cc[0] if Cc else '?'} | "
                 f"taxi_pipeline.py:{gb[1] if len(gb) > 1 else '?'} `groupBy(PU, DO)` |")
    if est and A and est["map_tasks"] != A[1]["num_tasks"]:
        L += ["", f"**Увага:** розвідка рахувала розбиття для іншої кількості ядер ({est['map_tasks']} задач), "
                  f"а Spark мав defaultParallelism = {summ.get('default_parallelism')} і {A[1]['num_tasks']} задач. "
                  "Перезапустіть explore.py/estimate.py у тому самому контейнері або поясніть розбіжність."]
    L += ["", "Відбір днів (`filter(n_trips >= 30)`) і фінальний фільтр не дають shuffle: це Filter у stage B і C.",
          f"Фінальна кількість маршрутів: {summ.get('result_routes')} (оцінка {est['routes_out'] if est else '—'}).", "",
          "## Stage-і job-а analysis", "",
          "| Stage | Роль | Задач | Час, с | Input | Shuffle write | Shuffle read | Задача: час min / медіана / max, мс |",
          "|---|---|---|---|---|---|---|---|"]
    for sid, s in stages:
        td = s["task_duration_ms"]
        L.append(f"| {sid} | {s['role']} | {s['num_tasks']} | {s['duration_ms'] / 1000:.2f} | {fmt_bytes(s['input_bytes'])} | "
                 f"{s['sw_records']:,} / {fmt_bytes(s['sw_bytes'])} | {s['sr_records']:,} / {fmt_bytes(s['sr_bytes'])} | "
                 f"{td['min']} / {td['median']:.0f} / {td['max']} |")
    L += ["", "## Фізичний план (EXPLAIN FORMATTED)", "", "```",
          open(os.path.join(out_dir, "explain.txt"), encoding="utf-8").read().split("\n\n(1)")[0].strip(), "```", "",
          f"Повний план з усіма вузлами: `out/{a.label}/explain.txt`."]
    path = os.path.join(C.APP_ROOT, "docs", f"phase_map_{a.label}.md")
    open(path, "w", encoding="utf-8").write("\n".join(x for x in L if x is not None))
    print("\n".join(L))


if __name__ == "__main__":
    main()
