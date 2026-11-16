"""Незалежна перевірка результату Spark на DuckDB.

    python3 scripts/verify_result.py --input sample --label sample

Порівнює out/<label>/result.csv і quality.json з тим самим обчисленням на DuckDB:
кількості за причинами, набір маршрутів, n_days (точно), mean/std/cv (з допуском:
порядок додавання дробових чисел у Spark і DuckDB різний).
"""
import argparse
import csv
import json
import math
import os
import sys

LOG = []


def say(msg):
    print(msg)
    LOG.append(msg)


sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from rules_sql import C, classified_sql, connect, raw_source, stages_sql  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="sample")
    ap.add_argument("--label", default="sample")
    ap.add_argument("--rel-tol", type=float, default=1e-9)
    a = ap.parse_args()
    out_dir = os.path.join(C.OUT_DIR, a.label)

    con = connect()
    # лише потрібні стовпці: на повному наборі 19 рядкових полів не вмістилися б у пам'ять
    con.execute(f"""CREATE TEMP TABLE cl AS SELECT trip_date, pu, do_, duration_sec, status
                    FROM ({classified_sql(raw_source(C.resolve_input(a.input)))})""")
    st = stages_sql("cl")
    con.execute(f"CREATE TEMP TABLE daily_all AS {st['daily_all']}")
    con.execute(f"CREATE TEMP TABLE daily AS {st['daily']}")
    duck = {(r[0], r[1]): r[2:] for r in con.execute(st["result"]).fetchall()}
    duck_q = dict(con.execute("SELECT status, count(*) FROM cl GROUP BY 1").fetchall())

    ok = True
    qpath = os.path.join(out_dir, "quality.json")
    if os.path.exists(qpath):
        spark_q = {k: v["n"] for k, v in json.load(open(qpath, encoding="utf-8")).items()}
        for k in sorted(set(spark_q) | set(duck_q)):
            same = spark_q.get(k, 0) == duck_q.get(k, 0)
            ok &= same
            say(f"  {'OK ' if same else 'ERR'} {k:<24} spark={spark_q.get(k, 0):>12,} duckdb={duck_q.get(k, 0):>12,}")

    spark = {}
    with open(os.path.join(out_dir, "result.csv"), encoding="utf-8") as f:
        for r in csv.DictReader(f):
            spark[(int(r["PULocationID"]), int(r["DOLocationID"]))] = (
                int(r["n_days"]), float(r["mean_daily_min"]), float(r["std_daily_min"]), float(r["cv"]))

    only_s, only_d = set(spark) - set(duck), set(duck) - set(spark)
    say(f"Маршрутів: spark={len(spark)}, duckdb={len(duck)}, лише в spark={len(only_s)}, лише в duckdb={len(only_d)}")
    ok &= not only_s and not only_d
    worst = 0.0
    for k in set(spark) & set(duck):
        s, d = spark[k], duck[k]
        if s[0] != d[0]:
            ok = False
            say(f"  ERR n_days {k}: {s[0]} != {d[0]}")
        for x, y in zip(s[1:], d[1:]):
            rel = abs(x - y) / max(abs(y), 1e-12)
            worst = max(worst, rel)
            if not math.isclose(x, y, rel_tol=a.rel_tol, abs_tol=1e-12):
                ok = False
    say(f"Найбільша відносна різниця mean/std/cv: {worst:.2e} (допуск {a.rel_tol:.0e})")
    say("РЕЗУЛЬТАТ ЗБІГАЄТЬСЯ" if ok else "Є РОЗБІЖНОСТІ")
    with open(os.path.join(out_dir, "verify.txt"), "w", encoding="utf-8") as f:
        f.write(f"verify_result.py --input {a.input} --label {a.label}\n" + "\n".join(LOG) + "\n")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
