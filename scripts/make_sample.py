"""Створення реальної вибірки для демонстрації (1-2 хв на ноутбуці).

    docker compose run --rm tools scripts/make_sample.py --target-rows 500000

Чому не випадкова: при випадкових 2-3% рядків майже жоден маршрут не набере 30 поїздок
за день, і фінальна таблиця буде порожньою. Тому беремо ВСІ поїздки з кількох зон
відправлення за всі 182 дні - маршрути з цих зон зберігаються цілими, пороги працюють.

Правило відбору (детерміноване): зони відправлення, у яких є хоча б один маршрут у
фінальному результаті, упорядковані за кількістю рядків (від меншої) і далі за номером;
додаємо зони, доки не наберемо --target-rows. Плюс до --dirty-per-reason брудних рядків
кожної причини з інших зон, щоб на вибірці було видно всі правила очищення.
Рядки зберігаються без змін (усі 19 полів як у CSV) і в початковому порядку.
"""
import argparse
import glob
import hashlib
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from rules_sql import C, classified_sql, connect, raw_source, stages_sql  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="full")
    ap.add_argument("--target-rows", type=int, default=500_000)
    ap.add_argument("--dirty-per-reason", type=int, default=200)
    ap.add_argument("--out", default=C.SAMPLE_PATH)
    a = ap.parse_args()

    files = sorted(glob.glob(C.resolve_input(a.input)))
    con = connect()
    cols = ", ".join(C.COLUMNS)
    con.execute(f"CREATE TABLE raw ({', '.join(c + ' VARCHAR' for c in C.COLUMNS)}, file_idx INTEGER, rn BIGINT)")
    for i, f in enumerate(files):
        con.execute(f"INSERT INTO raw SELECT {cols}, {i}, row_number() OVER () FROM {raw_source(f)}")
        print(f"  прочитано {os.path.basename(f)}", flush=True)

    con.execute(f"CREATE TABLE cl AS {classified_sql('raw')}")
    st = stages_sql("cl")
    con.execute(f"CREATE TABLE daily_all AS {st['daily_all']}")
    con.execute(f"CREATE TABLE daily AS {st['daily']}")
    con.execute(f"CREATE TABLE result AS {st['result']}")

    zones = con.execute("""
        SELECT z.pu, z.n FROM (SELECT pu, count(*) AS n FROM cl WHERE pu IS NOT NULL GROUP BY pu) z
        WHERE z.pu IN (SELECT pu FROM result) ORDER BY z.n, z.pu""").fetchall()
    chosen, total = [], 0
    for pu, n in zones:
        if total >= a.target_rows:
            break
        chosen.append(pu)
        total += n
    if not chosen:
        sys.exit("Немає жодного маршруту в результаті - перевірте дані/пороги.")
    zl = ", ".join(map(str, chosen))

    con.execute(f"""
        CREATE TABLE pick AS
        SELECT file_idx, rn FROM cl WHERE pu IN ({zl})
        UNION
        SELECT file_idx, rn FROM (
            SELECT file_idx, rn, row_number() OVER (PARTITION BY status ORDER BY file_idx, rn) AS k
            FROM cl WHERE status <> '{C.ACCEPTED}' AND (pu IS NULL OR pu NOT IN ({zl})))
        WHERE k <= {a.dirty_per_reason}""")
    os.makedirs(os.path.dirname(a.out), exist_ok=True)
    con.execute(f"""COPY (SELECT {cols} FROM raw JOIN pick USING (file_idx, rn) ORDER BY file_idx, rn)
                    TO '{a.out}' (HEADER, DELIMITER ',', COMPRESSION gzip)""")

    n = con.execute(f"SELECT count(*) FROM pick").fetchone()[0]
    sha = hashlib.sha256(open(a.out, "rb").read()).hexdigest()
    exp_routes = con.execute(f"SELECT count(*) FROM result WHERE pu IN ({zl})").fetchone()[0]
    info = {"source_files": [os.path.basename(f) for f in files], "pickup_zones": chosen,
            "rows": n, "rows_from_zones": total, "dirty_per_reason": a.dirty_per_reason,
            "expected_result_routes": exp_routes, "file": os.path.basename(a.out),
            "bytes": os.path.getsize(a.out), "sha256": sha}
    with open(os.path.join(os.path.dirname(a.out), "SAMPLE_INFO.json"), "w", encoding="utf-8") as f:
        json.dump(info, f, ensure_ascii=False, indent=2)
    print(json.dumps(info, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
