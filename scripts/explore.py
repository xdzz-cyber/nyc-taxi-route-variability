"""Розвідка даних ДО реалізації конвеєра: вхід для правил очищення й таблиці оцінок.

    docker compose run --rm tools scripts/explore.py --input full

Працює на DuckDB, незалежно від Spark. Рахує:
  * рядки й байти кожного файлу;
  * кількість записів за причинами відкидання (правила - pipeline/config.py);
  * розподіл тривалості (для обґрунтування меж MIN/MAX_DURATION_SEC);
  * чи мають записи з від'ємною сумою "двійника" з додатною сумою;
  * кількість різних ключів кожного етапу і розподіл записів на ключ;
  * модель часткової агрегації: скільки різних ключів етапу 1 потрапляє в кожну
    map-задачу Spark (розбиття CSV на шматки по maxSplitBytes, як у FilePartition).
Результат: out/explore/explore.json (його читає scripts/estimate.py).
"""
import argparse
import glob
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from rules_sql import C, classified_sql, connect, raw_source  # noqa: E402

MiB = 1024 * 1024


def spark_partitions(file_sizes, cores, max_partition_bytes=128 * MiB, open_cost=4 * MiB):
    """Повторює логіку Spark 3.5 FilePartition.maxSplitBytes + getFilePartitions.

    Повертає список розділів; розділ = список (file_idx, start_byte, end_byte).
    """
    total = sum(file_sizes) + open_cost * len(file_sizes)
    bytes_per_core = total // cores
    max_split = min(max_partition_bytes, max(open_cost, bytes_per_core))
    splits = []
    for i, size in enumerate(file_sizes):
        off = 0
        while off < size:
            ln = min(max_split, size - off)
            splits.append((i, off, off + ln))
            off += ln
    splits.sort(key=lambda s: s[2] - s[1], reverse=True)
    parts, cur, cur_size = [], [], 0
    for s in splits:
        ln = s[2] - s[1]
        if cur and cur_size + ln > max_split:
            parts.append(cur)
            cur, cur_size = [], 0
        cur.append(s)
        cur_size += ln + open_cost
    if cur:
        parts.append(cur)
    return max_split, parts


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="full")
    ap.add_argument("--cores", type=int, default=os.cpu_count(),
                    help="кількість ядер, які побачить local[*] (за замовчуванням - як у контейнері)")
    ap.add_argument("--out", default=os.path.join(C.OUT_DIR, "explore"))
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)
    t0 = time.time()

    files = sorted(glob.glob(C.resolve_input(a.input)))
    if not files:
        sys.exit(f"Немає файлів за шаблоном {C.resolve_input(a.input)}")
    con = connect()

    # 1. Класифікація кожного рядка; rowid таблиці = порядок рядків у файлі.
    con.execute("""CREATE TABLE cl (file_idx INTEGER, row_in_file BIGINT, pickup_ts TIMESTAMP,
                   dropoff_ts TIMESTAMP, pu INTEGER, do_ INTEGER, amount DOUBLE,
                   duration_sec BIGINT, trip_date DATE, status VARCHAR, dst_corrected BOOLEAN)""")
    info = []
    for i, f in enumerate(files):
        con.execute(f"""INSERT INTO cl SELECT {i}, row_number() OVER () - 1, pickup_ts, dropoff_ts,
                        pu, do_, amount, duration_sec, trip_date, status, dst_corrected
                        FROM ({classified_sql(raw_source(f))})""")
        n = con.execute(f"SELECT count(*) FROM cl WHERE file_idx={i}").fetchone()[0]
        info.append({"file": os.path.basename(f), "bytes": os.path.getsize(f), "rows": n})
        print(f"  {os.path.basename(f)}: {n:,} рядків, {os.path.getsize(f):,} байтів", flush=True)
    res = {"files": info, "total_rows": sum(x["rows"] for x in info),
           "total_bytes": sum(x["bytes"] for x in info), "cores": a.cores}

    # 2. Причини відкидання.
    res["status"] = dict(con.execute("SELECT status, count(*) FROM cl GROUP BY 1 ORDER BY 2 DESC").fetchall())
    res["dst_corrected_accepted"] = con.execute(
        f"SELECT count(*) FROM cl WHERE dst_corrected AND status='{C.ACCEPTED}'").fetchone()[0]

    # 3. Розподіл тривалості серед записів, що пройшли всі перевірки, крім меж тривалості.
    dur_where = "status IN ('accepted','too_short','too_long','non_positive_duration')"
    qs = [0.001, 0.01, 0.05, 0.5, 0.9, 0.99, 0.995, 0.999, 0.9999]
    vals = con.execute(f"SELECT {', '.join(f'quantile_cont(duration_sec, {q})' for q in qs)} "
                       f"FROM cl WHERE {dur_where}").fetchone()
    res["duration_quantiles_sec"] = {str(q): v for q, v in zip(qs, vals)}
    thresholds = [0, 30, 60, 120, 3600, 2 * 3600, 3 * 3600, 4 * 3600, 6 * 3600, 12 * 3600, 23 * 3600, 24 * 3600]
    res["duration_counts"] = {
        **{f"<= {t}s": con.execute(f"SELECT count(*) FROM cl WHERE {dur_where} AND duration_sec <= {t}").fetchone()[0]
           for t in thresholds[:4]},
        **{f"> {t}s": con.execute(f"SELECT count(*) FROM cl WHERE {dur_where} AND duration_sec > {t}").fetchone()[0]
           for t in thresholds[4:]},
    }
    hist = con.execute(f"""SELECT least(duration_sec // 1800, 48) AS bin, count(*) FROM cl
                           WHERE {dur_where} AND duration_sec > 0 GROUP BY 1 ORDER BY 1""").fetchall()
    res["duration_hist_30min"] = {f"{b * 30}-{b * 30 + 30} хв" if b < 48 else ">= 24 год": n for b, n in hist}

    # 4. Від'ємні суми: чи є додатний "двійник" з тими ж часом і зонами.
    res["negative_amount"] = dict(zip(("rows", "with_positive_twin"), con.execute("""
        WITH neg AS (SELECT * FROM cl WHERE amount < 0)
        SELECT count(*), count(*) FILTER (WHERE EXISTS (
            SELECT 1 FROM cl p WHERE p.amount > 0 AND p.pickup_ts = neg.pickup_ts
              AND p.dropoff_ts = neg.dropoff_ts AND p.pu = neg.pu AND p.do_ = neg.do_))
        FROM neg""").fetchone()))

    # 5. Ключі етапів (серед прийнятих записів).
    acc = f"status = '{C.ACCEPTED}'"
    con.execute(f"""CREATE TABLE k1 AS SELECT trip_date, pu, do_, count(*) AS n FROM cl WHERE {acc}
                    GROUP BY ALL""")
    r = con.execute("""SELECT count(*), count(DISTINCT (pu, do_)), max(n), median(n), quantile_cont(n, 0.9),
                              sum(n) FROM k1""").fetchone()
    top10 = con.execute("SELECT sum(n) FROM (SELECT n FROM k1 ORDER BY n DESC LIMIT 10)").fetchone()[0]
    topkey = con.execute("SELECT trip_date, pu, do_, n FROM k1 ORDER BY n DESC LIMIT 1").fetchone()
    res["stage1_keys"] = {"K1_distinct_date_route": r[0], "routes_distinct": r[1],
                          "max_trips_per_key": r[2], "max_key": [str(topkey[0]), topkey[1], topkey[2]],
                          "median_trips_per_key": r[3], "p90_trips_per_key": r[4],
                          "accepted_trips": r[5], "top10_share": top10 / r[5] if r[5] else 0,
                          "keys_per_day_avg": r[0] / max(1, con.execute("SELECT count(DISTINCT trip_date) FROM k1").fetchone()[0])}
    con.execute(f"CREATE TABLE d AS SELECT pu, do_, count(*) AS days FROM k1 WHERE n >= {C.MIN_TRIPS_PER_DAY} GROUP BY ALL")
    r2 = con.execute(f"""SELECT (SELECT count(*) FROM k1 WHERE n >= {C.MIN_TRIPS_PER_DAY}), count(*), max(days),
                         median(days), count(*) FILTER (WHERE days >= {C.MIN_DAYS_PER_ROUTE}) FROM d""").fetchone()
    res["stage2_keys"] = {"daily_rows_after_filter": r2[0], "routes_with_qualifying_day": r2[1],
                          "max_days_per_route": r2[2], "median_days_per_route": r2[3],
                          "routes_with_min_days": r2[4]}
    # Гістограма "кількість відібраних днів -> скільки маршрутів" (для оцінки shuffle 2).
    res["stage2_days_hist"] = dict(con.execute("SELECT days, count(*) FROM d GROUP BY 1 ORDER BY 1").fetchall())

    # 6. Модель часткової агрегації етапу 1: розділи вводу Spark і різні ключі в кожному.
    max_split, parts = spark_partitions([x["bytes"] for x in info], a.cores)
    rows_ranges = []
    for pid, part in enumerate(parts):
        for fi, b0, b1 in part:
            size, n = info[fi]["bytes"], info[fi]["rows"]
            # рядок належить шматку, в якому починається; байти ~ пропорційні рядкам
            rows_ranges.append((pid, fi, int(b0 / size * n), int(b1 / size * n)))
    con.execute("CREATE TABLE ranges (pid INTEGER, fi INTEGER, r0 BIGINT, r1 BIGINT)")
    con.executemany("INSERT INTO ranges VALUES (?, ?, ?, ?)", rows_ranges)
    per_part = con.execute(f"""
        SELECT r.pid, count(*) AS rows_, count(*) FILTER (WHERE {acc}) AS acc,
               count(DISTINCT CASE WHEN {acc} THEN (trip_date, pu, do_) END) AS k1,
               count(DISTINCT trip_date) AS dates,
               count(DISTINCT status) AS statuses
        FROM cl JOIN ranges r ON cl.file_idx = r.fi AND cl.row_in_file >= r.r0 AND cl.row_in_file < r.r1
        GROUP BY r.pid ORDER BY r.pid""").fetchall()
    res["map_tasks"] = {
        "max_split_bytes": max_split, "num_partitions": len(parts),
        "partitions": [{"pid": p[0], "rows": p[1], "accepted": p[2], "distinct_stage1_keys": p[3],
                        "distinct_dates": p[4], "distinct_status": p[5]} for p in per_part],
    }
    res["map_tasks"]["shuffle1_records_model"] = sum(p[3] for p in per_part)
    res["map_tasks"]["quality_shuffle_records_model"] = sum(p[5] for p in per_part)
    res["elapsed_s"] = round(time.time() - t0, 1)

    path = os.path.join(a.out, "explore.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(res, f, ensure_ascii=False, indent=2, default=str)
    print(json.dumps({k: v for k, v in res.items() if k not in ("map_tasks", "stage2_days_hist")},
                     ensure_ascii=False, indent=2, default=str))
    print(f"Розділів вводу: {len(parts)} (maxSplitBytes={max_split:,}); "
          f"модель записів shuffle 1: {res['map_tasks']['shuffle1_records_model']:,}")
    print(f"Збережено: {path}")


if __name__ == "__main__":
    main()
