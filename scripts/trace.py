"""Простежити один вхідний рядок через усі етапи (підготовка до питання на захисті).

    docker compose run --rm tools scripts/trace.py --input sample --line 12345 --partitions 32
    docker compose run --rm tools scripts/trace.py --input sample --random 7

--line N: N-й рядок даних файлу (без заголовка, від 1). Для --input full додайте --file
(номер місячного файлу від 0). Показує: сирий рядок, правило очищення, map-задачу stage A
(модель розбиття CSV), ключ і reduce-розділ shuffle 1, денний підсумок і відбір,
ключ і розділ shuffle 2, підсумок маршруту і місце у фінальній таблиці.
"""
import argparse
import glob
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from explore import spark_partitions  # noqa: E402
from rules_sql import C, classified_sql, connect, raw_source, stages_sql  # noqa: E402
from sparkhash import partition  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="sample")
    ap.add_argument("--file", type=int, default=0, help="номер файлу (для --input full)")
    ap.add_argument("--line", type=int, help="номер рядка даних у файлі, від 1")
    ap.add_argument("--random", type=int, help="seed: взяти випадковий ПРИЙНЯТИЙ рядок з маршруту в результаті")
    ap.add_argument("--partitions", type=int, default=32)
    ap.add_argument("--cores", type=int, default=os.cpu_count())
    a = ap.parse_args()

    files = sorted(glob.glob(C.resolve_input(a.input)))
    con = connect()
    cols = ", ".join(C.COLUMNS)
    con.execute(f"CREATE TABLE raw ({', '.join(c + ' VARCHAR' for c in C.COLUMNS)}, file_idx INTEGER, rn BIGINT)")
    for i, f in enumerate(files):
        con.execute(f"INSERT INTO raw SELECT {cols}, {i}, row_number() OVER () FROM {raw_source(f)}")
    con.execute(f"CREATE TABLE cl AS {classified_sql('raw')}")
    st = stages_sql("cl")
    for name in ("daily_all", "daily", "result"):
        con.execute(f"CREATE TABLE {name} AS {st[name]}")

    if a.random is not None:
        con.execute(f"SELECT setseed({(a.random % 1000) / 1000})")
        fi, line = con.execute(f"""SELECT file_idx, rn FROM cl WHERE status='{C.ACCEPTED}'
            AND (pu, do_) IN (SELECT (pu, do_) FROM result)
            AND (trip_date, pu, do_) IN (SELECT (trip_date, pu, do_) FROM daily)
            ORDER BY random() LIMIT 1""").fetchone()
    else:
        fi, line = a.file, a.line
    cur = con.execute(f"SELECT * FROM cl WHERE file_idx={fi} AND rn={line}")
    one = cur.fetchone()
    if one is None:
        sys.exit("Рядок не знайдено")
    r = dict(zip([x[0] for x in cur.description], one))
    P = a.partitions

    print(f"=== Рядок {line} файлу {os.path.basename(files[fi])} ===")
    for c in ("tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID", "DOLocationID",
              "trip_distance", "total_amount"):
        print(f"  {c:<24} {r[c]}")

    sizes = [os.path.getsize(f) for f in files]
    nrows = [con.execute(f"SELECT count(*) FROM raw WHERE file_idx={i}").fetchone()[0] for i in range(len(files))]
    _, parts = spark_partitions(sizes, a.cores)
    byte_pos = (line - 1) / nrows[fi] * sizes[fi]
    task = next((pid for pid, p in enumerate(parts) for (f_, b0, b1) in p if f_ == fi and b0 <= byte_pos < b1), None)
    gz = files[fi].endswith(".gz")
    print(f"\nStage A: map-задача {0 if gz else task} з {1 if gz else len(parts)} "
          f"({'gzip не ділиться - один розділ на файл' if gz else 'модель розбиття CSV, local[*] з ' + str(a.cores) + ' ядрами'})")

    print(f"Очищення: статус = {r['status']}, тривалість = {r['duration_sec']} с"
          + (" (виправлено на -3600 с: перехід на літній час)" if r["dst_corrected"] else ""))
    if r["status"] != C.ACCEPTED:
        print("  Рядок відкинуто тут - далі він нікуди не йде (лише в лічильник звіту якості).")
        return

    d, pu, do = r["trip_date"], int(r["pu"]), int(r["do_"])
    k1 = con.execute(f"SELECT n_trips, sum_sec FROM daily_all WHERE trip_date=DATE '{d}' AND pu={pu} AND do_={do}").fetchone()
    print(f"\nЕтап 1 / shuffle 1: ключ (trip_date={d}, PU={pu}, DO={do}), значення duration_sec={r['duration_sec']}")
    print(f"  map-задача спершу додає його до часткового (count, sum) цього ключа у своїй хеш-таблиці;")
    print(f"  частковий запис іде в reduce-розділ pmod(hash(date, PU, DO), {P}) = {partition(P, d, pu, do)}")
    print(f"  підсумок ключа: n_trips={k1[0]}, sum_sec={k1[1]}, середня={k1[1] / k1[0] / 60:.2f} хв")
    if k1[0] < C.MIN_TRIPS_PER_DAY:
        print(f"  Відбір днів: {k1[0]} < {C.MIN_TRIPS_PER_DAY} - день відкинуто, рядок далі не впливає.")
        return
    print(f"  Відбір днів: {k1[0]} >= {C.MIN_TRIPS_PER_DAY} - день іде в етап 2 як один рядок avg_min.")

    rt = con.execute(f"""SELECT count(*), avg(avg_min), stddev_pop(avg_min) FROM daily WHERE pu={pu} AND do_={do}""").fetchone()
    print(f"\nЕтап 2 / shuffle 2: ключ (PU={pu}, DO={do}), значення avg_min={k1[1] / k1[0] / 60:.4f}")
    print(f"  розділ pmod(hash(PU, DO), {P}) = {partition(P, pu, do)}")
    print(f"  підсумок маршруту: n_days={rt[0]}, mean={rt[1]:.3f} хв, std_pop={rt[2]:.3f} хв")
    if rt[0] < C.MIN_DAYS_PER_ROUTE:
        print(f"  Фінал: {rt[0]} днів < {C.MIN_DAYS_PER_ROUTE} - маршрут не входить у результат.")
        return
    rank = con.execute(f"""SELECT rk FROM (SELECT pu, do_, rank() OVER (ORDER BY cv DESC) rk FROM result)
                           WHERE pu={pu} AND do_={do}""").fetchone()[0]
    total = con.execute("SELECT count(*) FROM result").fetchone()[0]
    print(f"  Фінал: cv = {rt[2] / rt[1]:.4f}, місце {rank} з {total} за CV.")


if __name__ == "__main__":
    main()
