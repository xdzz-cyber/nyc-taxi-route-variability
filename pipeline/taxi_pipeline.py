"""Мінливість середньої тривалості поїздок між зонами Нью-Йорка за днями.

Конвеєр:
  0. Очищення (без shuffle):      рядок CSV -> (trip_date, PU, DO, duration_sec) або причина відкидання
  1. Підсумок за день (shuffle 1): ключ (trip_date, PU, DO) -> n_trips, sum_sec
     Відбір днів (без shuffle):     n_trips >= 30, avg_min = sum_sec / n_trips / 60
  2. Порівняння днів (shuffle 2):  ключ (PU, DO) -> n_days, mean(avg_min), stddev_pop(avg_min)
     Фінал (без shuffle):           n_days >= 30, cv = std / mean

Запуск - через run.py (він збирає spark-submit з потрібними налаштуваннями).
"""
import argparse
import csv
import json
import os
import time

from pyspark.sql import SparkSession, functions as F, types as T

import config as C

# Усі 19 полів читаємо як рядки (див. config.COLUMNS).
RAW_SCHEMA = T.StructType([T.StructField(c, T.StringType(), True) for c in C.COLUMNS])
RAW_KEEP = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID", "DOLocationID",
            "trip_distance", "total_amount"]


def read_raw(spark, path):
    # Явна схема: Spark не робить окремого проходу для inferSchema (зайвий job на 2,2 ГБ).
    return spark.read.csv(path, header=True, schema=RAW_SCHEMA, mode="PERMISSIVE")


def classify(raw):
    """Крок очищення. Повертає по одному рядку на вхідний запис зі стовпцем status.

    Вузькі перетворення (map-side): жодного shuffle тут немає.
    """
    df = raw.select(
        # сирі рядки - лише для прикладів у звіті якості; головний job їх не читає
        # (оптимізатор прибирає непотрібні стовпці зі сканування CSV)
        *[F.col(c).alias(f"raw_{c}") for c in RAW_KEEP],
        F.col("tpep_pickup_datetime").cast("timestamp").alias("pickup_ts"),
        F.col("tpep_dropoff_datetime").cast("timestamp").alias("dropoff_ts"),
        # cast рядка в int відкидає дробову частину ("132.0" -> 132), порожній рядок -> null
        F.col("PULocationID").cast("int").alias("PULocationID"),
        F.col("DOLocationID").cast("int").alias("DOLocationID"),
        F.col("total_amount").cast("double").alias("total_amount"),
    )

    # Сесія Spark працює в UTC, тому різниця - це різниця "за годинником" (як записано в TLC).
    wall_dur = F.unix_timestamp("dropoff_ts") - F.unix_timestamp("pickup_ts")

    in_gap = F.lit(False)
    crosses_gap = F.lit(False)
    for day in C.DST_SPRING_DAYS:
        g0 = F.lit(f"{day} 02:00:00").cast("timestamp")
        g1 = F.lit(f"{day} 03:00:00").cast("timestamp")
        in_gap = in_gap | ((F.col("pickup_ts") >= g0) & (F.col("pickup_ts") < g1)) \
                        | ((F.col("dropoff_ts") >= g0) & (F.col("dropoff_ts") < g1))
        crosses_gap = crosses_gap | ((F.col("pickup_ts") < g0) & (F.col("dropoff_ts") >= g1))

    df = (df
          .withColumn("dst_corrected", F.coalesce(crosses_gap, F.lit(False)))
          # Виправлення: поїздка через 02:00->03:00 насправді на 3600 с коротша.
          .withColumn("duration_sec",
                      F.when(F.col("dst_corrected"), wall_dur - 3600).otherwise(wall_dur))
          .withColumn("trip_date", F.to_date("pickup_ts")))

    start = F.lit(C.PERIOD_START).cast("timestamp")
    end = F.lit(C.PERIOD_END_EXCL).cast("timestamp")
    pu, do, dur = F.col("PULocationID"), F.col("DOLocationID"), F.col("duration_sec")

    checks = {
        "missing_required": F.col("pickup_ts").isNull() | F.col("dropoff_ts").isNull()
                            | pu.isNull() | do.isNull(),
        "out_of_period": (F.col("pickup_ts") < start) | (F.col("pickup_ts") >= end),
        "unknown_zone": ~pu.between(C.ZONE_MIN, C.ZONE_MAX) | ~do.between(C.ZONE_MIN, C.ZONE_MAX),
        "negative_amount": (F.col("total_amount") < 0) if C.DROP_NEGATIVE_AMOUNT else F.lit(False),
        "dst_gap": in_gap,
        "non_positive_duration": dur <= 0,
        "too_short": dur < C.MIN_DURATION_SEC,
        "too_long": dur > C.MAX_DURATION_SEC,
    }
    status = None
    for reason in C.REASONS:  # перша причина, що спрацювала
        cond = checks[reason]
        status = F.when(cond, reason) if status is None else status.when(cond, reason)
    return df.withColumn("status", status.otherwise(C.ACCEPTED))


def build_stages(clean):
    """Повертає (daily_all, result). daily_all - вихід етапу 1 до відбору днів."""
    accepted = (clean.filter(F.col("status") == C.ACCEPTED)
                     .select("trip_date", "PULocationID", "DOLocationID", "duration_sec"))

    # ---- Етап 1, SHUFFLE 1: ключ (trip_date, PU, DO) ----
    # count і sum - асоціативні, тому Spark рахує їх частково в кожній map-задачі
    # (HashAggregate partial = combiner) і передає в shuffle лише підсумки ключів задачі.
    daily_all = (accepted
                 .groupBy("trip_date", "PULocationID", "DOLocationID")
                 .agg(F.count(F.lit(1)).alias("n_trips"),
                      F.sum("duration_sec").alias("sum_sec")))

    # Відбір днів: фільтр по агрегату (HAVING) - у тому ж stage, що й фінал етапу 1.
    daily = (daily_all
             .filter(F.col("n_trips") >= C.MIN_TRIPS_PER_DAY)
             .withColumn("avg_min", F.col("sum_sec") / F.col("n_trips") / 60.0))

    # ---- Етап 2, SHUFFLE 2: ключ (PU, DO), дата вже не в ключі ----
    # Розбиття після shuffle 1 - hash(trip_date, PU, DO); воно НЕ групує разом дні одного
    # маршруту, тому Spark зобов'язаний вставити новий Exchange hashpartitioning(PU, DO).
    # count/avg/stddev_pop теж мають часткові буфери: (n), (sum, n), (n, mean, M2).
    routes = (daily
              .groupBy("PULocationID", "DOLocationID")
              .agg(F.count(F.lit(1)).alias("n_days"),
                   F.avg("avg_min").alias("mean_daily_min"),
                   # stddev_pop: як у прикладі заявки (10, 20, 30 хв -> 8,16 хв)
                   F.stddev_pop("avg_min").alias("std_daily_min")))

    result = (routes
              .filter(F.col("n_days") >= C.MIN_DAYS_PER_ROUTE)
              .withColumn("cv", F.col("std_daily_min") / F.col("mean_daily_min")))
    return daily_all, result


def quality_report(clean, n_examples=3):
    """Звіт якості: два невеликі job-и поза головним аналізом.

    1) кількість записів за статусом - groupBy(status) з HashAggregate (лише лічильники);
    2) приклади відкинутих рядків: n_examples на причину, детерміновано (найменший хеш рядка).
       Через shuffle іде лише невелика частка відкинутих рядків, а не всі 20 млн.
    min_by(struct, ...) тут не підходить: буфер зі struct змушує Spark перейти на
    SortAggregate і сортувати всі рядки в кожній map-задачі.
    """
    from pyspark.sql import Window

    counts = (clean.groupBy("status")
                   .agg(F.count(F.lit(1)).alias("n"),
                        F.sum(F.col("dst_corrected").cast("int")).alias("dst_corrected"))
                   .collect())
    out = {r["status"]: {"n": r["n"], "dst_corrected": r["dst_corrected"] or 0, "examples": []}
           for r in counts}

    raw_cols = [f"raw_{c}" for c in RAW_KEEP]
    h = F.xxhash64(*[F.col(c) for c in raw_cols])
    w = Window.partitionBy("status").orderBy("h")
    examples = (clean.filter(F.col("status") != C.ACCEPTED)
                     .withColumn("h", h)
                     .withColumn("rn", F.row_number().over(w))
                     .filter(F.col("rn") <= n_examples)
                     .select("status", "duration_sec", *raw_cols)
                     .collect())
    examples += (clean.filter(F.col("status") == C.ACCEPTED)
                      .select("status", "duration_sec", *raw_cols).limit(n_examples).collect())
    for r in examples:
        d = r.asDict()
        st = d.pop("status")
        out[st]["examples"].append({k.replace("raw_", ""): v for k, v in d.items()})
    return out


def load_zone_names(path):
    names = {}
    if os.path.exists(path):
        with open(path, newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                names[int(r["LocationID"])] = f'{r["Borough"]} / {r["Zone"]}'
    return names


def explain_text(df):
    sc = df.sparkSession.sparkContext
    return sc._jvm.PythonSQLUtils.explainString(df._jdf.queryExecution(), "formatted")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="sample", help="sample | full | шлях або шаблон CSV")
    ap.add_argument("--partitions", type=int, default=32, help="spark.sql.shuffle.partitions")
    ap.add_argument("--label", help="мітка запуску = папка в out/ (за замовчуванням sample/full/run)")
    ap.add_argument("--no-quality", action="store_true", help="не рахувати звіт якості")
    ap.add_argument("--save-daily", action="store_true",
                    help="додатково записати вихід етапу 1 (для аналізу перекосу)")
    ap.add_argument("--hold-ui", action="store_true",
                    help="не закривати Spark UI (localhost:4040) до натискання Enter")
    args = ap.parse_args()

    in_path = C.resolve_input(args.input)
    if not args.label:
        args.label = args.input if args.input in ("sample", "full") else "run"
    out_dir = os.path.join(C.OUT_DIR, args.label)
    os.makedirs(out_dir, exist_ok=True)

    spark = (SparkSession.builder
             .appName(f"taxi-route-variability:{args.label}")
             .config("spark.sql.shuffle.partitions", args.partitions)
             .getOrCreate())
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    summary = {"label": args.label, "input": in_path, "partitions": args.partitions,
               "app_id": sc.applicationId, "default_parallelism": sc.defaultParallelism,
               "timings_s": {}}

    clean = classify(read_raw(spark, in_path))

    if not args.no_quality:
        sc.setJobGroup("quality", "Звіт якості: кількість за причинами + приклади")
        t = time.time()
        q = quality_report(clean)
        summary["timings_s"]["quality"] = round(time.time() - t, 2)
        total = sum(v["n"] for v in q.values())
        summary["quality"] = {"total_rows": total,
                              "by_status": {k: v["n"] for k, v in sorted(q.items())},
                              "dst_corrected_accepted": q.get(C.ACCEPTED, {}).get("dst_corrected", 0)}
        with open(os.path.join(out_dir, "quality.json"), "w", encoding="utf-8") as f:
            json.dump(q, f, ensure_ascii=False, indent=2, default=str)

    daily_all, result = build_stages(clean)
    with open(os.path.join(out_dir, "explain.txt"), "w", encoding="utf-8") as f:
        f.write(explain_text(result))

    # ---- Головний job: один collect() = stage A -> shuffle 1 -> stage B -> shuffle 2 -> stage C.
    # Результат малий (тисячі маршрутів), тому сортуємо й пишемо у драйвері: без orderBy
    # немає зайвого range-shuffle, і всі stage-і залежать лише від spark.sql.shuffle.partitions.
    sc.setJobGroup("analysis", "Етап 1 + етап 2: мінливість денної тривалості маршрутів")
    t = time.time()
    rows = result.collect()
    summary["timings_s"]["analysis"] = round(time.time() - t, 2)
    summary["result_routes"] = len(rows)

    names = load_zone_names(C.ZONE_LOOKUP)
    rows = sorted(rows, key=lambda r: (-(r["cv"] or 0.0), r["PULocationID"], r["DOLocationID"]))
    with open(os.path.join(out_dir, "result.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["PULocationID", "DOLocationID", "pickup_zone", "dropoff_zone", "n_days",
                    "mean_daily_min", "std_daily_min", "cv"])
        for r in rows:
            w.writerow([r["PULocationID"], r["DOLocationID"],
                        names.get(r["PULocationID"], ""), names.get(r["DOLocationID"], ""),
                        r["n_days"], repr(r["mean_daily_min"]), repr(r["std_daily_min"]),
                        repr(r["cv"])])

    if args.save_daily:
        # Окремий job: повторно рахує етап 1 і пише всі ключі (дата, маршрут) до відбору.
        sc.setJobGroup("save-daily", "Вихід етапу 1 для аналізу перекосу")
        t = time.time()
        daily_all.write.mode("overwrite").parquet(os.path.join(out_dir, "daily_all.parquet"))
        summary["timings_s"]["save_daily"] = round(time.time() - t, 2)

    with open(os.path.join(out_dir, "summary.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print("\n=== Топ-10 маршрутів за CV ===")
    for r in rows[:10]:
        print(f'{r["PULocationID"]:>3} -> {r["DOLocationID"]:>3}  днів={r["n_days"]:>3}  '
              f'середнє={r["mean_daily_min"]:.2f} хв  std={r["std_daily_min"]:.2f}  cv={r["cv"]:.3f}  '
              f'{names.get(r["PULocationID"], "")} -> {names.get(r["DOLocationID"], "")}')
    print(json.dumps(summary, ensure_ascii=False, indent=2))

    if args.hold_ui:
        input("Spark UI: http://localhost:4040 . Натисніть Enter, щоб завершити...")
    spark.stop()


if __name__ == "__main__":
    main()
