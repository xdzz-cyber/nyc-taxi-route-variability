"""Ті самі правила очищення й етапи, що в Spark, але на DuckDB (SQL).

Навіщо: незалежна перевірка результату Spark (verify_result.py), розвідка даних для оцінок
(explore.py), створення вибірки (make_sample.py) і трасування рядка (trace.py).
Пороги беруться з pipeline/config.py - тим самим файлом, що й у Spark.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "pipeline"))
import config as C  # noqa: E402,F401


def connect(memory_limit="4GB"):
    import duckdb
    con = duckdb.connect()
    con.execute(f"SET memory_limit='{memory_limit}'")
    con.execute("SET preserve_insertion_order=true")
    con.execute("SET temp_directory='/tmp/duckdb-tmp'")
    return con


def raw_source(path):
    """Читання CSV як рядків (як у Spark: RAW_SCHEMA зі StringType)."""
    types = ", ".join(f"'{c}': 'VARCHAR'" for c in C.COLUMNS)
    return (f"read_csv('{path}', header=true, columns={{{types}}}, "
            f"filename=true, auto_detect=false, delim=',', quote='\"')")


def classified_sql(source):
    """SELECT з полями trip_date, PU, DO, duration_sec, status, dst_corrected + сирі поля."""
    gaps = []
    crosses = []
    for d in C.DST_SPRING_DAYS:
        g0, g1 = f"TIMESTAMP '{d} 02:00:00'", f"TIMESTAMP '{d} 03:00:00'"
        gaps.append(f"(pickup_ts >= {g0} AND pickup_ts < {g1}) OR (dropoff_ts >= {g0} AND dropoff_ts < {g1})")
        crosses.append(f"(pickup_ts < {g0} AND dropoff_ts >= {g1})")
    in_gap = " OR ".join(gaps) or "false"
    cross = " OR ".join(crosses) or "false"
    neg = "amount < 0" if C.DROP_NEGATIVE_AMOUNT else "false"
    return f"""
WITH parsed AS (
  SELECT *,
    TRY_CAST(tpep_pickup_datetime AS TIMESTAMP)  AS pickup_ts,
    TRY_CAST(tpep_dropoff_datetime AS TIMESTAMP) AS dropoff_ts,
    -- як Spark cast(string as int): дробова частина відкидається
    TRY_CAST(trunc(TRY_CAST(PULocationID AS DOUBLE)) AS INTEGER) AS pu,
    TRY_CAST(trunc(TRY_CAST(DOLocationID AS DOUBLE)) AS INTEGER) AS do_,
    TRY_CAST(total_amount AS DOUBLE) AS amount
  FROM {source}
), d AS (
  SELECT *,
    -- як Spark unix_timestamp(): секунди з відкиданням дробової частини
    CAST(epoch(date_trunc('second', dropoff_ts)) AS BIGINT)
      - CAST(epoch(date_trunc('second', pickup_ts)) AS BIGINT) AS wall_dur,
    coalesce({cross}, false) AS dst_corrected,
    coalesce({in_gap}, false) AS in_gap
  FROM parsed
), c AS (
  SELECT *,
    CASE WHEN dst_corrected THEN wall_dur - 3600 ELSE wall_dur END AS duration_sec,
    CAST(pickup_ts AS DATE) AS trip_date
  FROM d
)
SELECT *,
  CASE
    WHEN pickup_ts IS NULL OR dropoff_ts IS NULL OR pu IS NULL OR do_ IS NULL THEN 'missing_required'
    WHEN pickup_ts < TIMESTAMP '{C.PERIOD_START}' OR pickup_ts >= TIMESTAMP '{C.PERIOD_END_EXCL}' THEN 'out_of_period'
    WHEN pu NOT BETWEEN {C.ZONE_MIN} AND {C.ZONE_MAX} OR do_ NOT BETWEEN {C.ZONE_MIN} AND {C.ZONE_MAX} THEN 'unknown_zone'
    WHEN coalesce({neg}, false) THEN 'negative_amount'
    WHEN in_gap THEN 'dst_gap'
    WHEN duration_sec <= 0 THEN 'non_positive_duration'
    WHEN duration_sec < {C.MIN_DURATION_SEC} THEN 'too_short'
    WHEN duration_sec > {C.MAX_DURATION_SEC} THEN 'too_long'
    ELSE '{C.ACCEPTED}'
  END AS status
FROM c
"""


def stages_sql(classified_table):
    """daily_all, daily (відібрані дні) і result - так само, як у Spark."""
    return {
        "daily_all": f"""
            SELECT trip_date, pu, do_, count(*) AS n_trips, sum(duration_sec) AS sum_sec
            FROM {classified_table} WHERE status = '{C.ACCEPTED}'
            GROUP BY trip_date, pu, do_""",
        "daily": f"""
            SELECT *, sum_sec / n_trips / 60.0 AS avg_min FROM daily_all
            WHERE n_trips >= {C.MIN_TRIPS_PER_DAY}""",
        "result": f"""
            SELECT pu, do_, count(*) AS n_days, avg(avg_min) AS mean_daily_min,
                   stddev_pop(avg_min) AS std_daily_min,
                   stddev_pop(avg_min) / avg(avg_min) AS cv
            FROM daily GROUP BY pu, do_ HAVING count(*) >= {C.MIN_DAYS_PER_ROUTE}""",
    }
