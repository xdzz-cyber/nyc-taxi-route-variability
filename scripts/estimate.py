"""Таблиця оцінок shuffle ДО запуску Spark (крок 2). Вхід - out/explore/explore.json.

    docker compose run --rm tools scripts/estimate.py --partitions 32 8

Пише docs/estimates.md. Стовпці "Факт" і "Stage" порожні: їх заповнюємо після запуску
(scripts/fill_facts.py підставляє числа з журналу подій).

Модель розміру рядка shuffle (UnsafeRow): 8 Б бітової маски null (до 64 полів)
+ 8 Б на кожне поле фіксованої довжини + 4 Б довжини рядка від UnsafeRowSerializer.
  shuffle 1: trip_date, PU, DO, partial_count, partial_sum            -> 8 + 5*8 + 4 = 52 Б
  shuffle 2: PU, DO, count, avg(sum, count), stddev_pop(n, avg, m2)   -> 8 + 8*8 + 4 = 76 Б
Spark показує Shuffle Bytes Written ПІСЛЯ стиснення lz4, тому факт має бути меншим за
нестиснену оцінку; коефіцієнт стиснення - окреме припущення (див. нижче).
"""
import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from rules_sql import C  # noqa: E402

ROW1, ROW2 = 52, 76
COMPRESSION_ASSUMED = (2.0, 3.0)  # очікуваний діапазон стиснення lz4 для таких рядків


def mib(b):
    return f"{b / 1024 / 1024:,.1f} MiB"


def expected_partitions_hit(days, p):
    """Скільки різних розділів shuffle 1 (з P) містять d денних рядків одного маршруту.

    Розбиття - hash(trip_date, PU, DO) mod P, тож дні маршруту розкидані ~випадково:
    E = P * (1 - (1 - 1/P)^d). Стільки часткових підсумків маршрут дасть у shuffle 2.
    """
    return p * (1 - (1 - 1 / p) ** days)


def numbers(e, p):
    """Числові оцінки для заданого P (використовує також scripts/phase_map.py)."""
    hist = {int(k): v for k, v in e["stage2_days_hist"].items()}
    s1 = e["map_tasks"]["shuffle1_records_model"]
    s2 = sum(cnt * expected_partitions_hit(d, p) for d, cnt in hist.items())
    return {"input_records": e["total_rows"], "input_bytes": e["total_bytes"],
            "map_tasks": e["map_tasks"]["num_partitions"],
            "s1_records": s1, "s1_bytes_raw": s1 * ROW1,
            "s2_records": s2, "s2_bytes_raw": s2 * ROW2,
            "s1_keys": e["stage1_keys"]["K1_distinct_date_route"],
            "daily_after_filter": e["stage2_keys"]["daily_rows_after_filter"],
            "routes_out": e["stage2_keys"]["routes_with_min_days"]}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--explore", default=os.path.join(C.OUT_DIR, "explore", "explore.json"))
    ap.add_argument("--partitions", type=int, nargs="+", default=[32, 8])
    ap.add_argument("--out", default=os.path.join(C.APP_ROOT, "docs", "estimates.md"))
    a = ap.parse_args()
    e = json.load(open(a.explore, encoding="utf-8"))

    k1 = e["stage1_keys"]["K1_distinct_date_route"]
    s1 = e["map_tasks"]["shuffle1_records_model"]
    m = e["map_tasks"]["num_partitions"]
    acc = e["stage1_keys"]["accepted_trips"]
    daily_f = e["stage2_keys"]["daily_rows_after_filter"]
    hist = {int(k): v for k, v in e["stage2_days_hist"].items()}
    routes_any = e["stage2_keys"]["routes_with_qualifying_day"]
    routes_out = e["stage2_keys"]["routes_with_min_days"]
    lo_c, hi_c = COMPRESSION_ASSUMED

    L = [f"# Оцінки shuffle до запуску (згенеровано scripts/estimate.py)", "",
         "## Вхідні величини (scripts/explore.py, DuckDB, окремо від Spark)", "",
         f"* Файлів: {len(e['files'])}, рядків: {e['total_rows']:,}, байтів CSV: {e['total_bytes']:,} ({mib(e['total_bytes'])}).",
         f"* Прийнято після очищення: {acc:,} поїздок.",
         f"* Ядер для local[*]: {e['cores']}; maxSplitBytes = {e['map_tasks']['max_split_bytes']:,} Б "
         f"=> **{m} map-задач** у stage A (формула Spark: min(128 MiB, max(4 MiB, (байти + файли*4 MiB) / ядра)), "
         f"залишки файлів пакуються в спільні розділи). Межа 128 MiB діє для будь-якої кількості ядер "
         f"≤ {(e['total_bytes'] + len(e['files']) * 4 * 1024 * 1024) // (128 * 1024 * 1024)}, тож для такого ноутбука "
         f"кількість map-задач від ядер не залежить.",
         f"* K1 = різних ключів (дата, PU, DO): {k1:,}; маршрутів (PU, DO): {e['stage1_keys']['routes_distinct']:,}; "
         f"у середньому {e['stage1_keys']['keys_per_day_avg']:,.0f} ключів на день.",
         f"* Денних рядків із n_trips >= {C.MIN_TRIPS_PER_DAY}: {daily_f:,}; маршрутів, що мають хоч один такий день: {routes_any:,}; "
         f"маршрутів із >= {C.MIN_DAYS_PER_ROUTE} днями: {routes_out:,}.", "",
         "## Як рахуємо записи shuffle", "",
         "**Shuffle 1.** Часткова агрегація (HashAggregate partial = combiner) у кожній map-задачі "
         "залишає один запис на ключ, що зустрівся в цій задачі. Отже записів = Σ по map-задачах "
         "(різних ключів (дата, PU, DO) у задачі). Нижня межа - K1 (кожен ключ лише в одній задачі), "
         f"верхня - min(прийнятих поїздок, K1 × {m}). Модель (точна сума по розділах, зіставлених "
         f"з байтовими межами шматків) дає **{s1:,}** = {s1 / k1:.3f} × K1.",
         "",
         "**Shuffle 2.** Після shuffle 1 денні рядки маршруту лежать у розділах hash(дата, PU, DO) mod P. "
         "Кожен з P reduce-розділів дає по одному частковому підсумку на маршрут, що в ньому є. "
         "Для маршруту з d відібраними днями очікуємо P·(1 − (1 − 1/P)^d) таких розділів.", ""]

    L += ["## Таблиця етапів", "",
          "| Етап | Вхід (ключ, значення) | Вихід (ключ, значення) | Що йде в shuffle | Оцінка записів і байтів | Факт | Stage / вершина / job |",
          "|---|---|---|---|---|---|---|",
          f"| 0. Очищення | рядок CSV (19 полів-рядків) | (trip_date, PU, DO) → duration_sec, або причина відкидання | нічого (вузькі перетворення) | "
          f"читання: {e['total_rows']:,} записів, {e['total_bytes']:,} Б; вихід: {acc:,} записів | | |",
          f"| 1. Підсумок за день | (trip_date, PU, DO) → duration_sec | (trip_date, PU, DO) → (n_trips, sum_sec); далі фільтр n_trips ≥ {C.MIN_TRIPS_PER_DAY} | "
          f"часткові (trip_date, PU, DO, count, sum), {ROW1} Б/запис | "
          f"**{s1:,} записів**; {s1 * ROW1:,} Б ({mib(s1 * ROW1)}) без стиснення, ≈ {mib(s1 * ROW1 / hi_c)}–{mib(s1 * ROW1 / lo_c)} після lz4; "
          f"{m} map-задач; вихід {k1:,} ключів → {daily_f:,} після фільтра | | |"]
    for p in a.partitions:
        s2 = sum(cnt * expected_partitions_hit(d, p) for d, cnt in hist.items())
        L.append(
            f"| 2. Порівняння днів (P={p}) | (PU, DO) → avg_min | (PU, DO) → (n_days, mean, std_pop); далі n_days ≥ {C.MIN_DAYS_PER_ROUTE}, cv | "
            f"часткові (PU, DO, count, sum, count, n, avg, m2), {ROW2} Б/запис | "
            f"**≈ {s2:,.0f} записів** (≤ min({daily_f:,}, {routes_any:,}×{p})); {s2 * ROW2:,.0f} Б ({mib(s2 * ROW2)}) без стиснення; "
            f"вихід {routes_any:,} маршрутів → {routes_out:,} після фільтра | | |")
    L += ["", "## Що очікуємо побачити у фактичному плані", "",
          "* Job `analysis` (один `collect()`): stage A = Scan csv → Filter/Project → HashAggregate(partial) → Exchange "
          "hashpartitioning(trip_date, PU, DO, P); stage B = HashAggregate(final) → Filter(n_trips ≥ 30) → Project(avg_min) → "
          "HashAggregate(partial) → Exchange hashpartitioning(PU, DO, P); stage C = HashAggregate(final) → Filter(n_days ≥ 30) → Project(cv).",
          f"* Stage A: {m} задач; stage B і C: по P задач (P = spark.sql.shuffle.partitions, AQE вимкнено).",
          "* Другий Exchange обов'язковий: розбиття hash(trip_date, PU, DO) не гарантує, що всі дні одного маршруту "
          "в одному розділі, тож вимогу ClusteredDistribution(PU, DO) воно не задовольняє.",
          "* `spark.sql.shuffle.partitions` не змінює stage A (його задачі = розбиття CSV). Від P залежить кількість задач B і C "
          "та обсяг shuffle 2 (у ~P разів більше часткових підсумків на частий маршрут).",
          "* Окремі job-и `quality` (звіт якості, свої маленькі shuffle) і `save-daily` у таблицю етапів не входять.", ""]
    os.makedirs(os.path.dirname(a.out), exist_ok=True)
    open(a.out, "w", encoding="utf-8").write("\n".join(L))
    print("\n".join(L))


if __name__ == "__main__":
    main()
