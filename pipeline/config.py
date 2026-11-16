"""Спільні константи проєкту.

Один файл використовують і Spark-конвеєр (pipeline/taxi_pipeline.py), і незалежні
перевірки на DuckDB (scripts/*). Тому правила очищення і пороги визначені тут один раз.
"""

# Період аналізу: дата поїздки = дата початку (tpep_pickup_datetime), [початок, кінець).
PERIOD_START = "2024-01-01 00:00:00"
PERIOD_END_EXCL = "2024-07-01 00:00:00"

# Коректні зони TLC: 1..263. 264 = Unknown, 265 = Outside of NYC (див. taxi_zone_lookup.csv).
ZONE_MIN, ZONE_MAX = 1, 263

# Межі правдоподібної тривалості, секунди. Обґрунтування - у docs/algorithm.md
# (на основі процентилів із scripts/explore.py).
MIN_DURATION_SEC = 60
MAX_DURATION_SEC = 3 * 60 * 60

# Записи з від'ємною сумою - сторнування/коригування платежу, вони дублюють поїздку.
DROP_NEGATIVE_AMOUNT = True

# Перехід на літній час у Нью-Йорку в межах періоду. О 02:00 годинник переводять на 03:00:
# часу 02:00-02:59 не існує, а поїздка через цей проміжок за годинником довша на 1 год.
DST_SPRING_DAYS = ["2024-03-10"]

# Пороги задачі (із заявки).
MIN_TRIPS_PER_DAY = 30   # день маршруту враховується, якщо в ньому >= 30 поїздок
MIN_DAYS_PER_ROUTE = 30  # маршрут у результаті, якщо в нього >= 30 таких днів

# Порядок перевірок: запис отримує ПЕРШУ причину, що спрацювала.
REASONS = [
    "missing_required",       # немає/не розбирається дата початку, кінця чи зона
    "out_of_period",          # дата початку поза [PERIOD_START, PERIOD_END_EXCL)
    "unknown_zone",           # зона поза 1..263
    "negative_amount",        # total_amount < 0
    "dst_gap",                # час початку/кінця у неіснуючій годині 02:00-03:00 10.03
    "non_positive_duration",  # тривалість <= 0
    "too_short",              # тривалість < MIN_DURATION_SEC
    "too_long",               # тривалість > MAX_DURATION_SEC
]
ACCEPTED = "accepted"

# 19 полів у порядку файлів TLC 2024. Читаємо ВСЕ як рядки і розбираємо явно:
# так нерозібране значення стає причиною відкидання, а не мовчазним null.
COLUMNS = [
    "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count",
    "trip_distance", "RatecodeID", "store_and_fwd_flag", "PULocationID", "DOLocationID",
    "payment_type", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
    "improvement_surcharge", "total_amount", "congestion_surcharge", "Airport_fee",
]

# Шляхи всередині контейнера (APP_ROOT/DATA_ROOT можна перевизначити для запуску без Docker).
import os as _os
APP_ROOT = _os.environ.get("APP_ROOT", "/app")
DATA_ROOT = _os.environ.get("DATA_ROOT", "/data")
SAMPLE_PATH = f"{APP_ROOT}/sample/yellow_2024H1_sample.csv.gz"
FULL_GLOB = f"{DATA_ROOT}/csv/yellow_tripdata_2024-*.csv"
ZONE_LOOKUP = f"{APP_ROOT}/ref/taxi_zone_lookup.csv"
EVENT_DIR = f"{APP_ROOT}/spark-events"
OUT_DIR = f"{APP_ROOT}/out"


def resolve_input(name):
    """'sample' | 'full' | будь-який шлях/шаблон."""
    return {"sample": SAMPLE_PATH, "full": FULL_GLOB}.get(name, name)
