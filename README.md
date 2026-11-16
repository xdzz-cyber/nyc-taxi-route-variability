# Мінливість тривалості поїздок між зонами Нью-Йорка за днями (PySpark)

Фінальний проєкт з Big Data. Дані - NYC TLC Yellow Taxi, січень-червень 2024
(20 332 093 записи, 2,19 ГБ CSV). Конвеєр з двома shuffle: підсумок за (дата, маршрут) →
відбір днів із ≥ 30 поїздками → мінливість денних середніх за маршрутом (mean, std_pop, CV).

* **Звіт:** [`docs/report.pdf`](docs/report.pdf) · **слайди:** [`docs/slides.pdf`](docs/slides.pdf) ([`.pptx`](docs/slides.pptx))
* Алгоритм, ключі, правила очищення: [`docs/algorithm.md`](docs/algorithm.md)
* Оцінки до запуску: [`docs/estimates.md`](docs/estimates.md)
* Оцінка проти факту, карта фаз: `docs/phase_map_full.md`
* Перекіс: `docs/skew_full.md`; експеримент: план [`docs/experiment_plan.md`](docs/experiment_plan.md) (до замірів) і результат [`docs/experiment.md`](docs/experiment.md)
* Докази повного запуску й експерименту: `evidence/` (журнали подій для History Server, `verify.txt`, `runs.csv`)

## Вимоги

Лише Docker (Docker Desktop з WSL2 на Windows). Python, Spark, Java - усередині образу
`apache/spark:3.5.3-scala2.12-java17-python3-ubuntu` (версія зафіксована в `docker/Dockerfile`).

Перед першим запуском (з інтернетом): `docker compose build`. Далі все працює офлайн.

## Запуск на вибірці (≈ 1 хв) - одна команда

```
docker compose run --rm pipeline
```

Очікуваний результат: у консолі - топ-10 маршрутів за CV і `summary.json`; файли в
`out/sample/`: `result.csv` (усі маршрути з CV), `quality.json` (кількість відкинутих за
причинами + приклади рядків), `explain.txt` (фізичний план), `summary.json`. Кількість
маршрутів у результаті має дорівнювати `expected_result_routes` із `sample/SAMPLE_INFO.json`.

Перевірка тим самим обчисленням на DuckDB (незалежно від Spark):

```
docker compose run --rm tools scripts/verify_result.py --input sample --label sample
```

Spark UI під час запуску: `docker compose run --rm --service-ports pipeline --hold-ui`,
потім http://localhost:4040 (UI відкритий 120 с після завершення аналізу; інший час: `--hold-ui 300`). Журнали всіх запусків: `docker compose up -d history`,
http://localhost:18080.

## Повний набір

1. Дані в `./data` (або в папці з `DATA_DIR` у `.env`): `data/csv/yellow_tripdata_2024-0[1-6].csv`.
   Якщо їх немає - завантаження й конвертація (≈ 342 МБ Parquet → 2,19 ГБ CSV, потрібен інтернет):
   ```
   docker compose run --rm tools scripts/prepare_data.py
   ```
   Скрипт пише `data/evidence/manifest.json`; SHA-256 CSV мають збігтися з `manifest.json`
   у корені репозиторію (це паспорт даних із заявки).
2. Доказовий запуск (звіт якості + аналіз + вихід етапу 1 для перекосу):
   ```
   docker compose run --rm pipeline --input full --label full --save-daily
   docker compose run --rm tools scripts/verify_result.py --input full --label full
   docker compose run --rm tools scripts/phase_map.py --label full
   docker compose run --rm tools scripts/skew.py --label full
   ```
3. Експеримент (прогрів + 3 заміри кожного варіанта):
   ```
   docker compose run --rm tools scripts/run_experiment.py --input full --runs 3
   ```

## Допоміжні скрипти (`docker compose run --rm tools scripts/<name>.py`)

| Скрипт | Що робить |
|---|---|
| `explore.py --input full` | розвідка до реалізації: причини відкидання, розподіл тривалості, кількість ключів, модель часткової агрегації |
| `estimate.py --partitions 32 8` | таблиця оцінок shuffle → `docs/estimates.md` |
| `make_sample.py` | створює `sample/yellow_2024H1_sample.csv.gz` і `SAMPLE_INFO.json` (правило відбору - у docstring) |
| `verify_result.py` | порівнює результат Spark із DuckDB |
| `trace.py --input sample --random 7` | проводить один рядок через усі етапи: map-задача, розділи shuffle 1 і 2, денний підсумок, маршрут |
| `eventlog.py spark-events/<app-id>` | job-и, stage-і, метрики задач і фізичний план з журналу подій |
| `phase_map.py`, `skew.py`, `run_experiment.py` | карта фаз, перекіс, експеримент → `docs/` |

## Структура

```
pipeline/config.py        пороги й правила (спільні для Spark і DuckDB)
pipeline/taxi_pipeline.py конвеєр PySpark
run.py, scripts/submit.py spark-submit з фіксованими налаштуваннями (AQE off, UTC, event log)
scripts/                  розвідка, оцінки, вибірка, перевірка, метрики
sample/                   вибірка + SAMPLE_INFO.json (sha256)
spark-events/             журнали подій (History Server)
evidence/                 збережені журнали й звіти повного запуску
docs/                     алгоритм, оцінки, карта фаз, перекіс, експеримент, звіт
```

## Дані й ліцензія

NYC TLC Trip Record Data (NYC Open Data, умови використання NYC Open Data). Повний набір
у репозиторій не входить: посилання й SHA-256 у `manifest.json`.
