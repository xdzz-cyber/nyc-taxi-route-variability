# Spark 3.5.3 + Java 17 + Python 3 (офіційний образ Apache Spark), версія зафіксована тегом.
# Після першого `docker compose build` можна закріпити ще й digest:
#   docker image inspect apache/spark:3.5.3-scala2.12-java17-python3-ubuntu --format '{{index .RepoDigests 0}}'
FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu

USER root
# DuckDB - допоміжні скрипти (розвідка, вибірка, незалежна перевірка);
# pyarrow 19.0.1 - конвертація Parquet -> CSV (scripts/prepare_data.py), та сама версія, що для заявки.
# Встановлюються під час збирання образу, тому запуск працює без інтернету.
RUN pip3 install --no-cache-dir duckdb==1.1.3 pyarrow==19.0.1

ENV PATH="${PATH}:/opt/spark/bin:/opt/spark/sbin" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    SPARK_NO_DAEMONIZE=true
COPY log4j2.properties /opt/spark/conf/log4j2.properties
WORKDIR /app
