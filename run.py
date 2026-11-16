#!/usr/bin/env python3
"""Єдина точка входу конвеєра.

    docker compose run --rm pipeline                         # вибірка, 32 розділи
    docker compose run --rm pipeline --input full --save-daily --label full
    docker compose run --rm --service-ports pipeline --hold-ui   # Spark UI на localhost:4040 протягом 120 с

Усі аргументи передаються в pipeline/taxi_pipeline.py (див. --help).
"""
import os
import subprocess
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "scripts"))
from submit import spark_submit_cmd  # noqa: E402

if __name__ == "__main__":
    cmd = spark_submit_cmd(sys.argv[1:])
    print(" ".join(cmd), flush=True)
    sys.exit(subprocess.call(cmd))
