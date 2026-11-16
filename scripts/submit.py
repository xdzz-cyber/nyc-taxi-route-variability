"""Збирає команду spark-submit з однаковими для всіх запусків налаштуваннями."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "pipeline"))
import config as C  # noqa: E402

# Налаштування, які фіксуємо для всіх запусків (вони потрапляють і в журнал подій).
SPARK_CONF = {
    "spark.eventLog.enabled": "true",
    "spark.eventLog.dir": f"file://{C.EVENT_DIR}",
    # AQE вимкнено: інакше Spark сам змінює кількість розділів shuffle (див. заявку, крок 5).
    "spark.sql.adaptive.enabled": "false",
    # Час TLC - локальний час Нью-Йорка без поясу. UTC-сесія = арифметика "за годинником".
    "spark.sql.session.timeZone": "UTC",
    "spark.local.dir": "/tmp/spark-local",
    "spark.ui.showConsoleProgress": "true",
}


def spark_submit_cmd(pipeline_args, driver_memory=None, master=None):
    driver_memory = driver_memory or os.environ.get("DRIVER_MEMORY", "4g")
    master = master or os.environ.get("SPARK_MASTER", "local[*]")
    cmd = ["spark-submit", "--master", master, "--driver-memory", driver_memory]
    for k, v in SPARK_CONF.items():
        cmd += ["--conf", f"{k}={v}"]
    cmd.append(os.path.join(C.APP_ROOT, "pipeline", "taxi_pipeline.py"))
    return cmd + list(pipeline_args)
