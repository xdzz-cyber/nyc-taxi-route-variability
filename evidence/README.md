# Докази повного запуску

Скопіюйте сюди після доказового запуску (`--input full --label full --save-daily`):

* `spark-events/<app-id>` повного запуску і всіх запусків експерименту (для History Server:
  `docker compose up -d history` читає `spark-events/`, ці копії - архів для здачі);
* `out/full/summary.json`, `quality.json`, `explain.txt`, `result.csv`;
* `out/explore/explore.json`, `out/experiment/runs.csv`;
* вивід `verify_result.py --input full --label full` (текстовий файл).

Журнали подій можуть бути великими - за потреби стисніть у zip.
