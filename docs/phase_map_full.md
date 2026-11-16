# Карта фаз запуску `full` (app local-1794847024536, P = 32)

Job `analysis` = job 3; stage-і [5, 6, 7]. Час job-а: 30.7 с; усього застосунку: 123.1 с.

| Етап | Оцінка | Факт | Факт / оцінка | Stage / job | Рядок коду |
|---|---|---|---|---|---|
| 0. Очищення (читання) | 20,332,093 записів, 2,194,482,008 Б, 17 задач | 20,332,093 записів, 2,195,268,440 Б, 17 задач | 1.00× | stage 5 / job 3 | `read_raw`, `classify` |
| 1. Shuffle 1: (trip_date, PU, DO) | 1,765,216 записів; 87.5 MiB без стиснення | 1,761,900 записів; 28.2 MiB (lz4) | записи 1.00×, байти 0.32× | write: stage 5; read: stage 6 | taxi_pipeline.py:100 `groupBy(trip_date, PU, DO)` |
| 2. Shuffle 2: (PU, DO) | ≈ 43,420 записів; 3.1 MiB без стиснення | 43,404 записів; 1.6 MiB (lz4) | записи 1.00×, байти 0.52× | write: stage 6; read: stage 7 | taxi_pipeline.py:114 `groupBy(PU, DO)` |

Відбір днів (`filter(n_trips >= 30)`) і фінальний фільтр не дають shuffle: це Filter у stage B і C.
Фінальна кількість маршрутів: 1303 (оцінка 1303).

## Stage-і job-а analysis

| Stage | Роль | Задач | Час, с | Input | Shuffle write | Shuffle read | Задача: час min / медіана / max, мс |
|---|---|---|---|---|---|---|---|
| 5 | A (scan + partial agg -> shuffle) | 17 | 29.70 | 2.0 GiB | 1,761,900 / 28.2 MiB | 0 / 0 B | 9664 / 23668 / 24018 |
| 6 | B (shuffle read -> shuffle write) | 32 | 0.73 | 0 B | 43,404 / 1.6 MiB | 1,761,900 / 28.2 MiB | 119 / 312 / 581 |
| 7 | C (shuffle read -> result) | 32 | 0.24 | 0 B | 0 / 0 B | 43,404 / 1.6 MiB | 48 / 96 / 178 |

## Фізичний план (EXPLAIN FORMATTED)

```
== Physical Plan ==
* Project (14)
+- * Filter (13)
   +- * HashAggregate (12)
      +- Exchange (11)
         +- * HashAggregate (10)
            +- * Project (9)
               +- * Filter (8)
                  +- * HashAggregate (7)
                     +- Exchange (6)
                        +- * HashAggregate (5)
                           +- * Project (4)
                              +- * Project (3)
                                 +- * Filter (2)
                                    +- Scan csv  (1)
```

Повний план з усіма вузлами: `out/full/explain.txt`.