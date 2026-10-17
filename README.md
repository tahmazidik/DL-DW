# Database vs Data Lake vs Data Warehouse

```
Postgres (БД, OLTP) ──► Go-сервис ──► MinIO/S3 (озеро, Parquet) ──► ClickHouse (хранилище, OLAP) ──► Grafana
                         ▲                                              ▲
                         └──────────── Airflow (оркестратор) ───────────┘
```

| Компонент  | Роль в докладе | Где в коде |
|------------|----------------|------------|
| Postgres   | База данных: OLTP, schema-on-write | `internal/database`, `build/sql/postgres_init.sql` |
| MinIO (S3) | Озеро: сырые файлы в Parquet, schema-on-read | `internal/lake` |
| ClickHouse | Хранилище: колоночная OLAP-база | `internal/warehouse`, `build/sql/clickhouse_init.sql` |
| Airflow    | Оркестратор: запускает шаги по порядку | `airflow/dags/people_pipeline.py` |
| Grafana    | Дашборды поверх ClickHouse | `build/dashboards` |

## Запуск

Все команды выполняются из корня проекта.

```bash
docker compose -f build/docker-compose.yaml up -d --build
```

| Что | Адрес | Логин |
|-----|-------|-------|
| Go-сервис | http://localhost:8088/health | — |
| Airflow | http://localhost:8080 | без логина |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin123 |
| Adminer (Postgres) | http://localhost:8081 | сервер `postgres`, app / password, БД `app` |
| ClickHouse HTTP | http://localhost:8123/play | admin / admin |
| Grafana | http://localhost:3000 | admin / admin |

Остановить: `docker compose -f build/docker-compose.yaml down`
Остановить и удалить все данные: `... down -v`

## Сценарий демо

**1. Заполняем базу данных.** Генерируем 10 млн людей (около 20 секунд). Скрипт **сначала очищает** таблицу `people`. Данные специально неравномерные: популярные имена встречаются чаще, страны разного размера, регистраций с каждым месяцем больше.

```bash
docker compose -f build/docker-compose.yaml exec postgres \
  psql -U app -d app -v rows=10000000 -f /sql/seed_postgres.sql
```

**2. Schema-on-write.** База не принимает кривые данные:

```bash
curl -X POST localhost:8088/people -d '{"name": "Вольф", "country": "Russia", "age": 25}'
curl -X POST localhost:8088/people -d '{"name": "", "country": "Russia", "age": -5}'   # 400
```

**3. Запускаем пайплайн.** В Airflow (http://localhost:8080) открываем DAG `people_pipeline` и нажимаем Trigger.
Он выполняет две задачи:
- `extract_to_lake`: Postgres → `s3://data-lake/raw/people/dt=YYYY-MM-DD/people_HHMMSS.parquet`
- `load_to_warehouse`: ClickHouse сам читает этот Parquet из S3 (функция `s3()`) и загружает в `analytics.people`

Файл можно посмотреть в MinIO Console. Он заметно меньше таблицы в Postgres: колоночный формат плюс сжатие.

То же самое без Airflow:

```bash
curl -X POST localhost:8088/export/s3
curl -X POST localhost:8088/load/clickhouse -d '{"key": "<key из ответа выше>"}'
```

**4. Schema-on-read.** ClickHouse читает файл прямо из озера, схему выводит сам:

```sql
-- http://localhost:8123/play
DESCRIBE s3('http://minio:9000/data-lake/raw/people/*/*.parquet', 'minioadmin', 'minioadmin123', 'Parquet');
SELECT country, count() FROM s3('http://minio:9000/data-lake/raw/people/*/*.parquet', 'minioadmin', 'minioadmin123', 'Parquet') GROUP BY country;
```

`DESCRIBE` покажет, что ClickHouse сам вывел типы колонок из файла, а ещё добавил колонку `dt`, взяв её из пути `dt=2026-10-17/`.

**5. Сравниваем скорость.**

```bash
./scripts/benchmark.sh
```

Замер на 10 млн строк:

| Запрос | Postgres | ClickHouse |
|--------|----------|------------|
| `GROUP BY country` | ~380 мс | ~19 мс |
| `GROUP BY month` | ~1600 мс | ~15 мс |
| `WHERE id = ...` | ~2 мс | ~23 мс |

Аналитические запросы (`GROUP BY` по всей таблице) ClickHouse выполняет в десятки раз быстрее.
Поиск одной строки по `id` быстрее в Postgres: у него индекс по первичному ключу. Это и есть разница между OLTP и OLAP.

**6. Дашборды.** Grafana → папка *People Analytics* → дашборд *People Analytics*: график регистраций по дням, статистика по странам, топ-10 имён.
