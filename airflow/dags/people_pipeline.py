"""Postgres (БД) -> S3/MinIO (озеро, Parquet) -> ClickHouse (хранилище).

Airflow здесь только оркестратор: он не таскает данные сам, а по порядку
вызывает шаги пайплайна в Go-сервисе и передаёт между ними ключ файла (XCom).
"""

import json
import os
from datetime import datetime, timedelta, timezone
from urllib.request import Request, urlopen

from airflow.sdk import dag, task

PIPELINE_URL = os.environ.get("PIPELINE_URL", "http://pipeline:8088")
# Чуть больше, чем таймаут на стороне Go-сервиса (10 минут).
TIMEOUT_SECONDS = 11 * 60


def post(path: str, body: dict | None = None) -> dict:
    data = json.dumps(body).encode() if body is not None else None
    request = Request(
        PIPELINE_URL + path,
        data=data,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    with urlopen(request, timeout=TIMEOUT_SECONDS) as response:
        return json.loads(response.read())


@dag(
    dag_id="people_pipeline",
    # Запускаем вручную кнопкой в UI — удобнее для демо.
    # Для регулярной выгрузки можно поставить, например, "*/15 * * * *".
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(seconds=10)},
    tags=["postgres", "s3", "clickhouse"],
)
def people_pipeline():

    @task
    def extract_to_lake() -> str:
        """Postgres -> Parquet-файл в S3. Возвращает ключ файла."""
        result = post("/export/s3")
        print(f"Uploaded {result['key']}, rows={result['rows']}")
        return result["key"]

    @task
    def load_to_warehouse(key: str) -> None:
        """Parquet-файл из S3 -> ClickHouse."""
        result = post("/load/clickhouse", {"key": key})
        print(f"Loaded {result['key']} into ClickHouse, rows={result['rows']}")

    load_to_warehouse(extract_to_lake())


people_pipeline()
