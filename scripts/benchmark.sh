#!/usr/bin/env bash
# Гоняет одинаковые запросы на Postgres (OLTP) и ClickHouse (OLAP) и показывает время.
# Запуск из корня проекта: ./scripts/benchmark.sh
set -euo pipefail

COMPOSE="docker compose -f $(dirname "$0")/../build/docker-compose.yaml"

pg() {
    $COMPOSE exec -T postgres psql -U app -d app -q -c '\timing on' -c "$1"
}

ch() {
    $COMPOSE exec -T clickhouse clickhouse-client --user admin --password admin \
        --format PrettyCompact --time -q "$1" 2>&1 \
        | sed -E 's/^([0-9]+\.[0-9]+)$/Time: \1 s/'
}

run() {
    local title=$1 pg_query=$2 ch_query=$3
    echo
    echo "=================================================================="
    echo "  $title"
    echo "=================================================================="
    echo "--- Postgres:   $pg_query"
    pg "$pg_query"
    echo "--- ClickHouse: $ch_query"
    ch "$ch_query"
}

run "1. Сколько людей в каждой стране и их средний возраст (OLAP)" \
    "SELECT country, count(*) AS people, round(avg(age), 1) AS avg_age FROM people GROUP BY country ORDER BY people DESC;" \
    "SELECT country, count() AS people, round(avg(age), 1) AS avg_age FROM people GROUP BY country ORDER BY people DESC"

run "2. Новые пользователи по месяцам (OLAP)" \
    "SELECT date_trunc('month', created_at) AS month, count(*) AS people FROM people GROUP BY month ORDER BY month;" \
    "SELECT toStartOfMonth(created_at) AS month, count() AS people FROM people GROUP BY month ORDER BY month"

run "3. Найти одного человека по id (OLTP — тут Postgres должен выиграть)" \
    "SELECT * FROM people WHERE id = 4242424;" \
    "SELECT * FROM people WHERE id = 4242424"
