-- Генерирует тестовых людей в Postgres, чтобы разница в скорости была заметна.
-- Запуск (по умолчанию 10 млн строк):
--   docker compose -f build/docker-compose.yaml exec postgres \
--     psql -U app -d app -v rows=10000000 -f /sql/seed_postgres.sql

\if :{?rows}
\else
    \set rows 10000000
\endif

\timing on

INSERT INTO people (name, country, age, created_at)
SELECT
    'user_' || g,
    (ARRAY['Russia', 'USA', 'Germany', 'France', 'China', 'India',
           'Brazil', 'Japan', 'Kazakhstan', 'Turkey'])[1 + floor(random() * 10)::int],
    floor(random() * 101)::int,
    now() - random() * interval '365 days'
FROM generate_series(1, :rows) AS g;

-- Обновляем статистику, чтобы планировщик знал реальный размер таблицы.
VACUUM ANALYZE people;

SELECT count(*) AS total_people, pg_size_pretty(pg_total_relation_size('people')) AS table_size
FROM people;
