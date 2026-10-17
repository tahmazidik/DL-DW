-- Генерирует тестовых людей в Postgres, чтобы разница в скорости была заметна.
-- ВНИМАНИЕ: сначала очищает таблицу people.
-- Запуск (по умолчанию 10 млн строк):
--   docker compose -f build/docker-compose.yaml exec postgres \
--     psql -U app -d app -v rows=10000000 -f /sql/seed_postgres.sql

\if :{?rows}
\else
    \set rows 10000000
\endif

\timing on

TRUNCATE people RESTART IDENTITY;

-- Данные специально неравномерные, чтобы на дашбордах было что разглядывать:
--   * имена: первые в списке встречаются гораздо чаще последних;
--   * страны: первые в списке крупнее, и у каждой страны свой средний возраст;
--   * регистрации: чем ближе к сегодняшнему дню, тем их больше (рост сервиса).
INSERT INTO people (name, country, age, created_at)
SELECT
    (ARRAY['Александр', 'Мария', 'Михаил', 'Анна', 'Иван', 'София', 'Дмитрий', 'Анастасия',
           'Максим', 'Елена', 'Артём', 'Дарья', 'Никита', 'Полина', 'Егор', 'Виктория',
           'Кирилл', 'Ксения', 'Андрей', 'Алиса', 'Илья', 'Вероника', 'Тимофей', 'Ева',
           'Роман', 'Варвара', 'Матвей', 'Ульяна', 'Лев', 'Василиса'])[1 + floor(30 * power(random(), 2))::int],
    (ARRAY['Russia', 'USA', 'Germany', 'India', 'China', 'Brazil',
           'Kazakhstan', 'France', 'Turkey', 'Japan'])[1 + c],
    least(100, 16 + 2 * c + floor((random() + random() + random()) * 18))::int,
    now() - interval '365 days' * (1 - sqrt(random()))
FROM (
    SELECT floor(10 * power(random(), 1.5))::int AS c
    FROM generate_series(1, :rows)
) AS g;

-- Обновляем статистику, чтобы планировщик знал реальный размер таблицы.
VACUUM ANALYZE people;

SELECT count(*) AS total_people, pg_size_pretty(pg_total_relation_size('people')) AS table_size
FROM people;
