CREATE DATABASE IF NOT EXISTS analytics;

-- Колоночная таблица для аналитики. Данные на диске отсортированы по ORDER BY,
-- поэтому фильтры и группировки по country/age читают минимум данных.
CREATE TABLE IF NOT EXISTS analytics.people
(
    id          UInt64,
    name        String,
    country     LowCardinality(String),
    age         UInt8,
    created_at  DateTime('UTC')
)
ENGINE = MergeTree
ORDER BY (country, age, id);
