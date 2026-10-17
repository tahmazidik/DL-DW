-- OLTP-таблица. Схема проверяется при записи (schema-on-write):
-- строку с age = -5 или без имени база просто не примет.
CREATE TABLE IF NOT EXISTS people (
    id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name       TEXT        NOT NULL,
    country    TEXT        NOT NULL,
    age        INT         NOT NULL CHECK (age BETWEEN 0 AND 120),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
