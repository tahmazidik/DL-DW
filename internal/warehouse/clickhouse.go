// Package warehouse — Data Warehouse (ClickHouse): колоночная OLAP-база
// для аналитики. Данные забирает прямо из озера (Parquet в S3).
package warehouse

import (
	"context"
	"fmt"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type Warehouse struct {
	conn driver.Conn
	// Ключи доступа к S3 передаём в функцию s3() ClickHouse.
	s3AccessKey string
	s3SecretKey string
}

type Config struct {
	Addr        string
	User        string
	Password    string
	S3AccessKey string
	S3SecretKey string
}

func New(ctx context.Context, cfg Config) (*Warehouse, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{cfg.Addr},
		Auth: clickhouse.Auth{
			Database: "analytics",
			Username: cfg.User,
			Password: cfg.Password,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("open clickhouse: %w", err)
	}
	if err := conn.Ping(ctx); err != nil {
		conn.Close()
		return nil, fmt.Errorf("ping clickhouse: %w", err)
	}
	return &Warehouse{conn: conn, s3AccessKey: cfg.S3AccessKey, s3SecretKey: cfg.S3SecretKey}, nil
}

func (w *Warehouse) Close() error {
	return w.conn.Close()
}

// LoadPeople полностью заменяет analytics.people содержимым Parquet-файла из озера.
// ClickHouse сам читает файл из S3 и сам выводит его схему (schema-on-read).
// Сначала грузим во временную таблицу, потом атомарно меняем таблицы местами,
// чтобы при ошибке аналитики не остались с пустой таблицей.
func (w *Warehouse) LoadPeople(ctx context.Context, objectURL string) (uint64, error) {
	source := fmt.Sprintf("s3(%s, %s, %s, 'Parquet')",
		quote(objectURL), quote(w.s3AccessKey), quote(w.s3SecretKey))

	steps := []string{
		`DROP TABLE IF EXISTS analytics.people_staging`,
		`CREATE TABLE analytics.people_staging AS analytics.people`,
		`INSERT INTO analytics.people_staging (id, name, country, age, created_at)
		 SELECT id, name, country, age, created_at FROM ` + source + `
		 -- Не превращать часть пути dt=YYYY-MM-DD в отдельную колонку.
		 SETTINGS use_hive_partitioning = 0`,
		`EXCHANGE TABLES analytics.people AND analytics.people_staging`,
		`DROP TABLE analytics.people_staging`,
	}
	for _, query := range steps {
		if err := w.conn.Exec(ctx, query); err != nil {
			return 0, fmt.Errorf("%s: %w", strings.Fields(query)[0], err)
		}
	}

	var count uint64
	if err := w.conn.QueryRow(ctx, `SELECT count() FROM analytics.people`).Scan(&count); err != nil {
		return 0, fmt.Errorf("count people: %w", err)
	}
	return count, nil
}

// quote превращает строку в строковый литерал ClickHouse.
func quote(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `'`, `\'`)
	return "'" + s + "'"
}
