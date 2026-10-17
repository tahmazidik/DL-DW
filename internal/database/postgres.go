// Package database — OLTP-часть системы (Postgres): принимает записи
// по одной, проверяет схему при записи (schema-on-write) и отдаёт данные на выгрузку.
package database

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Person struct {
	ID        int64     `json:"id"`
	Name      string    `json:"name"`
	Country   string    `json:"country"`
	Age       int32     `json:"age"`
	CreatedAt time.Time `json:"created_at"`
}

type Postgres struct {
	pool *pgxpool.Pool
}

func New(ctx context.Context, dsn string) (*Postgres, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect to postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return &Postgres{pool: pool}, nil
}

func (p *Postgres) Close() {
	p.pool.Close()
}

// InsertPeople сохраняет всех людей в одной транзакции: либо все, либо никто.
// id и created_at проставляет сама база.
func (p *Postgres) InsertPeople(ctx context.Context, people []Person) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	for i := range people {
		err := tx.QueryRow(ctx,
			`INSERT INTO people (name, country, age)
			 VALUES ($1, $2, $3)
			 RETURNING id, created_at`,
			people[i].Name, people[i].Country, people[i].Age,
		).Scan(&people[i].ID, &people[i].CreatedAt)
		if err != nil {
			return fmt.Errorf("insert person %d: %w", i, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// StreamPeople читает таблицу построчно и не держит её целиком в памяти.
func (p *Postgres) StreamPeople(ctx context.Context, fn func(Person) error) error {
	rows, err := p.pool.Query(ctx,
		`SELECT id, name, country, age, created_at FROM people ORDER BY id`)
	if err != nil {
		return fmt.Errorf("query people: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var person Person
		if err := rows.Scan(&person.ID, &person.Name, &person.Country, &person.Age, &person.CreatedAt); err != nil {
			return fmt.Errorf("scan person: %w", err)
		}
		if err := fn(person); err != nil {
			return err
		}
	}
	return rows.Err()
}
