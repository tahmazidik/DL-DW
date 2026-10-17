package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"example-system/internal/database"
	"example-system/internal/lake"
	"example-system/internal/warehouse"
)

// Выгрузка и загрузка больших снапшотов (десятки миллионов строк) занимает время.
const pipelineTimeout = 10 * time.Minute

// PersonRow — схема Parquet-файла в озере.
type PersonRow struct {
	ID        int64  `parquet:"id"`
	Name      string `parquet:"name"`
	Country   string `parquet:"country,dict"`
	Age       int32  `parquet:"age"`
	CreatedAt int64  `parquet:"created_at,timestamp(microsecond)"`
}

type App struct {
	db        *database.Postgres
	lake      *lake.Lake
	warehouse *warehouse.Warehouse
}

func env(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("missing environment variable: %s", key)
	}
	return v
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// fail логирует настоящую ошибку и отдаёт клиенту короткое сообщение.
func fail(w http.ResponseWriter, status int, msg string, err error) {
	slog.Error(msg, "error", err)
	http.Error(w, msg, status)
}

// POST /people — принимает один объект или массив объектов.
func (a *App) peopleHandler(w http.ResponseWriter, r *http.Request) {
	// Читаем JSON, но пока не знаем, объект это или массив.
	var raw json.RawMessage

	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&raw); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	// Не разрешаем второе JSON-значение после первого.
	var extra json.RawMessage
	if err := decoder.Decode(&extra); err != io.EOF {
		http.Error(w, "expected one JSON value", http.StatusBadRequest)
		return
	}

	var people []database.Person

	switch raw[0] {
	case '{':
		var person database.Person
		if err := json.Unmarshal(raw, &person); err != nil {
			http.Error(w, "invalid person", http.StatusBadRequest)
			return
		}
		people = []database.Person{person}

	case '[':
		if err := json.Unmarshal(raw, &people); err != nil {
			http.Error(w, "invalid people array", http.StatusBadRequest)
			return
		}

	default:
		http.Error(w, "expected a person object or an array", http.StatusBadRequest)
		return
	}

	if len(people) == 0 {
		http.Error(w, "no people in request", http.StatusBadRequest)
		return
	}

	for i := range people {
		people[i].Name = strings.TrimSpace(people[i].Name)
		people[i].Country = strings.TrimSpace(people[i].Country)

		if people[i].Name == "" || people[i].Country == "" {
			http.Error(w, fmt.Sprintf("person at index %d: name and country are required", i), http.StatusBadRequest)
			return
		}
		if people[i].Age < 0 || people[i].Age > 120 {
			http.Error(w, fmt.Sprintf("person at index %d: age must be between 0 and 120", i), http.StatusBadRequest)
			return
		}
	}

	if err := a.db.InsertPeople(r.Context(), people); err != nil {
		fail(w, http.StatusInternalServerError, "failed to save people", err)
		return
	}

	writeJSON(w, http.StatusCreated, map[string]any{
		"count":  len(people),
		"people": people,
	})
}

// POST /export/s3 — Postgres -> Parquet-файл в озере.
func (a *App) exportS3Handler(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), pipelineTimeout)
	defer cancel()

	// Раскладываем снапшоты по дням, как принято в озёрах: raw/people/dt=2026-10-17/...
	now := time.Now().UTC()
	key := fmt.Sprintf("raw/people/dt=%s/people_%s.parquet", now.Format("2006-01-02"), now.Format("150405"))

	started := time.Now()
	rows, err := lake.UploadParquet(ctx, a.lake, key, func(write func(PersonRow) error) error {
		return a.db.StreamPeople(ctx, func(p database.Person) error {
			return write(PersonRow{
				ID:        p.ID,
				Name:      p.Name,
				Country:   p.Country,
				Age:       p.Age,
				CreatedAt: p.CreatedAt.UnixMicro(),
			})
		})
	})
	if err != nil {
		fail(w, http.StatusInternalServerError, "export to S3 failed", err)
		return
	}

	slog.Info("exported snapshot", "key", key, "rows", rows, "took", time.Since(started))
	writeJSON(w, http.StatusOK, map[string]any{"key": key, "rows": rows})
}

// POST /load/clickhouse {"key": "raw/people/..."} — файл из озера -> ClickHouse.
func (a *App) loadClickHouseHandler(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), pipelineTimeout)
	defer cancel()

	var req struct {
		Key string `json:"key"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Key == "" {
		http.Error(w, `expected JSON body {"key": "raw/people/..."}`, http.StatusBadRequest)
		return
	}

	started := time.Now()
	rows, err := a.warehouse.LoadPeople(ctx, a.lake.ObjectURL(req.Key))
	if err != nil {
		fail(w, http.StatusInternalServerError, "load to ClickHouse failed", err)
		return
	}

	slog.Info("loaded ClickHouse", "key", req.Key, "rows", rows, "took", time.Since(started))
	writeJSON(w, http.StatusOK, map[string]any{"key": req.Key, "rows": rows})
}

func main() {
	ctx := context.Background()

	db, err := database.New(ctx, env("PG_DSN"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	dataLake, err := lake.New(ctx, env("S3_ENDPOINT"), env("S3_BUCKET"))
	if err != nil {
		log.Fatal(err)
	}
	if err := dataLake.EnsureBucket(ctx); err != nil {
		log.Fatal(err)
	}

	dwh, err := warehouse.New(ctx, warehouse.Config{
		Addr:        env("CH_ADDR"),
		User:        env("CH_USER"),
		Password:    env("CH_PASSWORD"),
		S3AccessKey: env("AWS_ACCESS_KEY_ID"),
		S3SecretKey: env("AWS_SECRET_ACCESS_KEY"),
	})
	if err != nil {
		log.Fatal(err)
	}
	defer dwh.Close()

	app := &App{db: db, lake: dataLake, warehouse: dwh}
	mux := http.NewServeMux()

	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "ok")
	})
	mux.HandleFunc("POST /people", app.peopleHandler)
	mux.HandleFunc("POST /export/s3", app.exportS3Handler)
	mux.HandleFunc("POST /load/clickhouse", app.loadClickHouseHandler)

	addr := ":" + envOr("PORT", "8088")
	slog.Info("API started", "addr", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}
