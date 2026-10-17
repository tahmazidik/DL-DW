// Package lake — Data Lake (S3/MinIO): складываем «сырые» снапшоты файлами
// в колоночном формате Parquet. Схему у файла никто не проверяет при записи.
package lake

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/parquet-go/parquet-go"
)

// Сколько строк копим в памяти перед записью в Parquet-файл.
const batchSize = 10_000

type Lake struct {
	client   *s3.Client
	endpoint string
	bucket   string
}

func New(ctx context.Context, endpoint, bucket string) (*Lake, error) {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion("us-east-1"))
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
		// MinIO из образа 2023 года не знает про новые контрольные суммы AWS SDK.
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
	})

	return &Lake{client: client, endpoint: endpoint, bucket: bucket}, nil
}

// EnsureBucket создаёт бакет, если его ещё нет. MinIO может подниматься
// дольше нашего сервиса, поэтому пробуем несколько раз.
func (l *Lake) EnsureBucket(ctx context.Context) error {
	var err error
	for attempt := 1; attempt <= 30; attempt++ {
		_, err = l.client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(l.bucket)})

		var owned *types.BucketAlreadyOwnedByYou
		var exists *types.BucketAlreadyExists
		if err == nil || errors.As(err, &owned) || errors.As(err, &exists) {
			return nil
		}

		slog.Warn("S3 not ready yet", "attempt", attempt, "error", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	return fmt.Errorf("create bucket %q: %w", l.bucket, err)
}

// ObjectURL — адрес объекта, по которому его может прочитать ClickHouse.
func (l *Lake) ObjectURL(key string) string {
	return l.endpoint + "/" + l.bucket + "/" + key
}

// UploadParquet пишет строки, которые отдаёт produce, во временный Parquet-файл
// и загружает его в бакет под ключом key. Возвращает число записанных строк.
func UploadParquet[T any](ctx context.Context, l *Lake, key string, produce func(write func(T) error) error) (int, error) {
	file, err := os.CreateTemp("", "snapshot-*.parquet")
	if err != nil {
		return 0, fmt.Errorf("create temp file: %w", err)
	}
	defer os.Remove(file.Name())
	defer file.Close()

	writer := parquet.NewGenericWriter[T](file, parquet.Compression(&parquet.Zstd))
	batch := make([]T, 0, batchSize)
	count := 0

	flush := func() error {
		if _, err := writer.Write(batch); err != nil {
			return fmt.Errorf("write parquet: %w", err)
		}
		count += len(batch)
		batch = batch[:0]
		return nil
	}

	err = produce(func(row T) error {
		batch = append(batch, row)
		if len(batch) == batchSize {
			return flush()
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	if err := flush(); err != nil {
		return 0, err
	}
	if err := writer.Close(); err != nil {
		return 0, fmt.Errorf("close parquet writer: %w", err)
	}

	if _, err := file.Seek(0, 0); err != nil {
		return 0, fmt.Errorf("rewind temp file: %w", err)
	}
	_, err = l.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(l.bucket),
		Key:    aws.String(key),
		Body:   file,
	})
	if err != nil {
		return 0, fmt.Errorf("upload %s: %w", key, err)
	}
	return count, nil
}
