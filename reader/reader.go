package reader

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/xitongsys/parquet-go-source/s3v2"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/source"
)

type S3ParquetReader[T any] struct {
	s3Client   *s3.Client
	s3Reader   *source.ParquetFile
	diskReader *reader.ParquetReader
	chunkSize  int
}

func (pr *S3ParquetReader[T]) GetBatchSize() int {
	return pr.chunkSize
}

func (pr *S3ParquetReader[T]) GetNumRows() int {
	return int(pr.diskReader.GetNumRows())
}

func (pr *S3ParquetReader[T]) GetNumChunks() int {
	return int(pr.GetNumRows()/pr.chunkSize) + 1
}

func (pr *S3ParquetReader[T]) GetNextChunk() (*[]T, error) {

	outputRows := make([]T, pr.chunkSize)
	if err := pr.diskReader.Read(&outputRows); err != nil {
		return nil, err
	}

	return &outputRows, nil
}

func (pr *S3ParquetReader[T]) Close() error {

	s3Reader := *pr.s3Reader
	errS3 := s3Reader.Close()

	diskReader := *pr.diskReader
	diskReader.ReadStop()

	return errS3
}

func New[T any](
	s3Client *s3.Client, bucket string, key string, batchSize int, numProcs int64,
) (*S3ParquetReader[T], error) {

	s3Reader, err := s3v2.NewS3FileReaderWithClient(
		context.Background(), s3Client, bucket, key,
	)
	if err != nil {
		return nil, err
	}

	diskReader, err := reader.NewParquetReader(s3Reader, new(T), numProcs)
	if err != nil {
		// TODO: close s3Reader, but figure out what to
		// do with err from that if one is raised
		return nil, err
	}

	return &S3ParquetReader[T]{
		s3Client:   s3Client,
		s3Reader:   &s3Reader,
		diskReader: diskReader,
		chunkSize:  batchSize,
	}, nil
}
