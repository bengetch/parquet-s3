package writer

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/xitongsys/parquet-go-source/s3v2"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"
)

type S3ParquetWriter[T any] struct {
	s3Client   *s3.Client
	s3Writer   *source.ParquetFile
	diskWriter *writer.ParquetWriter
}

func (pw *S3ParquetWriter[T]) SetRowGroupSize(size int64) {
	pw.diskWriter.RowGroupSize = size
}

func (pw *S3ParquetWriter[T]) SetCompressionType(comprType parquet.CompressionCodec) {
	pw.diskWriter.CompressionType = comprType
}

func (pw *S3ParquetWriter[T]) Write(r T) error {
	/*
		on the surface, it looks like the parquet writer is doing single-line writes to the
		in-memory parquet file object. the parquet-go library is actually writing to a buffer,
		though, and only doing file writes once the buffer is full. the size of the buffer is
		set via the RowGroupSize parameter
		TODO: should we check if pw.diskWriter.RowGroupSize is 0 here, or defer error handling
		 to the underlying parquet-go library?
	*/
	return pw.diskWriter.Write(r)
}

func (pw *S3ParquetWriter[T]) WriteChunk(rs *[]T) error {
	for _, r := range *rs {
		err := pw.Write(r)
		if err != nil {
			return err
		}
	}
	return nil
}

func (pw *S3ParquetWriter[T]) Close() (error, error) {

	diskWriter := *pw.diskWriter
	errDisk := diskWriter.WriteStop()

	s3Writer := *pw.s3Writer
	errS3 := s3Writer.Close()

	return errS3, errDisk
}

func New[T any](
	s3Client *s3.Client, bucket string, key string, numProcs int64,
) (*S3ParquetWriter[T], error) {

	s3Writer, err := s3v2.NewS3FileWriterWithClient(
		context.Background(), s3Client, bucket, key, nil,
	)
	if err != nil {
		return nil, err
	}

	diskWriter, err := writer.NewParquetWriter(s3Writer, new(T), numProcs)
	if err != nil {
		// TODO: close s3Writer, but figure out what to
		// do with err from that if one is raised
		return nil, err
	}

	return &S3ParquetWriter[T]{
		s3Client:   s3Client,
		s3Writer:   &s3Writer,
		diskWriter: diskWriter,
	}, nil
}
