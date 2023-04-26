package nmsg

import (
	"context"
	"mime/multipart"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"golang.org/x/sync/errgroup"
)

func (p *NMSG) uploadMultiPartFile(ctx context.Context, listFiles []*multipart.FileHeader, prefix string, Tagging *string, expiredTime *time.Time) ([]*s3manager.UploadOutput, error) {

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(3)
	results := make([]*s3manager.UploadOutput, len(listFiles))
	for x := range listFiles {
		a := x

		g.Go(func() error {
			file, err := listFiles[a].Open()
			if err != nil {
				return err
			}

			uploader := s3manager.NewUploader(p.S3Sessions)
			output, err := uploader.UploadWithContext(ctx, &s3manager.UploadInput{
				Bucket:      p.Bucket,
				Key:         aws.String(filepath.Join(prefix, listFiles[x].Filename)),
				Body:        file,
				ContentType: aws.String(listFiles[a].Header.Get("Content-Type")),
				Expires:     expiredTime,
				Tagging:     Tagging,
			})
			if err != nil {
				return err
			}
			results[a] = output
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return results, nil
}
