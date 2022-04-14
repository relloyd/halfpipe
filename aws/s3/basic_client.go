package s3

import (
	"bytes"
	"io"
	"io/ioutil"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

func NewBasicClient(bucket, region, prefix string) BasicClient {
	awsConfig := aws.NewConfig()
	awsConfig.Region = aws.String(region)
	sess := session.Must(session.NewSession(awsConfig))
	api := s3.New(sess)

	return &basicClient{
		bucket: bucket,
		region: region,
		prefix: prefix,
		api:    api,
	}
}

//func NewBasicClientWithAPI(bucket, region, prefix string, api s3iface.S3API) BasicClient {
//	return &basicClient{
//		bucket: bucket,
//		region: region,
//		prefix: prefix,
//		api:    api,
//	}
//}

type basicClient struct {
	region string
	bucket string
	prefix string
	api    s3iface.S3API
}

func (s *basicClient) List(key string) (keys []string, err error) {
	keys = make([]string, 0, 1000)
	lastKey := ""
	for {
		params := &s3.ListObjectsInput{
			Bucket:  aws.String(s.bucket),
			Marker:  aws.String(lastKey),
			MaxKeys: aws.Int64(1000),
			Prefix:  aws.String(s.getKeyWithPrefix(key)),
		}
		resp, err := s.api.ListObjects(params)
		if err != nil {
			return nil, err
		}

		for _, v := range resp.Contents {
			keys = append(keys, *v.Key)
		}
		if len(keys) > 0 {
			lastKey = keys[len(keys)-1]
		}

		if *resp.IsTruncated == false {
			break
		}
	}
	return
}

func (s *basicClient) Get(key string) ([]byte, error) {
	res, err := s.api.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.getKeyWithPrefix(key)),
	})

	if err != nil {
		awsErr := err.(awserr.Error)
		if awsErr.Code() == s3.ErrCodeNoSuchKey {
			return nil, ErrKeyNotFound
		}
		return nil, err
	}

	defer res.Body.Close()

	return ioutil.ReadAll(res.Body)
}

func (s *basicClient) Put(key string, data []byte) error {
	dataBuf := bytes.NewReader(data)

	_, err := s.api.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.getKeyWithPrefix(key)),
		Body:   dataBuf,
	})

	return err
}

func (s *basicClient) BufferPut(key string, dataBuf io.ReadSeeker) error {
	_, err := s.api.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.getKeyWithPrefix(key)),
		Body:   dataBuf,
	})

	return err
}

func (s *basicClient) Delete(key string) error {
	_, err := s.api.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.getKeyWithPrefix(key)),
	})
	return err
}

func (s *basicClient) getKeyWithPrefix(key string) string {
	// return s.prefix + key
	if s.prefix != "" {
		return strings.TrimRight(s.prefix, "/") + "/" + key // ensure trailing slash after prefix.
	} else {
		return key
	}

}
