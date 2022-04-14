package s3

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/relloyd/halfpipe/constants"
	"github.com/relloyd/halfpipe/rdbms/shared"
)

type AwsS3Bucket struct {
	Name   string `errorTxt:"bucket name" mandatory:"yes"`
	Prefix string `errorTxt:"bucket prefix"`
	Region string `errorTxt:"bucket region" mandatory:"yes"`
	Dsn    string
}

func (d AwsS3Bucket) Parse() error {
	_, err := ParseDSN(fmt.Sprintf("%s/%s", d.Name, d.Prefix), d.Region)
	return err
}

func (d AwsS3Bucket) GetScheme() (string, error) {
	return constants.ConnectionTypeS3, nil
}

func (d AwsS3Bucket) GetMap(m map[string]string) map[string]string {
	if m == nil {
		m = make(map[string]string)
	}
	m["name"] = d.Name
	m["prefix"] = d.Prefix
	m["region"] = d.Region
	return m
}

func NewAwsBucket(c *shared.ConnectionDetails) *AwsS3Bucket {
	return &AwsS3Bucket{
		Name:   c.Data["name"],
		Prefix: c.Data["prefix"],
		Region: c.Data["region"],
	}
}

// ParseDSN expects bucketPrefix to be of the form [s3://]<bucket>/<prefix>
// It returns an AwsS3Bucket populated with the components of bucketPrefix and the supplied region.
// If there is a parsing error it returns an error.
// The region may be empty.
func ParseDSN(bucketPrefix string, region string) (retval AwsS3Bucket, err error) {
	expectedScheme := "s3"
	s3url, err := url.Parse(bucketPrefix)
	if err != nil {
		return retval, fmt.Errorf("error parsing S3 URL: %v", err)
	}
	if s3url.Scheme != "" && s3url.Scheme != expectedScheme {
		return retval, fmt.Errorf("expected S3 URL scheme %q but got %q", expectedScheme, s3url.Scheme)
	}
	if region == "" {
		return retval, fmt.Errorf("value expected for bucket region")
	}
	retval.Name = s3url.Host
	if retval.Name == "" {
		return retval, fmt.Errorf("DSN failed to parse bucket name")
	}
	retval.Prefix = strings.Trim(s3url.Path, "/")
	retval.Region = region
	return
}

func AwsBucketToMap(m map[string]string, b AwsS3Bucket) map[string]string {
	m["name"] = b.Name
	m["prefix"] = b.Prefix
	m["region"] = b.Region
	return m
}
