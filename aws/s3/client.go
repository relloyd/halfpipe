package s3

func NewClient(bucket, region, prefix string) Client {
	basicClient := NewBasicClient(bucket, region, prefix)
	return NewClientFromBasic(basicClient)
}

func NewClientFromBasic(basicClient BasicClient) Client {
	return &client{
		BasicClient: basicClient,
	}
}

type client struct {
	BasicClient
}

func (s *client) Move(src, dst string) error {
	data, err := s.Get(src)
	if err != nil {
		return err
	}

	err = s.Put(dst, data)
	if err != nil {
		return err
	}

	return s.Delete(src)
}
