package gcskv

import (
	"context"
	"fmt"
	"strings"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

type GcsStore struct {
	client     *storage.Client
	bucketName string
	basepath   string
}

func New(bucket string, basepath string) (GcsStore, error) {
	ctx := context.Background()
	client, gcsErr := storage.NewClient(ctx)
	if gcsErr != nil {
		return GcsStore{}, gcsErr
	}
	return GcsStore{
		client:     client,
		bucketName: bucket,
		basepath:   basepath,
	}, nil
}

func (store GcsStore) Get(key string) ([]byte, error) {
	ctx := context.Background()
	key = store.basepath + key
	rc, err := store.client.Bucket(store.bucketName).Object(key).NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("file not found")
	}
	defer rc.Close()
	out := make([]byte, rc.Attrs.Size)
	size, err := rc.Read(out)
	if int64(size) != rc.Attrs.Size {
		return nil, fmt.Errorf("reading incomplete")
	}
	return out, err
}

func (store GcsStore) Set(key string, value []byte) error {
	ctx := context.Background()
	key = store.basepath + key
	writer := store.client.Bucket(store.bucketName).Object(key).NewWriter(ctx)
	defer writer.Close()
	size, err := writer.Write(value)
	if size != len(value) {
		return fmt.Errorf("writing incomplete")
	}
	return err
}

func (store GcsStore) Del(key string) error {
	ctx := context.Background()
	key = store.basepath + key
	return store.client.Bucket(store.bucketName).Object(key).Delete(ctx)
}

func (store GcsStore) Size() (int, error) {
	ctx := context.Background()
	query := &storage.Query{Prefix: store.basepath}
	iter := store.client.Bucket(store.bucketName).Objects(ctx, query)
	count := 0
	for {
		_, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return 0, err
		}
		count++
	}
	return count, nil
}

func (store GcsStore) Scan(prefix, startKey, endKey string) ([]string, error) {
	ctx := context.Background()
	query := &storage.Query{
		StartOffset: store.basepath + prefix + startKey,
		EndOffset:   store.basepath + prefix + endKey,
	}
	query.SetAttrSelection([]string{"Name"})
	iter := store.client.Bucket(store.bucketName).Objects(ctx, query)
	names := make([]string, 0, iter.PageInfo().MaxSize)
	for {
		objAttrs, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return names, err
		}
		name := strings.Replace(objAttrs.Name, store.basepath, "", 1)
		names = append(names, name)
	}
	return names, nil
}

func (store GcsStore) Clear() error {
	ctx := context.Background()
	query := &storage.Query{Prefix: store.basepath}
	iter := store.client.Bucket(store.bucketName).Objects(ctx, query)
	for {
		objAttrs, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}
		err = store.client.Bucket(store.bucketName).Object(objAttrs.Name).Delete(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}
