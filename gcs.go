// GCS KV is a simple, persistent, key-value store built on top of Google Cloud Storage
// It stores all data flatly in the configure GCS bucket.

package gcskv

import (
	"context"
	"fmt"
	"strings"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

// GcsStore is an implementation of a persistent Key Value store.
//
// Each key is stored as the name of a GCS object.
// Each value is stored in the content of a GCS object.
// One GCS object contains one key/value pair.
//
// Write and scan operations are not safe for concurrent mutation by multiple
// goroutines, but Read operations are.
//
// One should use the New() method to create the GCSStore
type GcsStore struct {
	client     *storage.Client
	bucketName string
	basepath   string
}

// New creates and returns a new GCSStore, initializing the GCS storage client.
//
// bucket specifies the name of the GCS bucket to store keys and values
//
// basepath is prepended to the name of each created object.
// For example, if the value of basepath is "gcskv/", all the key/value objects will be created in the gcskv/ folder
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

// Get returns the value of the given key. Error is returned if the key is not found
//
// add more docs
// even more docs
// feature 1
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

// Set creates or overwrites a key/value pair
// bug fix in here
// feature 2 completed
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

// Del removes the key/value pair from the GcsStore
// bug fix 2
func (store GcsStore) Del(key string) error {
	ctx := context.Background()
	key = store.basepath + key
	return store.client.Bucket(store.bucketName).Object(key).Delete(ctx)
}

// Size returns the number of key/value pairs in the GcsStore
// feature 3
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

// Scan returns all the keys >= startKey and < endKey in lexicographic order
//
// prefix is prepended to both the startKey and endKey to form 2 complete keys.
// For example, Scan("foo/", "a", "b") will return all the keys >= "foo/a" and < "foo/b"
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

// Clear remove all key/value pairs from GcsStore.
//
// It should not affect other objects in the bucket not prefixed by store.basepath
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
