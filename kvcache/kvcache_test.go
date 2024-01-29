package kvcache

import (
	"context"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TODO:
// - open/close
// - cache invalidation
// - benchmarks

const waitingTimeout = 1000 * time.Millisecond // followed by panic

type _tDummyDB struct {
	KVDatabase
	count atomic.Int64
	data  map[string]string
}

func (s *_tDummyDB) Get(key string) (string, error) {
	s.count.Add(1)
	time.Sleep(100 * time.Millisecond)
	if v, ok := s.data[key]; ok {
		return v, nil
	} else {
		return "", ErrNotExists
	}
}

func (s *_tDummyDB) MGet(keys []string) ([]*string, error) {
	s.count.Add(1)
	time.Sleep(100 * time.Millisecond)
	vals := make([]*string, len(keys))
	for i, key := range keys {
		if v, ok := s.data[key]; ok {
			vals[i] = &v
		}
	}
	return vals, nil
}

func (s *_tDummyDB) Keys() ([]string, error) {
	s.count.Add(1)
	time.Sleep(100 * time.Millisecond)
	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	return keys, nil
}

func TestCache_Get(t *testing.T) {
	db := &_tDummyDB{
		data: map[string]string{
			"first":  "192.168.0.1",
			"second": "192.168.0.2",
			"third":  "192.168.0.3",
		},
	}

	c := Open(context.Background(), db)
	defer c.Close()

	keys := []string{"first", "second", "third", "non-existent", "non-existent-2"}

	tests := []struct {
		name           string
		cacheReqCount  int
		wantDelay      time.Duration
		wantDBReqCount int
	}{
		{
			"not cached",
			100, // *must* be more than the length of the list of keys
			200 * time.Millisecond,
			len(keys),
		},
		{
			"cached",
			100,
			10 * time.Millisecond,
			0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			startTime := time.Now()
			startDBReqCount := db.count.Load()

			var wg sync.WaitGroup
			wg.Add(tt.cacheReqCount)
			for i := 0; i < tt.cacheReqCount; i++ {
				i := i
				go func() {
					defer wg.Done()
					key := keys[i%len(keys)]
					want, ok := db.data[key]
					wantErr := !ok
					got, err := c.Get(key)
					if (err != nil) != wantErr {
						t.Errorf("Cache.Get() error = %v, wantErr %v", err, wantErr)
						return
					}
					if got != want {
						t.Errorf("Cache.Get() = %v, want %v", got, want)
					}
				}()
			}

			tm := time.AfterFunc(waitingTimeout, func() { panic("waiting timeout expired") })
			wg.Wait()
			tm.Stop()

			if d := time.Since(startTime); d > tt.wantDelay {
				t.Errorf("total delay = %v, wantDelay < %v", d, tt.wantDelay)
			}
			if n := db.count.Load() - startDBReqCount; n != int64(tt.wantDBReqCount) {
				t.Errorf("number of request to DB = %d, want %d", n, tt.wantDBReqCount)
			}
		})
	}
}

func _ps(s string) *string { return &s }

func _pss2ss(pss []*string) []string {
	ss := make([]string, len(pss))
	for i, ps := range pss {
		if ps == nil {
			ss[i] = "<nil>"
		} else {
			ss[i] = *pss[i]
		}
	}
	return ss
}

// useless work for detect race
func _fullRotate[T any](a []T, k int) {
	for i, n := 0, len(a)*k; i < n; i++ {
		for i := 1; i < len(a); i++ {
			a[i-1], a[i] = a[i], a[i-1]
		}
	}
}

func TestCache_MGet(t *testing.T) {
	db := &_tDummyDB{
		data: map[string]string{
			"first":  "192.168.0.1",
			"second": "192.168.0.2",
			"third":  "192.168.0.3",
		},
	}

	c := Open(context.Background(), db)
	defer c.Close()

	keys := []string{"first", "second", "third", "non-existent", "non-existent-2"}
	wantVals := []*string{_ps("192.168.0.1"), _ps("192.168.0.2"), _ps("192.168.0.3"), nil, nil}

	tests := []struct {
		name           string
		cacheReqCount  int
		wantDelay      time.Duration
		wantDBReqCount int
	}{
		{
			"not cached",
			100,
			200 * time.Millisecond,
			100, // !!!
		},
		{
			"cached",
			100,
			10 * time.Millisecond,
			0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			startTime := time.Now()
			startDBReqCount := db.count.Load()

			var wg sync.WaitGroup
			wg.Add(tt.cacheReqCount)

			for i := 0; i < tt.cacheReqCount; i++ {
				go func() {
					defer wg.Done()
					wantErr := false
					vals, err := c.MGet(keys)

					if (err != nil) != wantErr {
						t.Errorf("Cache.MGet() error = %v, wantErr %v", err, wantErr)
						return
					}

					_fullRotate(vals, 1)

					if !reflect.DeepEqual(vals, wantVals) {
						t.Errorf("Cache.MGet() = %v, want %v", _pss2ss(vals), _pss2ss(wantVals))
					}
				}()
			}

			tm := time.AfterFunc(waitingTimeout, func() { panic("waiting timeout expired") })
			wg.Wait()
			tm.Stop()

			if d := time.Since(startTime); d > tt.wantDelay {
				t.Errorf("total delay = %v, wantDelay < %v", d, tt.wantDelay)
			}
			if n := db.count.Load() - startDBReqCount; n != int64(tt.wantDBReqCount) {
				t.Errorf("number of request to DB = %d, want %d", n, tt.wantDBReqCount)
			}
		})
	}
}

func TestCache_Keys(t *testing.T) {
	db := &_tDummyDB{
		data: map[string]string{
			"first":  "192.168.0.1",
			"second": "192.168.0.2",
			"third":  "192.168.0.3",
		},
	}

	c := Open(context.Background(), db)
	defer c.Close()

	wantKeys := []string{"first", "second", "third"}
	sort.Strings(wantKeys)

	tests := []struct {
		name           string
		cacheReqCount  int
		wantDelay      time.Duration
		wantDBReqCount int
	}{
		{
			"not cached",
			100,
			200 * time.Millisecond,
			100, // !!!
		},
		{
			"cached",
			100,
			10 * time.Millisecond,
			0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			startTime := time.Now()
			startDBReqCount := db.count.Load()

			var wg sync.WaitGroup
			wg.Add(tt.cacheReqCount)

			for i := 0; i < tt.cacheReqCount; i++ {
				go func() {
					defer wg.Done()
					wantErr := false
					keys, err := c.Keys()
					if (err != nil) != wantErr {
						t.Errorf("Cache.Keys() error = %v, wantErr %v", err, wantErr)
						return
					}

					sort.Strings(keys)
					_fullRotate(keys, 1)

					if !reflect.DeepEqual(keys, wantKeys) {
						t.Errorf("Cache.Keys() = %v, want %v", keys, wantKeys)
					}
				}()
			}

			tm := time.AfterFunc(waitingTimeout, func() { panic("waiting timeout expired") })
			wg.Wait()
			tm.Stop()

			if d := time.Since(startTime); d > tt.wantDelay {
				t.Errorf("total delay = %v, wantDelay < %v", d, tt.wantDelay)
			}
			if n := db.count.Load() - startDBReqCount; n != int64(tt.wantDBReqCount) {
				t.Errorf("number of request to DB = %d, want %d", n, tt.wantDBReqCount)
			}
		})
	}
}
