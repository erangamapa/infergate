package dedupe

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSameKeyRunsOnce(t *testing.T) {
	c := New[string](time.Minute, 100)
	var n atomic.Int32
	fn := func() (string, error) { n.Add(1); return "ok", nil }
	if _, hit, _ := c.Do(context.Background(), "k", fn); hit {
		t.Fatal("first call is not a hit")
	}
	v, hit, err := c.Do(context.Background(), "k", fn)
	if err != nil || !hit || v != "ok" || n.Load() != 1 {
		t.Fatalf("second call should be served from cache, v=%s hit=%v n=%d", v, hit, n.Load())
	}
}

func TestConcurrentCallersWait(t *testing.T) {
	c := New[int](time.Minute, 100)
	var n atomic.Int32
	release := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, _ = c.Do(context.Background(), "k", func() (int, error) {
				n.Add(1)
				<-release
				return 7, nil
			})
		}()
	}
	time.Sleep(20 * time.Millisecond)
	close(release)
	wg.Wait()
	if n.Load() != 1 {
		t.Fatalf("fn ran %d times", n.Load())
	}
}

func TestErrorsAreNotCached(t *testing.T) {
	c := New[int](time.Minute, 100)
	calls := 0
	_, _, err := c.Do(context.Background(), "k", func() (int, error) { calls++; return 0, errors.New("boom") })
	if err == nil {
		t.Fatal("want error")
	}
	v, _, err := c.Do(context.Background(), "k", func() (int, error) { calls++; return 3, nil })
	if err != nil || v != 3 || calls != 2 {
		t.Fatalf("retry after failure should run again, v=%d calls=%d", v, calls)
	}
}

func TestExpiry(t *testing.T) {
	c := New[int](time.Second, 100)
	now := time.Unix(0, 0)
	c.now = func() time.Time { return now }
	calls := 0
	fn := func() (int, error) { calls++; return calls, nil }
	_, _, _ = c.Do(context.Background(), "k", fn)
	now = now.Add(2 * time.Second)
	v, hit, _ := c.Do(context.Background(), "k", fn)
	if hit || v != 2 {
		t.Fatalf("expired entry should rerun, v=%d hit=%v", v, hit)
	}
}

func TestBounded(t *testing.T) {
	c := New[int](time.Hour, 4)
	for i := 0; i < 10; i++ {
		key := string(rune('a' + i))
		_, _, _ = c.Do(context.Background(), key, func() (int, error) { return i, nil })
	}
	if c.Len() > 4 {
		t.Fatalf("cache grew to %d", c.Len())
	}
}
