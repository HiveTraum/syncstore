package syncstore_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/HiveTraum/syncstore"
)

type fakeListener struct {
	signals chan struct{}
}

// буфер — чтобы тесты слали пачки сигналов, не дожидаясь перечиток
func newFakeListener() *fakeListener {
	return &fakeListener{signals: make(chan struct{}, 16)}
}

func (l *fakeListener) Listen(context.Context) (<-chan struct{}, error) {
	return l.signals, nil
}

func waitFor(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("не дождались: " + msg)
}

func TestGetLoadsLazilyAndCaches(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	st := syncstore.New(newFakeListener(), func(context.Context) (string, error) {
		calls.Add(1)
		return "value", nil
	})

	for range 3 {
		v, err := st.Get(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if v != "value" {
			t.Fatalf("получили %q", v)
		}
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("loader вызван %d раз, ожидали 1", n)
	}
}

func TestGetConcurrentLoadsOnce(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	gate := make(chan struct{})
	st := syncstore.New(newFakeListener(), func(context.Context) (int, error) {
		calls.Add(1)
		<-gate
		return 42, nil
	})

	var wg sync.WaitGroup
	errs := make(chan error, 10)
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, err := st.Get(t.Context())
			if err == nil && v != 42 {
				err = errors.New("неожиданное значение")
			}
			errs <- err
		}()
	}
	time.Sleep(20 * time.Millisecond) // даём горутинам добраться до Get
	close(gate)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("loader вызван %d раз, ожидали 1", n)
	}
}

func TestGetPropagatesLoadError(t *testing.T) {
	t.Parallel()
	boom := errors.New("boom")
	var fail atomic.Bool
	fail.Store(true)
	st := syncstore.New(newFakeListener(), func(context.Context) (int, error) {
		if fail.Load() {
			return 0, boom
		}
		return 7, nil
	})

	if _, err := st.Get(t.Context()); !errors.Is(err, boom) {
		t.Fatalf("ожидали boom, получили %v", err)
	}
	fail.Store(false)
	v, err := st.Get(t.Context())
	if err != nil || v != 7 {
		t.Fatalf("после починки loader: v=%d err=%v", v, err)
	}
}

func TestRunReloadsOnSignal(t *testing.T) {
	t.Parallel()
	var current atomic.Int64
	current.Store(1)
	var calls atomic.Int32
	lst := newFakeListener()
	st := syncstore.New(lst, func(context.Context) (int64, error) {
		calls.Add(1)
		return current.Load(), nil
	})

	go st.Run(t.Context())
	waitFor(t, func() bool { return calls.Load() >= 1 }, "первичная загрузка")

	current.Store(2)
	lst.signals <- struct{}{}
	waitFor(t, func() bool {
		v, err := st.Get(t.Context())
		return err == nil && v == 2
	}, "значение обновилось по сигналу")
}

func TestReloadErrorKeepsOldValue(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	var reported atomic.Int32
	lst := newFakeListener()
	st := syncstore.New(lst,
		func(context.Context) (int32, error) {
			n := calls.Add(1)
			if n > 1 {
				return 0, errors.New("boom")
			}
			return n, nil
		},
		// час — чтобы retry не успел затереть проверку старого значения
		syncstore.WithRetryInterval(time.Hour),
		syncstore.WithOnError(func(error) { reported.Add(1) }),
	)

	go st.Run(t.Context())
	waitFor(t, func() bool { return calls.Load() == 1 }, "первичная загрузка")

	lst.signals <- struct{}{}
	waitFor(t, func() bool { return reported.Load() == 1 }, "ошибка перезагрузки отрепорчена")

	v, err := st.Get(t.Context())
	if err != nil || v != 1 {
		t.Fatalf("старое значение потеряно: v=%d err=%v", v, err)
	}
}

func TestReloadErrorRetries(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	lst := newFakeListener()
	st := syncstore.New(lst,
		func(context.Context) (int32, error) {
			n := calls.Add(1)
			if n == 2 {
				return 0, errors.New("boom")
			}
			return n, nil
		},
		syncstore.WithRetryInterval(5*time.Millisecond),
	)

	go st.Run(t.Context())
	waitFor(t, func() bool { return calls.Load() == 1 }, "первичная загрузка")

	lst.signals <- struct{}{}
	waitFor(t, func() bool {
		v, err := st.Get(t.Context())
		return err == nil && v == 3
	}, "retry перечитал значение после ошибки")
}

func TestInitialLoadErrorRetries(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	st := syncstore.New(newFakeListener(),
		func(context.Context) (int32, error) {
			n := calls.Add(1)
			if n == 1 {
				return 0, errors.New("boom")
			}
			return n, nil
		},
		syncstore.WithRetryInterval(5*time.Millisecond),
	)

	go st.Run(t.Context())
	waitFor(t, func() bool {
		v, err := st.Get(t.Context())
		return err == nil && v >= 2
	}, "retry после неудачной первичной загрузки")
}

func TestDebounceCoalescesSignals(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	lst := newFakeListener()
	st := syncstore.New(lst,
		func(context.Context) (int32, error) { return calls.Add(1), nil },
		syncstore.WithDebounce(50*time.Millisecond),
	)

	go st.Run(t.Context())
	waitFor(t, func() bool { return calls.Load() == 1 }, "первичная загрузка")

	for range 5 {
		lst.signals <- struct{}{}
	}
	waitFor(t, func() bool { return calls.Load() == 2 }, "перечитка после debounce")
	time.Sleep(150 * time.Millisecond)
	if n := calls.Load(); n != 2 {
		t.Fatalf("пачка сигналов не схлопнулась: %d загрузок", n)
	}
}

type fakeNotifier struct {
	calls atomic.Int32
}

func (n *fakeNotifier) Notify(context.Context) error {
	n.calls.Add(1)
	return nil
}

func TestNotifyDelegatesToNotifier(t *testing.T) {
	t.Parallel()
	load := func(context.Context) (int, error) { return 1, nil }

	bare := syncstore.New(newFakeListener(), load)
	if err := bare.Notify(t.Context()); !errors.Is(err, syncstore.ErrNoNotifier) {
		t.Fatalf("без notifier ожидали ErrNoNotifier, получили %v", err)
	}

	var n fakeNotifier
	st := syncstore.New(newFakeListener(), load, syncstore.WithNotifier(&n))
	if err := st.Notify(t.Context()); err != nil {
		t.Fatal(err)
	}
	if n.calls.Load() != 1 {
		t.Fatalf("notifier вызван %d раз, ожидали 1", n.calls.Load())
	}
}

func TestRunReturnsWhenListenerStops(t *testing.T) {
	t.Parallel()
	lst := newFakeListener()
	st := syncstore.New(lst, func(context.Context) (int, error) { return 1, nil })

	done := make(chan error, 1)
	go func() { done <- st.Run(t.Context()) }()
	close(lst.signals)

	select {
	case err := <-done:
		if !errors.Is(err, syncstore.ErrListenerStopped) {
			t.Fatalf("ожидали ErrListenerStopped, получили %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run не завершился после остановки слушателя")
	}
}
