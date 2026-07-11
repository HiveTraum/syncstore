package syncstore

import (
	"context"
	"sync"
	"time"
)

const defaultRetryInterval = 5 * time.Second

// Option настраивает хранилище, созданное New.
type Option func(*options)

type options struct {
	debounce      time.Duration
	retryInterval time.Duration
	onError       func(error)
	notifier      Notifier
}

// WithDebounce откладывает перезагрузку на d после сигнала: пачка сигналов,
// пришедших подряд, схлопывается в одну перечитку. По умолчанию перезагрузка
// немедленная.
func WithDebounce(d time.Duration) Option {
	return func(o *options) { o.debounce = d }
}

// WithRetryInterval задаёт паузу перед повтором неудачной фоновой
// перезагрузки. По умолчанию 5 секунд.
func WithRetryInterval(d time.Duration) Option {
	return func(o *options) { o.retryInterval = d }
}

// WithOnError задаёт обработчик ошибок фоновых перезагрузок (логи, метрики).
// По умолчанию ошибки не репортятся: хранилище молча оставляет старое
// значение и повторяет попытку позже.
func WithOnError(fn func(error)) Option {
	return func(o *options) { o.onError = fn }
}

// WithNotifier задаёт пишущую сторону для Store.Notify. Без неё Notify
// возвращает ErrNoNotifier.
func WithNotifier(n Notifier) Option {
	return func(o *options) { o.notifier = n }
}

// New связывает источник сигналов и загрузчик в хранилище.
func New[T any](listener Listener, load Loader[T], opts ...Option) Store[T] {
	o := options{retryInterval: defaultRetryInterval}
	for _, opt := range opts {
		opt(&o)
	}
	return &store[T]{listener: listener, load: load, opts: o}
}

type store[T any] struct {
	listener Listener
	load     Loader[T]
	opts     options

	loadMu sync.Mutex   // сериализует вызовы load
	mu     sync.RWMutex // защищает value/loaded
	value  T
	loaded bool
}

func (s *store[T]) Notify(ctx context.Context) error {
	if s.opts.notifier == nil {
		return ErrNoNotifier
	}
	return s.opts.notifier.Notify(ctx)
}

func (s *store[T]) Get(ctx context.Context) (T, error) {
	if v, ok := s.current(); ok {
		return v, nil
	}
	return s.loadIfEmpty(ctx)
}

func (s *store[T]) current() (T, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.value, s.loaded
}

// loadIfEmpty загружает значение, если его ещё нет; параллельные вызовы
// сериализуются — опоздавшие получают результат первого без лишнего запроса.
func (s *store[T]) loadIfEmpty(ctx context.Context) (T, error) {
	s.loadMu.Lock()
	defer s.loadMu.Unlock()
	if v, ok := s.current(); ok {
		return v, nil
	}
	return s.reload(ctx)
}

func (s *store[T]) forceReload(ctx context.Context) error {
	s.loadMu.Lock()
	defer s.loadMu.Unlock()
	_, err := s.reload(ctx)
	return err
}

// reload вызывается только под loadMu.
func (s *store[T]) reload(ctx context.Context) (T, error) {
	v, err := s.load(ctx)
	if err != nil {
		var zero T
		return zero, err
	}
	s.mu.Lock()
	s.value, s.loaded = v, true
	s.mu.Unlock()
	return v, nil
}

func (s *store[T]) Run(ctx context.Context) error {
	signals, err := s.listener.Listen(ctx)
	if err != nil {
		return err
	}

	// первичная загрузка — сразу, без debounce; неудача уходит в retry
	var retry <-chan time.Time
	if _, err := s.loadIfEmpty(ctx); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		s.reportError(err)
		retry = time.After(s.opts.retryInterval)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-retry:
		case _, ok := <-signals:
			if !ok {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return ErrListenerStopped
			}
			if !s.debounceWait(ctx) {
				return ctx.Err()
			}
		}
		retry = nil
		drain(signals) // сигналы, накопившиеся к этому моменту, покроет перечитка
		if err := s.forceReload(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			s.reportError(err)
			retry = time.After(s.opts.retryInterval)
		}
	}
}

// debounceWait выдерживает паузу debounce после сигнала; false — контекст
// отменён.
func (s *store[T]) debounceWait(ctx context.Context) bool {
	if s.opts.debounce <= 0 {
		return true
	}
	timer := time.NewTimer(s.opts.debounce)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// drain забирает без блокировки всё, что скопилось в канале: пачка сигналов
// схлопывается в одну перечитку.
func drain[E any](ch <-chan E) {
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
		default:
			return
		}
	}
}

func (s *store[T]) reportError(err error) {
	if s.opts.onError != nil {
		s.opts.onError(err)
	}
}
