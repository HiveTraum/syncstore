// Package syncstorepgx — Postgres-драйвер syncstore: собирает готовое
// хранилище, которое перечитывает значение по NOTIFY (LISTEN/NOTIFY
// поверх pgx).
package syncstorepgx

import (
	"context"
	"fmt"
	"time"

	"github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/HiveTraum/syncstore"
)

const defaultReconnectDelay = 5 * time.Second

// Source читает актуальное значение целиком; pool — тот же, что передан New,
// чтобы не таскать его замыканием.
type Source[T any] func(ctx context.Context, pool *pgxpool.Pool) (T, error)

// Table — источник «таблица целиком»: SELECT * FROM table, строки сканируются
// в структуру E по именам колонок (pgx.RowToStructByName), значение
// хранилища — []E. Опциональные условия where (squirrel) объединяются в WHERE
// через AND:
//
//	Table[Rate]("rates", squirrel.Eq{"active": true})
//
// Для выборки сложнее (join, свои колонки) передайте в New свою функцию-Source.
func Table[E any](table string, where ...squirrel.Sqlizer) Source[[]E] {
	return func(ctx context.Context, pool *pgxpool.Pool) ([]E, error) {
		q := squirrel.Select("*").
			From(pgx.Identifier{table}.Sanitize()).
			PlaceholderFormat(squirrel.Dollar)
		for _, w := range where {
			q = q.Where(w)
		}
		sql, args, err := q.ToSql()
		if err != nil {
			return nil, fmt.Errorf("build select %s: %w", table, err)
		}
		rows, err := pool.Query(ctx, sql, args...)
		if err != nil {
			return nil, fmt.Errorf("select %s: %w", table, err)
		}
		return pgx.CollectRows(rows, pgx.RowToStructByName[E])
	}
}

// Option настраивает хранилище, созданное New.
type Option func(*options)

type options struct {
	reconnectDelay time.Duration
	onError        func(error)
	storeOpts      []syncstore.Option
}

// WithReconnectDelay задаёт паузу перед переподключением после обрыва.
// По умолчанию 5 секунд.
func WithReconnectDelay(d time.Duration) Option {
	return func(o *options) { o.reconnectDelay = d }
}

// WithOnError задаёт обработчик ошибок (логи, метрики): сюда попадают и
// обрывы соединения-слушателя, и ошибки фоновых перечиток. По умолчанию
// ошибки молча переживаются переподключением и повтором.
func WithOnError(fn func(error)) Option {
	return func(o *options) { o.onError = fn }
}

// WithStoreOptions пробрасывает опции ядра (syncstore.WithDebounce,
// syncstore.WithRetryInterval и т.п.) в создаваемое хранилище.
func WithStoreOptions(opts ...syncstore.Option) Option {
	return func(o *options) { o.storeOpts = append(o.storeOpts, opts...) }
}

// New собирает хранилище: LISTEN на channel, по каждому NOTIFY значение
// перечитывается через source. Один New слушает один канал; на другой
// канал — отдельное хранилище.
//
// Запуск и чтение — по контракту [syncstore.Store]:
//
//	go store.Run(ctx)
//	v, err := store.Get(ctx)
func New[T any](pool *pgxpool.Pool, channel string, source Source[T], opts ...Option) syncstore.Store[T] {
	o := options{reconnectDelay: defaultReconnectDelay}
	for _, opt := range opts {
		opt(&o)
	}
	storeOpts := o.storeOpts
	if o.onError != nil {
		// явный syncstore.WithOnError в storeOpts применится позже и победит
		storeOpts = append([]syncstore.Option{syncstore.WithOnError(o.onError)}, storeOpts...)
	}
	l := &listener{
		pool:           pool,
		channel:        channel,
		reconnectDelay: o.reconnectDelay,
		onError:        o.onError,
	}
	load := func(ctx context.Context) (T, error) { return source(ctx, pool) }
	return syncstore.New(l, load, storeOpts...)
}

var _ syncstore.Listener = (*listener)(nil)

// listener реализует syncstore.Listener: держит соединение из пула на
// LISTEN <channel> и доставляет каждый NOTIFY как сигнал-триггер.
//
// Соединение занимает один слот пула на всё время работы — пул должен быть
// рассчитан на это.
//
// Уведомления, отправленные за время обрыва соединения, Postgres не хранит,
// поэтому после каждого переподключения listener выдаёт синтетический сигнал —
// хранилище перечитает данные и догонит пропущенное.
type listener struct {
	pool           *pgxpool.Pool
	channel        string
	reconnectDelay time.Duration
	onError        func(error)
}

// Listen запускает прослушивание в фоне и возвращает канал сигналов;
// соединение переустанавливается после обрывов. Канал закрывается по
// отмене ctx.
func (l *listener) Listen(ctx context.Context) (<-chan struct{}, error) {
	signals := make(chan struct{}, 1)
	go func() {
		defer close(signals)
		resumed := false
		for {
			err := l.listenOnce(ctx, signals, resumed)
			if ctx.Err() != nil {
				return
			}
			if l.onError != nil {
				l.onError(err)
			}
			resumed = true
			select {
			case <-ctx.Done():
				return
			case <-time.After(l.reconnectDelay):
			}
		}
	}()
	return signals, nil
}

func (l *listener) listenOnce(ctx context.Context, signals chan<- struct{}, resumed bool) error {
	poolConn, err := l.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire: %w", err)
	}
	// соединение с активным LISTEN нельзя возвращать в пул — чужой запрос
	// получил бы наши уведомления; забираем его из пула и закрываем
	conn := poolConn.Hijack()
	defer conn.Close(context.Background())

	if _, err := conn.Exec(ctx, "LISTEN "+pgx.Identifier{l.channel}.Sanitize()); err != nil {
		return fmt.Errorf("listen %s: %w", l.channel, err)
	}
	if resumed {
		// уведомления за время обрыва потеряны — заставляем перечитать
		if !send(ctx, signals) {
			return ctx.Err()
		}
	}
	for {
		if _, err := conn.WaitForNotification(ctx); err != nil {
			return fmt.Errorf("wait for notification: %w", err)
		}
		if !send(ctx, signals) {
			return ctx.Err()
		}
	}
}

// send доставляет сигнал, не зависая на отменённом контексте; false — ctx
// отменён.
func send(ctx context.Context, signals chan<- struct{}) bool {
	select {
	case signals <- struct{}{}:
		return true
	case <-ctx.Done():
		return false
	}
}
