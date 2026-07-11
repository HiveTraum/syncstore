// Package syncstoreredis — Redis-драйвер syncstore: собирает готовое
// хранилище, которое перечитывает значение по сообщению Pub/Sub.
package syncstoreredis

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/HiveTraum/syncstore"
)

const defaultReconnectDelay = 5 * time.Second

// Source читает актуальное значение целиком; client — тот же, что передан
// New, чтобы не таскать его замыканием. Данные не обязаны жить в Redis —
// сигналить можно через Redis, а читать откуда угодно.
type Source[T any] func(ctx context.Context, client redis.UniversalClient) (T, error)

// Hash — источник «hash целиком»: HGETALL key, значение хранилища —
// map поле → значение. Для чтения сложнее (другие структуры, данные вне
// Redis) передайте в New свою функцию-Source.
func Hash(key string) Source[map[string]string] {
	return func(ctx context.Context, client redis.UniversalClient) (map[string]string, error) {
		m, err := client.HGetAll(ctx, key).Result()
		if err != nil {
			return nil, fmt.Errorf("hgetall %s: %w", key, err)
		}
		return m, nil
	}
}

// Option настраивает хранилище, созданное New.
type Option func(*options)

type options struct {
	reconnectDelay time.Duration
	onError        func(error)
	storeOpts      []syncstore.Option
}

// WithReconnectDelay задаёт паузу перед повторной подпиской после обрыва.
// По умолчанию 5 секунд.
func WithReconnectDelay(d time.Duration) Option {
	return func(o *options) { o.reconnectDelay = d }
}

// WithOnError задаёт обработчик ошибок (логи, метрики): сюда попадают и
// обрывы подписки, и ошибки фоновых перечиток. По умолчанию ошибки молча
// переживаются переподпиской и повтором.
func WithOnError(fn func(error)) Option {
	return func(o *options) { o.onError = fn }
}

// WithStoreOptions пробрасывает опции ядра (syncstore.WithDebounce,
// syncstore.WithRetryInterval и т.п.) в создаваемое хранилище.
func WithStoreOptions(opts ...syncstore.Option) Option {
	return func(o *options) { o.storeOpts = append(o.storeOpts, opts...) }
}

// New собирает хранилище: подписка Pub/Sub на channel, по каждому сообщению
// значение перечитывается через source. Один New слушает один канал; на
// другой канал — отдельное хранилище.
//
// Запуск, чтение и сигнал — по контракту [syncstore.Store]:
//
//	go store.Run(ctx)
//	v, err := store.Get(ctx)
//	err = store.Notify(ctx) // PUBLISH channel ""
func New[T any](client redis.UniversalClient, channel string, source Source[T], opts ...Option) syncstore.Store[T] {
	o := options{reconnectDelay: defaultReconnectDelay}
	for _, opt := range opts {
		opt(&o)
	}
	// дефолты драйвера идут первыми: явные опции из WithStoreOptions
	// применятся позже и победят
	base := []syncstore.Option{syncstore.WithNotifier(notifier{rdb: client, channel: channel})}
	if o.onError != nil {
		base = append(base, syncstore.WithOnError(o.onError))
	}
	storeOpts := append(base, o.storeOpts...)
	l := &listener{
		client:         client,
		channel:        channel,
		reconnectDelay: o.reconnectDelay,
		onError:        o.onError,
	}
	load := func(ctx context.Context) (T, error) { return source(ctx, client) }
	return syncstore.New(l, load, storeOpts...)
}

// Publisher публикует сообщение в канал: подходят все клиенты go-redis
// (*redis.Client, *redis.ClusterClient, *redis.Ring) и pipeline.
type Publisher interface {
	Publish(ctx context.Context, channel string, message any) *redis.IntCmd
}

// Notify посылает сигнал «данные изменились» в канал channel. В отличие от
// Postgres, Redis не привязывает сигнал к транзакции — шлите после записи.
func Notify(ctx context.Context, rdb Publisher, channel string) error {
	if err := rdb.Publish(ctx, channel, "").Err(); err != nil {
		return fmt.Errorf("notify %s: %w", channel, err)
	}
	return nil
}

// NewNotifier — syncstore.Notifier поверх rdb для канала channel: для кода,
// которому сигнальщик передаётся зависимостью (например, сервис только пишет).
func NewNotifier(rdb Publisher, channel string) syncstore.Notifier {
	return notifier{rdb: rdb, channel: channel}
}

type notifier struct {
	rdb     Publisher
	channel string
}

func (n notifier) Notify(ctx context.Context) error {
	return Notify(ctx, n.rdb, n.channel)
}

var _ syncstore.Listener = (*listener)(nil)

// listener реализует syncstore.Listener поверх подписки Pub/Sub.
//
// Redis Pub/Sub — at-most-once: сообщения, отправленные за время обрыва,
// теряются. Поэтому после каждого восстановления подписки listener выдаёт
// синтетический сигнал — хранилище перечитает данные и догонит пропущенное.
type listener struct {
	client         redis.UniversalClient
	channel        string
	reconnectDelay time.Duration
	onError        func(error)
}

// Listen запускает прослушивание в фоне и возвращает канал сигналов;
// подписка переустанавливается после обрывов. Канал закрывается по
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

// listenOnce сам управляет жизненным циклом подписки (вместо автопереподписки
// go-redis): обрыв виден как ошибка, и восстановление всегда проходит через
// синтетический сигнал — иначе тихий реконнект скрыл бы потерянные сообщения.
func (l *listener) listenOnce(ctx context.Context, signals chan<- struct{}, resumed bool) error {
	pubsub := l.client.Subscribe(ctx, l.channel)
	defer pubsub.Close()
	// заблокированный ReceiveMessage не реагирует на отмену ctx — будим закрытием
	stop := context.AfterFunc(ctx, func() { pubsub.Close() })
	defer stop()

	if _, err := pubsub.Receive(ctx); err != nil { // подтверждение подписки
		return fmt.Errorf("subscribe %s: %w", l.channel, err)
	}
	if resumed {
		if !send(ctx, signals) {
			return ctx.Err()
		}
	}
	for {
		if _, err := pubsub.ReceiveMessage(ctx); err != nil {
			return fmt.Errorf("receive %s: %w", l.channel, err)
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
