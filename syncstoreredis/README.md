# syncstoreredis

Драйвер [syncstore](../README.md) для Redis: собирает готовое хранилище
(`syncstore.Store`) поверх Pub/Sub средствами
[go-redis](https://github.com/redis/go-redis) — подписка на канал,
переподписка после обрыва, контракт syncstore про синтетический сигнал.

Отдельный Go-модуль (`github.com/HiveTraum/syncstore/syncstoreredis`) — ядро
syncstore не тянет go-redis.

## Использование

Конструктору задаются клиент, канал Pub/Sub и **источник данных** — что
перечитать по сигналу. Простейший источник — hash целиком (`Hash`):

```go
import "github.com/HiveTraum/syncstore/syncstoreredis"

store := syncstoreredis.New(client, "rates_changed", syncstoreredis.Hash("rates"))
go store.Run(ctx)

rates, err := store.Get(ctx) // map[string]string из памяти
```

Данные не обязаны жить в Redis: источником служит просто функция — сигналы
идут через Redis, а читать можно откуда угодно:

```go
store := syncstoreredis.New(client, "rates_changed",
    func(ctx context.Context, client redis.UniversalClient) ([]Rate, error) {
        return queries.AllRates(ctx) // хоть из Postgres
    },
)
```

### Опции

- `WithReconnectDelay(d)` — пауза перед переподпиской после обрыва
  (по умолчанию 5s);
- `WithOnError(fn)` — логирование/метрики: обрывы подписки и ошибки
  фоновых перечиток;
- `WithStoreOptions(opts...)` — опции ядра, например
  `syncstore.WithDebounce(d)`.

## Поведение

- Redis Pub/Sub — **at-most-once**: сообщения, отправленные за время обрыва
  соединения, теряются. Поэтому после каждого восстановления подписки драйвер
  выдаёт синтетический сигнал — хранилище перечитает данные и догонит
  пропущенное.
- Жизненный цикл подписки драйвер ведёт сам (без автопереподписки go-redis):
  тихий реконнект скрыл бы потерянные сообщения.
- Одно хранилище — один канал; на другой канал заводится отдельное
  хранилище.

## Как сигналить об изменениях

Методом самого хранилища (notifier драйвер подключает сам):

```go
err := store.Notify(ctx) // PUBLISH channel ""
```

Писателю без хранилища — `syncstoreredis.NewNotifier(client, "rates_changed")`
(`syncstore.Notifier` для передачи зависимостью) или напрямую
`syncstoreredis.Notify(ctx, client, "rates_changed")`.

В отличие от Postgres, Redis не привязывает сигнал к транзакции записи —
шлите сигнал после записи.

## Разработка

```sh
# интеграционный тест — нужен живой Redis:
docker run -d --name syncstore-redis -p 6379:6379 redis:7-alpine
SYNCSTORE_TEST_REDIS_ADDR='localhost:6379' go test ./...
```

Без `SYNCSTORE_TEST_REDIS_ADDR` интеграционный тест пропускается.
