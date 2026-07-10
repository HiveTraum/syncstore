# syncstorepgx

Драйвер [syncstore](../README.md) для Postgres: собирает готовое хранилище
(`syncstore.Store`) поверх LISTEN/NOTIFY средствами
[pgx](https://github.com/jackc/pgx) — выделенное соединение на LISTEN,
переподключение после обрыва, контракт syncstore про синтетический сигнал.

Отдельный Go-модуль (`github.com/HiveTraum/syncstore/syncstorepgx`) — ядро
syncstore не тянет pgx.

## Использование

Конструктору задаются пул, канал NOTIFY и **источник данных** — что перечитать
по сигналу. Простейший источник — таблица целиком (`Table`): строки
сканируются в структуру по именам колонок, значение хранилища — слайс строк.

```go
import "github.com/HiveTraum/syncstore/syncstorepgx"

type Rate struct {
    Currency string
    Value    float64
}

store := syncstorepgx.New(pool, "rates_changed", syncstorepgx.Table[Rate]("rates"))
go store.Run(ctx)

rates, err := store.Get(ctx) // []Rate из памяти
```

`Table` принимает опциональные условия WHERE — squirrel-выражения,
объединяемые через AND:

```go
import sq "github.com/Masterminds/squirrel"

syncstorepgx.Table[Rate]("rates", sq.Eq{"active": true})
```

Если чтение сложнее, чем `SELECT *` (join, фильтр, map вместо слайса) —
источником служит просто функция:

```go
store := syncstorepgx.New(pool, "rates_changed",
    func(ctx context.Context, pool *pgxpool.Pool) (map[string]Rate, error) {
        return queries.RatesByCurrency(ctx)
    },
)
```

### Опции

- `WithReconnectDelay(d)` — пауза перед переподключением после обрыва
  (по умолчанию 5s);
- `WithOnError(fn)` — логирование/метрики: обрывы соединения и ошибки
  фоновых перечиток;
- `WithStoreOptions(opts...)` — опции ядра, например
  `syncstore.WithDebounce(d)`.

## Поведение

- Держит одно соединение на `LISTEN <channel>`; соединение забирается из пула
  навсегда (hijack) — соединение с активным LISTEN нельзя возвращать в пул,
  иначе чужой запрос получил бы уведомления. Рассчитывайте размер пула с
  учётом одного постоянно занятого слота на каждое хранилище.
- Postgres не хранит уведомления, отправленные за время обрыва соединения,
  поэтому после каждого переподключения драйвер выдаёт синтетический сигнал —
  хранилище перечитает данные и догонит пропущенное.
- Одно хранилище — один канал; на другой канал заводится отдельное
  хранилище.

## Как сигналить об изменениях

Из кода приложения — `SELECT pg_notify('rates_changed', '')` рядом с записью
(в той же транзакции), либо триггером на таблице:

```sql
CREATE OR REPLACE FUNCTION notify_rates_changed() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('rates_changed', '');
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER rates_changed
AFTER INSERT OR UPDATE OR DELETE ON rates
FOR EACH STATEMENT EXECUTE FUNCTION notify_rates_changed();
```

`NOTIFY` доставляется только после коммита транзакции, так что перечитка
всегда видит зафиксированные данные.

## Разработка

```sh
# интеграционный тест — нужен живой Postgres:
docker run -d --name syncstore-pg -e POSTGRES_PASSWORD=syncstore \
  -e POSTGRES_USER=syncstore -e POSTGRES_DB=syncstore -p 5432:5432 postgres:16-alpine
SYNCSTORE_TEST_DATABASE_URL='postgres://syncstore:syncstore@localhost:5432/syncstore?sslmode=disable' \
  go test ./...
```

Без `SYNCSTORE_TEST_DATABASE_URL` интеграционный тест пропускается.
