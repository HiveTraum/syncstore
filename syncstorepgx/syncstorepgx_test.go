package syncstorepgx_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/HiveTraum/syncstore/syncstorepgx"
)

// Интеграционные тесты: полный цикл «таблица → хранилище → NOTIFY → перечитка»
// на живом Postgres. Требуют SYNCSTORE_TEST_DATABASE_URL.

func newTestPool(t *testing.T, ctx context.Context) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("SYNCSTORE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("SYNCSTORE_TEST_DATABASE_URL не задан — интеграционный тест пропущен")
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func resetTable(t *testing.T, ctx context.Context, pool *pgxpool.Pool, table string) {
	t.Helper()
	if _, err := pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS `+table+` (k text PRIMARY KEY, v text NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `TRUNCATE `+table); err != nil {
		t.Fatal(err)
	}
}

// дожидаемся установленной подписки, иначе NOTIFY уйдёт в пустоту
func waitForListen(t *testing.T, ctx context.Context, pool *pgxpool.Pool, channel string) {
	t.Helper()
	waitFor(t, ctx, func() bool {
		var n int
		err := pool.QueryRow(ctx,
			`SELECT count(*) FROM pg_stat_activity WHERE query ILIKE 'listen %'||$1||'%'`,
			channel).Scan(&n)
		return err == nil && n >= 1
	}, "подписка LISTEN установлена")
}

func TestSourceFuncSyncsOverPostgres(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool := newTestPool(t, ctx)
	resetTable(t, ctx, pool, "syncstore_it")

	load := func(ctx context.Context, pool *pgxpool.Pool) (map[string]string, error) {
		rows, err := pool.Query(ctx, `SELECT k, v FROM syncstore_it`)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		m := map[string]string{}
		for rows.Next() {
			var k, v string
			if err := rows.Scan(&k, &v); err != nil {
				return nil, err
			}
			m[k] = v
		}
		return m, rows.Err()
	}

	st := syncstorepgx.New(pool, "syncstore_it_changed", load)
	go st.Run(ctx)

	initial, err := st.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(initial) != 0 {
		t.Fatalf("ожидали пустое начальное состояние, получили %v", initial)
	}

	waitForListen(t, ctx, pool, "syncstore_it_changed")

	if _, err := pool.Exec(ctx, `INSERT INTO syncstore_it VALUES ('greeting', 'hello')`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `SELECT pg_notify('syncstore_it_changed', '')`); err != nil {
		t.Fatal(err)
	}

	waitFor(t, ctx, func() bool {
		m, err := st.Get(ctx)
		return err == nil && m["greeting"] == "hello"
	}, "значение доехало до хранилища по NOTIFY")
}

func TestTableSourceSyncsOverPostgres(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool := newTestPool(t, ctx)
	resetTable(t, ctx, pool, "syncstore_it_rows")

	type row struct{ K, V string }
	st := syncstorepgx.New(pool, "syncstore_it_rows_changed",
		syncstorepgx.Table[row]("syncstore_it_rows", squirrel.Eq{"k": "greeting"}),
		syncstorepgx.WithReconnectDelay(time.Second),
	)
	go st.Run(ctx)

	initial, err := st.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(initial) != 0 {
		t.Fatalf("ожидали пустое начальное состояние, получили %v", initial)
	}

	waitForListen(t, ctx, pool, "syncstore_it_rows_changed")

	if _, err := pool.Exec(ctx, `INSERT INTO syncstore_it_rows VALUES ('greeting', 'hello'), ('noise', 'skip me')`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `SELECT pg_notify('syncstore_it_rows_changed', '')`); err != nil {
		t.Fatal(err)
	}

	// фильтр squirrel должен отсечь строку 'noise'
	waitFor(t, ctx, func() bool {
		rs, err := st.Get(ctx)
		return err == nil && len(rs) == 1 && rs[0] == row{K: "greeting", V: "hello"}
	}, "отфильтрованное значение доехало до хранилища по NOTIFY")
}

func waitFor(t *testing.T, ctx context.Context, cond func() bool, msg string) {
	t.Helper()
	for {
		if cond() {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("не дождались: " + msg)
		case <-time.After(20 * time.Millisecond):
		}
	}
}
