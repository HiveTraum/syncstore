package syncstoreredis_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/HiveTraum/syncstore/syncstoreredis"
)

// Интеграционные тесты: полный цикл «данные → хранилище → PUBLISH → перечитка»
// на живом Redis. Требуют SYNCSTORE_TEST_REDIS_ADDR.

func newTestClient(t *testing.T) *redis.Client {
	t.Helper()
	addr := os.Getenv("SYNCSTORE_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("SYNCSTORE_TEST_REDIS_ADDR не задан — интеграционный тест пропущен")
	}
	client := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { client.Close() })
	return client
}

// дожидаемся установленной подписки, иначе PUBLISH уйдёт в пустоту
func waitForSubscribed(t *testing.T, ctx context.Context, client *redis.Client, channel string) {
	t.Helper()
	waitFor(t, ctx, func() bool {
		chs, err := client.PubSubChannels(ctx, channel).Result()
		return err == nil && len(chs) == 1
	}, "подписка Pub/Sub установлена")
}

func TestSourceFuncSyncsOverRedis(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client := newTestClient(t)
	if err := client.Del(ctx, "syncstore_it_value").Err(); err != nil {
		t.Fatal(err)
	}

	load := func(ctx context.Context, client redis.UniversalClient) (string, error) {
		v, err := client.Get(ctx, "syncstore_it_value").Result()
		if errors.Is(err, redis.Nil) {
			return "", nil
		}
		return v, err
	}

	st := syncstoreredis.New(client, "syncstore_it_changed", load)
	go st.Run(ctx)

	initial, err := st.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if initial != "" {
		t.Fatalf("ожидали пустое начальное состояние, получили %q", initial)
	}

	waitForSubscribed(t, ctx, client, "syncstore_it_changed")

	if err := client.Set(ctx, "syncstore_it_value", "hello", 0).Err(); err != nil {
		t.Fatal(err)
	}
	// сигналим методом самого хранилища — notifier драйвер подключил сам
	if err := st.Notify(ctx); err != nil {
		t.Fatal(err)
	}

	waitFor(t, ctx, func() bool {
		v, err := st.Get(ctx)
		return err == nil && v == "hello"
	}, "значение доехало до хранилища по PUBLISH")
}

func TestHashSourceSyncsOverRedis(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client := newTestClient(t)
	if err := client.Del(ctx, "syncstore_it_hash").Err(); err != nil {
		t.Fatal(err)
	}

	st := syncstoreredis.New(client, "syncstore_it_hash_changed",
		syncstoreredis.Hash("syncstore_it_hash"),
		syncstoreredis.WithReconnectDelay(time.Second),
	)
	go st.Run(ctx)

	initial, err := st.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(initial) != 0 {
		t.Fatalf("ожидали пустое начальное состояние, получили %v", initial)
	}

	waitForSubscribed(t, ctx, client, "syncstore_it_hash_changed")

	if err := client.HSet(ctx, "syncstore_it_hash", "greeting", "hello").Err(); err != nil {
		t.Fatal(err)
	}
	// сигнал без хранилища — как из сервиса, который только пишет
	if err := syncstoreredis.Notify(ctx, client, "syncstore_it_hash_changed"); err != nil {
		t.Fatal(err)
	}

	waitFor(t, ctx, func() bool {
		m, err := st.Get(ctx)
		return err == nil && m["greeting"] == "hello"
	}, "значение доехало до хранилища по PUBLISH")
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
