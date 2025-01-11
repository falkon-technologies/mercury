package mercury

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func testResolver(id int, delay time.Duration) ResolverFunc {
	return func(ctx context.Context, message *Message) error {
		fmt.Printf("Resolver %d recebeu mensagem: %v\n", id, message.Data)

		if delay > 0 {
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				fmt.Printf("Resolver %d cancelado: %v\n", id, ctx.Err())
				return ctx.Err()
			}
		}

		return nil
	}
}

func TestMercuryEngine(t *testing.T) {
	Initialize()

	dispatcher1, err := PushResolver("event1", testResolver(1, 0))
	if err != nil {
		t.Fatalf("Erro ao registrar resolver: %v", err)
	}

	dispatcher2, err := PushResolver("event2", testResolver(2, time.Second*10))
	if err != nil {
		t.Fatalf("Erro ao registrar resolver: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	msg1 := &Message{
		Key:  "event1",
		Data: map[string]any{"payload": "dados para resolver 1"},
	}
	msg2 := &Message{
		Key:  "event2",
		Data: map[string]any{"payload": "dados para resolver 2"},
	}

	dispatcher1.Dispatch(ctx, msg1)
	dispatcher2.Dispatch(ctx, msg2)

	time.Sleep(10 * time.Second)
}

func noopResolver(ctx context.Context, message *Message) error {
	return nil
}

func TestConcurrentOperations(t *testing.T) {
	Initialize()

	const routines = 100
	var wg sync.WaitGroup

	for i := 0; i < routines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("event-%d", i)
			_, err := PushResolver(key, noopResolver)
			if err != nil {
				t.Logf("Erro ao registrar resolver: %v", err)
			}
		}(i)
	}

	wg.Wait()

	for i := 0; i < routines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var dispatcher *ProcessDispatcher
			var err error
			if i%2 == 0 {
				dispatcher, err = PushResolver("sharedEvent", noopResolver)
			} else {
				dispatcher, err = PushResolver("sharedEvent", noopResolver, &ResolverConfig{
					AttachOnProcess:  "",
					OverrideResolver: true,
				})
			}
			if err != nil {
				t.Logf("Erro no dispatcher: %v", err)
				return
			}

			msg := &Message{
				Key:  "sharedEvent",
				Data: map[string]any{"iteration": i},
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			dispatcher.Dispatch(ctx, msg)
		}(i)
	}

	wg.Wait()

	time.Sleep(3 * time.Second)
}
