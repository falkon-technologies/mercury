package mercury

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
)

type (
	PID          string
	ResolverFunc func(ctx context.Context, message *Message) error
	ErrorFunc    func(ctx context.Context, err error)

	ProcessDispatcher struct {
		pid PID
	}

	Message struct {
		Ctx      context.Context
		pid      PID
		Key      string
		Data     map[string]any
		waitChan chan error
	}

	ResolverConfig struct {
		AttachOnProcess  PID
		OverrideResolver bool
	}

	EngineConfig struct {
		ErrorFunc ErrorFunc
	}

	Process struct {
		pid      PID
		resolver map[string]ResolverFunc
		channel  chan *Message
	}

	Engine struct {
		processMutex sync.RWMutex
		processes    map[PID]*Process
		mChannel     chan *Message
		config       EngineConfig
	}
)

var (
	engine              *Engine
	engineOnce          sync.Once
	defaultEngineConfig = EngineConfig{
		ErrorFunc: func(ctx context.Context, err error) {
			slog.Default().Error(err.Error())
		},
	}
)

func Initialize(config ...EngineConfig) {
	engineOnce.Do(func() {
		_config := defaultEngineConfig
		if len(config) > 0 && config[0].ErrorFunc != nil {
			_config.ErrorFunc = config[0].ErrorFunc
		}

		engine = &Engine{
			processes: make(map[PID]*Process),
			mChannel:  make(chan *Message, 100),
			config:    _config,
		}

		setup()
	})
}

func setup() {
	go func() {
		for {
			select {
			case m := <-engine.mChannel:
				engine.processMutex.RLock() // Leitura simultânea segura
				if m.pid != "" {
					p := engine.processes[m.pid]
					if p != nil {
						slog.Default().Info("message received", "pid", p.pid, "key", m.Key)
						p.channel <- m
					}
				} else {
					for _, p := range engine.processes {
						if _, ok := p.resolver[m.Key]; ok {
							slog.Default().Info("message received", "pid", p.pid, "key", m.Key)
							p.channel <- m
						}
					}
				}
				engine.processMutex.RUnlock()
			}
		}
	}()
}

func (pd *ProcessDispatcher) Dispatch(ctx context.Context, m *Message) {
	m.pid = pd.pid
	m.Ctx = ctx
	PushMessage(m)
}

func (pd *ProcessDispatcher) PID() PID {
	return pd.pid
}

func (p *Process) listen() {
	defer func() {
		if r := recover(); r != nil {
			err, ok := r.(error)
			if !ok {
				err = fmt.Errorf("panic: %v", r)
			}
			engine.config.ErrorFunc(context.Background(), err)
			go p.listen()
		}
	}()

	for {
		select {
		case m := <-p.channel:
			if resolver, ok := p.resolver[m.Key]; ok {
				err := resolver(m.Ctx, m)

				if m.waitChan != nil {
					m.waitChan <- err
				} else if err != nil {
					engine.config.ErrorFunc(m.Ctx, err)
				}
			}
		}
	}
}

func PushMessage(m *Message) {
	engine.mChannel <- m
}

func WaitingPushMessage(m *Message) error {
	waitChan := make(chan error, 1)
	m.waitChan = waitChan

	PushMessage(m)

	err := <-waitChan

	return err
}

func PushResolver(key string, resolver ResolverFunc, config ...*ResolverConfig) (*ProcessDispatcher, error) {
	if len(config) > 0 && config[0].AttachOnProcess != "" {
		pid := config[0].AttachOnProcess

		engine.processMutex.Lock()
		defer engine.processMutex.Unlock()

		process := engine.processes[pid]
		if process == nil {
			return nil, fmt.Errorf("process not found for pid [%s]", pid)
		}

		if !config[0].OverrideResolver {
			if _, ok := process.resolver[key]; ok {
				return nil, fmt.Errorf("resolver already exists for key [%s]", key)
			}
		}

		process.resolver[key] = resolver
		return &ProcessDispatcher{pid: pid}, nil
	}

	pid := GeneratePID()

	engine.processMutex.Lock()
	defer engine.processMutex.Unlock()

	engine.processes[pid] = &Process{
		pid: pid,
		resolver: map[string]ResolverFunc{
			key: resolver,
		},
		channel: make(chan *Message, 10),
	}

	var wg sync.WaitGroup
	wg.Add(1)

	go func(p *Process) {
		wg.Done()
		p.listen()
	}(engine.processes[pid])

	wg.Wait()

	return &ProcessDispatcher{pid: pid}, nil
}
