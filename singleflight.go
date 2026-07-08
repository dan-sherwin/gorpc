package gorpc

import (
	"context"
	"encoding/base64"
	"sync"
)

type singleflightGroup struct {
	mu    sync.Mutex
	calls map[string]*singleflightCall
}

type singleflightCall struct {
	done  chan struct{}
	frame Frame
	err   error
}

func (g *singleflightGroup) do(ctx context.Context, key string, fn func() (Frame, error)) (Frame, error) {
	ctx = normalizeContext(ctx)
	if key == "" {
		return fn()
	}

	g.mu.Lock()
	if g.calls == nil {
		g.calls = make(map[string]*singleflightCall)
	}
	if call := g.calls[key]; call != nil {
		g.mu.Unlock()
		select {
		case <-call.done:
			return call.frame, call.err
		case <-ctx.Done():
			return Frame{}, ctx.Err()
		}
	}

	call := &singleflightCall{done: make(chan struct{})}
	g.calls[key] = call
	g.mu.Unlock()

	call.frame, call.err = fn()

	g.mu.Lock()
	delete(g.calls, key)
	g.mu.Unlock()

	close(call.done)

	return call.frame, call.err
}

func singleflightKey(codec Codec, function string, key string, req any) (string, error) {
	if key != "" {
		return function + "\x00" + key, nil
	}

	payload, err := defaultCodec(codec).Marshal(req)
	if err != nil {
		return "", err
	}

	return function + "\x00" + base64.RawStdEncoding.EncodeToString(payload), nil
}
