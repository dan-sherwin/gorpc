package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"
)

func TestReconnect(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	var out bytes.Buffer
	if err := run(ctx, &out); err != nil {
		t.Fatal(err)
	}
	want := "worker called catalog\ncatalog called worker over the same connection\nactive stream failed: unavailable\nnew call succeeded after reconnect; old endpoint stayed unavailable\nwatch handler started once; no automatic replay\n"
	if out.String() != want {
		t.Fatalf("output:\n%s\nwant:\n%s", &out, want)
	}
}

func TestCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := run(ctx, io.Discard); !errors.Is(err, context.Canceled) {
		t.Fatalf("run = %v, want context.Canceled", err)
	}
}
