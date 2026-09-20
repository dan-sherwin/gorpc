package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"
)

type brokenWriter struct{}

func (brokenWriter) Write([]byte) (int, error) {
	return 0, io.ErrClosedPipe
}

func TestOutputFailureStopsWork(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := run(ctx, brokenWriter{}); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("run = %v, want output error", err)
	}
}

func TestRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	var out bytes.Buffer
	if err := run(ctx, &out); err != nil {
		t.Fatal(err)
	}
	want := "unary call while stream is paused: ok\nitem 1\nitem 2\nitem 3\ncanceled after 3 items; handler stopped\n"
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
