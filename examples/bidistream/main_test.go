package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
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
	words := make([]string, 200)
	if err := run(ctx, words, brokenWriter{}); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("run = %v, want output error", err)
	}
}

func TestExchange(t *testing.T) {
	for _, count := range []int{0, 6, 200} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			words := make([]string, count)
			var want strings.Builder
			for i := range words {
				words[i] = "hello"
				fmt.Fprintf(&want, "%d: HELLO\n", i+1)
			}
			fmt.Fprintf(&want, "completed %d items after CloseSend\n", count)
			var out bytes.Buffer
			if err := run(ctx, words, &out); err != nil {
				t.Fatal(err)
			}
			if out.String() != want.String() {
				t.Fatalf("unexpected output:\n%s", &out)
			}
		})
	}
}

func TestCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := run(ctx, []string{"hello"}, io.Discard); !errors.Is(err, context.Canceled) {
		t.Fatalf("run = %v, want context.Canceled", err)
	}
}
