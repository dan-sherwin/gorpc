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

func TestUpload(t *testing.T) {
	for _, size := range []int{0, 1, chunkSize, chunkSize + 1, 300000} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			var out bytes.Buffer
			if err := run(ctx, strings.NewReader(strings.Repeat("x", size)), &out); err != nil {
				t.Fatal(err)
			}
			want := fmt.Sprintf("uploaded %d bytes in %d chunks\nsha256 verified\n", size, (size+chunkSize-1)/chunkSize)
			if out.String() != want {
				t.Fatalf("output = %q, want %q", out.String(), want)
			}
		})
	}
}

type brokenReader struct {
	err error
}

func (r brokenReader) Read([]byte) (int, error) {
	return 0, r.err
}

func TestReadFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	readErr := errors.New("source failed")
	if err := run(ctx, brokenReader{err: readErr}, io.Discard); !errors.Is(err, readErr) {
		t.Fatalf("run = %v, want source error", err)
	}
}

func TestCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := run(ctx, strings.NewReader("data"), io.Discard); !errors.Is(err, context.Canceled) {
		t.Fatalf("run = %v, want context.Canceled", err)
	}
}
