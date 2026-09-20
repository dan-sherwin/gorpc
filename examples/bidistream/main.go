// Package main sends work and receives results concurrently on one stream.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"time"

	"github.com/dan-sherwin/gorpc"
)

type work struct {
	ID   int
	Text string
}

type result struct {
	ID    int
	Text  string
	Done  bool
	Count int
}

func process(_ *gorpc.Context, stream *gorpc.BidiStreamHandle[result, work]) error {
	count := 0
	for {
		value, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			// The caller stopped sending, but can still receive this summary.
			return stream.Send(result{Done: true, Count: count})
		}
		if err != nil {
			return err
		}
		if err := stream.Send(result{ID: value.ID, Text: strings.ToUpper(value.Text)}); err != nil {
			return err
		}
		count++
	}
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := run(ctx, []string{"alpha", "bravo", "charlie", "delta", "echo", "foxtrot"}, os.Stdout); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, words []string, out io.Writer) (err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	windows := gorpc.StreamOptions{RecvBuffer: 2, RecvBytes: 1024}
	server := gorpc.NewServer(gorpc.ServerOptions{StreamOptions: windows})
	gorpc.MustRegisterBidiStream(server, "uppercase", process)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	served := make(chan error, 1)
	go func() { served <- server.ServeListener(listener) }()
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		err = errors.Join(err, server.Shutdown(shutdownCtx))
		select {
		case serveErr := <-served:
			if !errors.Is(serveErr, gorpc.ErrClosed) {
				err = errors.Join(err, serveErr)
			}
		case <-shutdownCtx.Done():
			err = errors.Join(err, shutdownCtx.Err())
		}
	}()

	client := gorpc.NewTCPClient(listener.Addr().String(), "worker", gorpc.ClientOptions{StreamOptions: windows})
	defer func() { _ = client.Close() }()
	if err := client.Connect(ctx); err != nil {
		return err
	}
	stream, err := gorpc.BidiStream[work, result](ctx, client, "uppercase")
	if err != nil {
		return err
	}
	sent := make(chan error, 1)
	go func() {
		sendErr := sendWords(stream, words)
		if sendErr != nil {
			cancel()
		}
		sent <- sendErr
	}()
	defer func() {
		cancel()
		_ = stream.Cancel()
		// Don't leave the sender running when receiving fails or stops early.
		err = errors.Join(err, <-sent)
	}()

	count := 0
	gotSummary := false
	for {
		value, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			if !gotSummary || count != len(words) {
				return errors.New("stream ended without all results and a final summary")
			}
			return nil
		}
		if err != nil {
			return err
		}
		if value.Done {
			if gotSummary || value.Count != count {
				return errors.New("invalid final summary")
			}
			gotSummary = true
			if _, err := fmt.Fprintf(out, "completed %d items after CloseSend\n", value.Count); err != nil {
				return err
			}
			continue
		}
		if gotSummary || count >= len(words) || value.ID != count+1 || value.Text != strings.ToUpper(words[count]) {
			return errors.New("unexpected result or result order")
		}
		count++
		if _, err := fmt.Fprintf(out, "%d: %s\n", value.ID, value.Text); err != nil {
			return err
		}
	}
}

func sendWords(stream *gorpc.BidiStreamHandle[work, result], words []string) error {
	for i, word := range words {
		if err := stream.Send(work{ID: i + 1, Text: word}); err != nil {
			return err
		}
	}
	return stream.CloseSend()
}
