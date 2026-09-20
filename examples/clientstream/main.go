// Package main uploads chunks and verifies the receiver's final checksum.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"time"

	"github.com/dan-sherwin/gorpc"
)

const chunkSize = 32 * 1024

type chunk struct {
	Data []byte
}

type summary struct {
	Bytes  int64
	Chunks int
	SHA256 []byte
}

func main() {
	file := flag.String("file", "", "file to upload; empty uses sample data")
	flag.Parse()
	if err := execute(*file); err != nil {
		log.Fatal(err)
	}
}

func execute(path string) error {
	var source io.Reader = strings.NewReader(strings.Repeat("gorpc\n", 50000))
	if path != "" {
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer func() { _ = file.Close() }()
		source = file
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return run(ctx, source, os.Stdout)
}

func receiveUpload(_ *gorpc.Context, stream *gorpc.StreamReader[chunk]) (summary, error) {
	hash := sha256.New()
	var result summary
	for {
		value, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			result.SHA256 = hash.Sum(nil)
			return result, nil
		}
		if err != nil {
			return summary{}, err
		}
		_, _ = hash.Write(value.Data)
		result.Bytes += int64(len(value.Data))
		result.Chunks++
	}
}

func run(ctx context.Context, source io.Reader, out io.Writer) (err error) {
	server := gorpc.NewServer(gorpc.ServerOptions{
		MaxFrameSize:  96 * 1024,
		StreamOptions: gorpc.StreamOptions{RecvBuffer: 2, RecvBytes: 128 * 1024},
	})
	gorpc.MustRegisterClientStream(server, "upload", receiveUpload)

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

	client := gorpc.NewTCPClient(listener.Addr().String(), "uploader")
	defer func() { _ = client.Close() }()
	if err := client.Connect(ctx); err != nil {
		return err
	}
	stream, err := gorpc.ClientStream[chunk, summary](ctx, client, "upload")
	if err != nil {
		return err
	}
	defer func() { _ = stream.Cancel() }()

	hash := sha256.New()
	buffer := make([]byte, chunkSize)
	var sent int64
	chunks := 0
	for {
		n, readErr := source.Read(buffer)
		if n > 0 {
			if err := stream.Send(chunk{Data: buffer[:n]}); err != nil {
				return err
			}
			// Send has encoded the item; the buffer can be reused.
			_, _ = hash.Write(buffer[:n])
			sent += int64(n)
			chunks++
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return readErr
		}
	}
	result, err := stream.CloseAndRecv()
	if err != nil {
		return err
	}
	if result.Bytes != sent || result.Chunks != chunks || !bytes.Equal(result.SHA256, hash.Sum(nil)) {
		return errors.New("receiver's byte count, chunk count, or checksum differs")
	}
	if _, err := fmt.Fprintf(out, "uploaded %d bytes in %d chunks\nsha256 verified\n", result.Bytes, result.Chunks); err != nil {
		return err
	}
	return nil
}
