package gorpc

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
)

// CompressionGzip is the built-in gzip compressor name used during handshake.
const CompressionGzip = "gzip"

// Compressor compresses and decompresses frame payloads after the GoRPC
// handshake negotiates a matching compressor on both peers.
type Compressor interface {
	Name() string
	Compress(data []byte) ([]byte, error)
	Decompress(data []byte) ([]byte, error)
}

type gzipCompressor struct{}

// GzipCompression returns the built-in gzip compressor.
func GzipCompression() Compressor {
	return gzipCompressor{}
}

func (gzipCompressor) Name() string {
	return CompressionGzip
}

func (gzipCompressor) Compress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	writer := gzip.NewWriter(&buf)
	if _, err := writer.Write(data); err != nil {
		_ = writer.Close()
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (gzipCompressor) Decompress(data []byte) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = reader.Close()
	}()

	return io.ReadAll(reader)
}

func normalizeCompressor(compressor Compressor) Compressor {
	if compressor == nil {
		return nil
	}
	if compressor.Name() == "" {
		return nil
	}

	return compressor
}

func compressorName(compressor Compressor) string {
	if compressor == nil {
		return ""
	}

	return compressor.Name()
}

func ensureCompressor(name string, compressor Compressor) error {
	if name == "" {
		return nil
	}
	if compressor == nil {
		return fmt.Errorf("%w: unsupported compression %q", ErrProtocol, name)
	}
	if compressor.Name() != name {
		return fmt.Errorf("%w: unsupported compression %q", ErrProtocol, name)
	}

	return nil
}
