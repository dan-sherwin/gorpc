# Client streaming: chunk upload and final response

The caller sends 32 KiB chunks. The handler hashes them as they arrive and
returns a final byte count, chunk count, and SHA-256 checksum. The caller checks
all three against what it sent.

## Run

From the repository root:

~~~sh
go run ./examples/clientstream
~~~

Expected output for the built-in sample:

~~~text
uploaded 300000 bytes in 10 chunks
sha256 verified
~~~

To read an actual file instead:

~~~sh
go run ./examples/clientstream -file /path/to/input.bin
~~~

The receiver only counts and hashes data in memory; it does not save or modify
the file. Empty files work too. Both ends run locally, select their own port,
and stop after the transfer, with a thirty-second overall deadline.

## What to notice

- [main.go](main.go) reads through a reusable buffer rather than loading the
  selected file into memory. Only the small built-in sample starts in memory.
- `Send` encodes an item before returning, so that buffer can then be reused.
- The receiver grants two items and 128 KiB of encoded payload capacity.
  Each item also fits the 96 KiB frame limit, including encoding overhead.
- `CloseAndRecv` ends the request items and receives the handler's final response.
- A deferred `Cancel` cleans up if reading the file or sending an item fails.
- The final checksum is an application-level receipt, unlike credit returns,
  which only free queue capacity.

This is not a durable or resumable file-transfer protocol. There is no storage
commit, retry, file identity, or resume offset. A production upload service must
define those and enforce its own total-size and authorization limits. A context
also cannot interrupt an arbitrary blocking `io.Reader`; the source needs its
own cancellation mechanism when that matters.

See [client streaming](../../docs/streaming.md#client-streaming).
