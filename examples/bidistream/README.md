# Bidirectional streaming: send and receive together

The caller submits words while receiving uppercase results. After sending its
last word, it half-closes its send direction. The handler can still return a
final summary before ending its own send direction.

## Run

From the repository root:

~~~sh
go run ./examples/bidistream
~~~

Expected output:

~~~text
1: ALPHA
2: BRAVO
3: CHARLIE
4: DELTA
5: ECHO
6: FOXTROT
completed 6 items after CloseSend
~~~

The program runs both ends locally, chooses its own port, and has a ten-second
deadline. No other server is needed.

## What to notice

- [main.go](main.go) uses one sending goroutine and one receiving goroutine.
  Each direction has a two-item receive window.
- Sending everything before starting to receive can deadlock if both sides
  fill their windows. Keep receiving while sending.
- `CloseSend` means “I have no more items to send,” not “close this stream.”
  The receiver reads the summary and then `io.EOF`.
- Either side's failure cancels the work. Cleanup joins the sending goroutine
  instead of leaving it behind.
- Result IDs preserve application ordering. Multiple concurrent senders would
  need their own ordering policy.

Tests also cover no items and 200 items, well beyond either receive window.
See [bidirectional streaming](../../docs/streaming.md#bidirectional-streaming).
