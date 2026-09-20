# Testing and release checks

The package tests use local sockets and do not need application credentials or
a database. Run the checks listed in the README before proposing a release.

## Examples and documentation

`go test ./...` runs the executable Go documentation examples and the runnable
examples' scenario tests. `go test -race ./examples/... -count=1` covers stream
cancellation, empty and multi-chunk uploads, source read failure, async wait
deadlines, server shutdown, and bidirectional exchanges beyond the receive window.

The integration suite also builds and launches every example command:

```sh
go test -tags=integration ./examples -run TestExampleCommands -count=1
```

These child programs are built with the race detector, use automatically
selected loopback ports, and have overall deadlines. The inventory pair is
tested with and without shared-secret auth, including signal-driven shutdown.
The smoke test compares each program's output with the block in its README;
update the program, test expectations, and documentation together.

Markdown snippets elsewhere are excerpts, not all standalone programs. The
complete runnable sources and executable Go documentation examples are the
checked entry points. When changing the public API, review those excerpts too.

The README and docs describe their checkout. Hosted API documentation follows
published versions; keep unreleased features marked until the release is tagged.

## Compatibility and failure cases

`go test -race -tags=integration ./...` includes the released-peer matrix:
current/rc.2, rc.2/current, current/rc.3, rc.3/current, and current/current.
The fixture uses the common released API, with unary calls, notifications,
all three stream shapes, shared-secret authentication, and gzip in both
directions. Its separate module keeps released dependencies out of the main
package. Building the fixture may require access to the Go module proxy.

Ordinary tests cover bounded receive queues, slow-reader isolation, item and
byte credit, half-close, cancellation, early responses, connection replacement,
and shutdown. Repeat the race suite when changing stream or connection state;
a single passing run gives limited coverage of scheduling-dependent failures.

Frame fuzzing is separate:

```sh
go test -run '^$' -fuzz=FuzzReadFrame -fuzztime=2000000x -parallel=4 -timeout=5m
```

The execution count avoids the timed-fuzz cancellation race in Go 1.26.6
([Go issue 75804](https://go.dev/issue/75804)). The separate test timeout still
limits a stalled run; do not suppress its exit status. A passing count-limited
run does not retroactively turn an earlier timed failure into a pass.

Keep any reported failing input and reproduce it before changing the decoder.
A runner timeout without a failing input is an incomplete run, not a passing
fuzz result.

## Downstream applications

Check actual consumers against both their pinned release and the proposed
GoRPC version. Use temporary module files and a local replacement to test an
unreleased checkout without editing the consumers' dependency pins.

Exercise the application paths, not only compilation:

- Transfers with realistic chunk sizes and content-integrity checks.
- Slow receivers alongside unrelated calls on the same connection.
- Bidirectional streams with concurrent sends and receives.
- Cancellation, deadlines, peer restarts, and connection replacement.
- Repeated connections and transfers, checking retained memory and goroutines
  after work has stopped.

Use disposable local data for database-backed tests. Keep provider calls and
production services outside the validation harness.

## Performance and rollout

`BenchmarkClientStream` measures sustained loopback streaming. Run benchmarks
without the race detector, alternate baseline and candidate measurements, and
compare more than one sample. Include an application's SDK path when its
buffering or encoding adds work. Loopback throughput is not network capacity;
latency, bandwidth, compression, and handler work change the result.

Green local checks justify a release candidate, not a production-readiness
claim. Complete hosted checks, test the candidate in a controlled deployment,
and observe real workloads before a stable release. Keep the old version
available for rollback. Both peers must be upgraded to negotiate flow control;
the mixed-version limits are described in [protocol.md](protocol.md).
