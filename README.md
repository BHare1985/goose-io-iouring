# goose-io-iouring

The io_uring event loop for [goose-io](https://github.com/BHare1985/goose-io), on real sockets, Linux only (5.1 or newer; liburing). One `iouring::Loop` is one thread's event loop. It supplies the
goose-io contract's core and the half-close, sizing, abort, inspection, batching and shared-listen capabilities (not unix, relay, peek, revoke, wake or limits), and passes the conformance suite
on them. It is [goose-io-epoll](https://github.com/BHare1985/goose-io-epoll) with the kernel interface changed and nothing else, so it supplies the same capabilities. Names are resolved beside the loop: a layer in
[goose-io-dns](https://github.com/BHare1985/goose-io-dns) adds the name-resolution capability to any loop.

- **io_uring, as readiness.** Each watched socket has one single-shot `IORING_OP_POLL_ADD` in flight; a completion is one event, and the
  descriptor is armed again at the next wait, so a still-ready socket is reported again, the level-triggered behaviour the engine is written
  for. Arming, removing and the wait go to the kernel in one `io_uring_enter`. The reads, writes, accepts and connects are still the
  non-blocking socket calls; moving them onto the ring (multishot accept, provided buffers, linked timeouts, as c_proxy does) is the work
  of goose-io-iouring-ext, not of this example.

This is an example event loop: read it to see what a conforming loop is. [goose-io-iouring-ext](https://github.com/BHare1985/goose-io-iouring-ext) is this loop with
the extra functionality a performant TCP proxy needs.

Self-contained: it fetches goose-io on the branch `apply-name-resolution`, contract 0.5, until it is tagged (`import goose_io from "github.com/BHare1985/goose-io@apply-name-resolution"`) with `goose fetch`. Needs Goose v2026.4,
a C compiler, liburing (`liburing-dev`), Python 3 for the network tests.

```bash
./build.sh http_hello.goose http_hello          # goose run cannot link the C layer: programs are built
./conformance.sh --repeat 3                     # the suite, every part of the core and each capability
net/run.sh                                      # the network tests on real peers (REPEAT=20 net/run.sh limits)
```

| File | |
|---|---|
| `engine.goose` | the `Loop`: links, buffers, send queue, dial, timeouts, serve |
| `messages.goose` | several loops, one per thread: `spawn`, `drain`, statistics |
| `address.goose`, `config.goose`, `sys.goose`, `src/sys.c` | addresses, limits, the C layer |
| `conformance.goose` | the runner of the suite, with its fixture |
| `tests/` | programs the network tests drive (and `capabilities_test.goose`, which checks the declared capabilities); `net/` the drivers |

Performance gates against tcprouter's own engine live with goose-io (`bench/`). Differences from tcprouter's engine: a slot is freed after its
`Closed` is handled, `Writable` comes once the largest refused send fits, `close` never resets a link with unread input, and a Tick comes at
least every 90 ms.

