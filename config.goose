// What a loop is configured with: plain data, set on the loop before it listens, dials or serves. The names and the
// defaults are tcprouter's `Limits`, less the limits of the proxy (the time allowed for a ClientHello).
namespace iouring;

struct Limits {
    connect_timeout_ms:    i64 = 2000,   // a dial's own limit when it is given connect_ms 0
    idle_timeout_ms:       i64 = 300000, // no byte either way: IdleTimeout
    half_close_timeout_ms: i64 = 30000,  // a half-closed link (peer finished, or shutdown_write): HalfCloseTimeout; a link `close` is waiting on: Local
    max_links:             i64 = 65536,  // links open at once on this loop; a dial past it ends in ConnectFailed { NoResources }, an accepted socket is refused
    buffer_size:           i64 = 32768,  // bytes of one pool buffer: the most one Data carries, and a quarter of max_send
    buffers_per_loop:      i64 = 4096,   // buffers in the pool, grown lazily
    batch_min_events:      i64 = 0,      // batching window, OFF when 0: a pass that finds fewer events than this waits up to batch_max_wait_us for more
    batch_max_wait_us:     i64 = 0,      // microseconds; 0 = off. Never waits past a deadline; a posted message ends the wait at once
    resolve_timeout_ms:    i64 = 5000,   // a name lookup's own limit when resolve is given timeout_ms 0
}
