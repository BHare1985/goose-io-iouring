// Messages to running loops, and draining many loops together. Everything here is for a program that runs several loops, one
// per thread, and stops them from another thread (a signal handler, a control thread). A program with one loop calls `stop`.
// This is tcprouter's messages.goose less what belonged to the proxy (deadlines, fences, close by tag, addresses, policy):
// two kinds of message remain, the ones the loop itself owns: drain and statistics.
//
// Goose's typed queues deliver a value to ONE receiver, so a broadcast needs an address per loop: each loop owns a pipe.
// spawn() makes all of them before it starts any loop (globals set in main are copied into each thread at spawn), post()
// writes one small flat record to every pipe, and a loop reads its own from the ring, before it handles the events of the same pass.
namespace iouring;

import std;
import binary;
import .sys;
import .engine;

let _MAX_LOOPS:     i64 = 64;
let _MESSAGE_DRAIN: u8  = 1;
let _MESSAGE_STATS: u8  = 4;
// kind(1) reserved(2) reserved(4) value(8), zero padded to 64 bytes. A pipe write up to PIPE_BUF (4096 bytes on Linux) is atomic,
// so each loop still receives each message whole and once.
let _MESSAGE_SIZE: i64 = 64;

var _message_read_fds:  i32[64] = [-1; 64];   // set by spawn(); each loop finds its own by index
var _message_write_fds: i32[64] = [-1; 64];
var _loop_count:        i64     = 0;
var _loop_ids:          i64[64] = [0; 64];
var _signal_fd:         i32     = -1;

enum Message {
    Drain { deadline_ms: i64 },
    Stats { reply_to: u16 },
}

// Starts `n` loops, each running `worker(index)`. `worker` is a thread_fn with the signature (index: i64), passed last:
//     iouring::spawn(4, worker);
// Before it starts anything it blocks SIGTERM, SIGINT and SIGUSR1 (so every thread inherits that and only wait_for_signal sees
// them) and creates one message pipe per loop. At most 64 loops. Returns the thread ids. A worker creates its own `Loop`, listens,
// calls `attach(io, index)` and serves.
fn spawn<W>(n: i64) -> i64[>..] {
    _signal_fd = c_signals_open();
    _loop_count = if n < _MAX_LOOPS { n } else { _MAX_LOOPS };
    for i in _loop_count {
        var fds: i32[2] = [-1, -1];
        c_pipe(fds);
        _message_read_fds[i] = fds[0];
        _message_write_fds[i] = fds[1];
    }
    var ids: i64[>..] = [];
    for i in _loop_count {
        _loop_ids[i] = thread_spawn(W, i);
        ids.push(_loop_ids[i]);
    }
    ids
}

// A sensible default for the number of loops: the CPUs this process may use.
fn cpu_count() -> i64 { hardware_threads() }

// Whether `address` can be bound by a plain socket: a port held by another process is an error here, instead of loops silently
// sharing it through SO_REUSEPORT (which listen uses so that several loops can share a port). Call it before spawn; the result
// is `listen`'s kind of error, None when the address is free.
fn probe(address: const u8[:]) -> goose_io::ListenError {
    var p = _ParsedAddress { false, [0; 16], 0 };
    if !_parse_address(address, p, true) { return goose_io::ListenError { kind: goose_io::ListenErrorKind.BadAddress, code: 22 }; }
    let fd = c_listen(p.v6, p.ip, p.port, false, 1);
    if fd < 0 { return goose_io::ListenError { kind: _listen_kind(-fd), code: -fd }; }
    c_close(fd);
    goose_io::ListenError { .. }
}

// Joins loop number `index` to the messages of spawn(): the loop then drains when drain() is called and answers gather_stats().
fn attach(io: Loop&, index: i64) {
    io._ensure();
    io.index = index;
    if index < _loop_count {
        io._message_fd = _message_read_fds[index];
        c_ring_ctl(io._ring_fd, c_WATCH_ADD, io._message_fd, c_POLLIN, _pack_event_data(_EVENT_KIND_MESSAGE, 0, 0));
        io._attached = true;
    }
}

// A worker that cannot start (a listener failed) still owes drain() its report.
fn report_failed_start() { qput(_DrainReport { drained: 0 }); }

// Sends a message to every loop; each receives it once. Call it from main, or from a thread started after spawn.
fn post(m: Message) {
    var raw: u8[>..] = [];
    match m {
        Drain d => { _write_message(raw, _MESSAGE_DRAIN, 0, d.deadline_ms); },
        Stats r => { _write_message(raw, _MESSAGE_STATS, r.reply_to, 0); },
    }
    while raw.len < _MESSAGE_SIZE { raw.push(0); }
    for k in _loop_count { c_pipe_write(_message_write_fds[k], raw[0], _MESSAGE_SIZE); }
}

fn _write_message(raw: u8[>..]&, kind: u8, tag: u16, value: i64) {
    binary::write_u8(raw, kind);
    binary::write_u16le(raw, tag);
    binary::write_u32le(raw, 0);
    binary::write_i64le(raw, value);
}

// Blocks the calling thread until SIGTERM, SIGINT or SIGUSR1 arrives and returns its number (15, 2, 10), or -1 when no signalfd
// exists (call spawn first). The program decides what each signal means: the usual pair is `drain` on all three.
fn wait_for_signal() -> i32 {
    if _signal_fd >= 0 { c_signal_wait(_signal_fd) } else { -1 }
}

// What a loop says when it has finished: how many links it ended at the end of the grace period (reason Shutdown).
struct _DrainReport { drained: i64 }

var _drain_done: bool = false;

// Stops every loop (`stop` on each, from its own thread): listeners close at once, links finish, and the ones still open `deadline_ms`
// later end with Shutdown. Returns when every loop has ended, with the number of links ended at the deadline (0 = every link ended
// by itself). Call it once, from the thread that called spawn; a second call returns 0.
fn drain(deadline_ms: i64) -> i64 {
    if _drain_done { return 0; }
    _drain_done = true;
    post(Message.Drain { deadline_ms: deadline_ms });
    for k in _loop_count { thread_wait(_loop_ids[k]); }
    var closed: i64 = 0;
    for k in _loop_count { closed += qget<_DrainReport>().drained; }     // every loop reports once, when it ends or fails to start
    closed
}

// Blocks until SIGTERM, SIGINT or SIGUSR1, then drains every loop with a grace period. A thin wrapper over the two calls above.
fn wait_for_signal_then_drain(deadline_ms: i64) {
    wait_for_signal();
    drain(deadline_ms);
}

// --- inside a loop -------------------------------------------------------------------------------

// Reads every message waiting in this loop's pipe and acts on it.
fn _read_messages(io: Loop&) {
    var raw: u8[64] = [0; 64];
    while c_pipe_read(io._message_fd, raw[0], _MESSAGE_SIZE) == _MESSAGE_SIZE {
        var r = binary::reader(raw[..]);
        let kind, ok = binary::read_u8(r);
        let tag, ok2 = binary::read_u16le(r);
        let gen, ok3 = binary::read_u32le(r);
        let value, ok4 = binary::read_i64le(r);
        if !ok || !ok2 || !ok3 || !ok4 { continue; }
        if kind == _MESSAGE_DRAIN { io.stop(value); }
        else if kind == _MESSAGE_STATS { qput(_StatsReply { loop_index: io.index, stats: io.stats() }); }
    }
}

// --- statistics across loops ---------------------------------------------------------------------

struct _StatsReply { loop_index: i64, stats: Stats }

fn _add_stats(total: Stats&, s: Stats) {
    total.accepted += s.accepted;  total.open += s.open;
    for k in 9 { total.closed[k] += s.closed[k]; }
    total.bytes_in += s.bytes_in;  total.bytes_out += s.bytes_out;
    total.connect_failures += s.connect_failures;
    total.buffers_total += s.buffers_total;  total.buffers_in_use += s.buffers_in_use;  total.buffer_waits += s.buffer_waits;
    total.waiting_len += s.waiting_len;
    total.batch_waits += s.batch_waits;  total.batch_wait_us += s.batch_wait_us;
    total.stale_events += s.stale_events;  total.bad_events += s.bad_events;  total.accept_failures += s.accept_failures;
}

// Asks every loop for its counters and adds them up: the totals of the whole program. Blocks until each of the `loops` has answered.
// One caller at a time: the replies share one queue.
fn gather_stats(loops: i64) -> Stats {
    post(Message.Stats { reply_to: 1 });
    var total = Stats { .. };
    for k in loops { _add_stats(total, qget<_StatsReply>().stats); }
    total
}
