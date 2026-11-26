// The io_uring event loop: one `Loop` is one thread's event loop on real sockets, and supplies the whole contract
// (the core and the half-close, sizing, abort, inspection and batching capabilities, declared near the end of this file; not unix, relay, peek, revoke, wake or limits).
//
// It is tcprouter's engine (its generic layer: the worker loop, slots and generations, buffer pool, bounded send
// queue, dial, timeouts, drain, statistics) with the proxy removed (routes, SNI inspection, forwarding, retry, policy,
// tags and deadlines), and moved to the contract's names: `Conn` is `Link`, `send` returns `SendStatus`, an event carries
// its link and nothing attached to it, `Opened` names the listener. Where the contract asks for something tcprouter did
// differently, the difference is marked "contract:" below.
//
// How it works, in one paragraph. A link lives in a slot with a generation; every socket is registered with the ring (a poll op per descriptor, re-armed each wait: level-triggered)
// and the completion data names the slot and its generation, so an event for a link that has ended is
// recognised and ignored. Events are never delivered from inside ring dispatch or from an operation: dispatch and the
// operations append flat records to a queue, and `serve` drains the queue after the dispatch, calling the application's
// block once per record. A `Data` record keeps its pool buffer until the block returns. A slot is freed only after
// its `Closed` has been handled (contract: tcprouter freed it when the `Closed` was queued).
//
// Usage:
//
//     var io = iouring::Loop { .. };
//     let l, err = io.listen("0.0.0.0:8080");
//     io.serve() { io, ev => handle(io, ev); };
namespace iouring;

import std;
import os;
import goose_io;
import .sys;
import .address;
import .config;

// --- link states and event kinds -----------------------------------------------------------------

let _FREE:    u8 = 0;
let _DIALING: u8 = 1;       // dial returned; the connect is in progress
let _OPEN:    u8 = 2;       // open (also closing and write-shut: those are flags)
let _ENDED:   u8 = 3;       // its Closed is queued; the slot and the descriptor are kept until it has been handled

let _EVENT_KIND_LISTENER: u64 = 1;
let _EVENT_KIND_LINK:     u64 = 2;
let _EVENT_KIND_ORPHAN:   u64 = 3;    // a socket of a link that has ended, kept open to read and drop the peer's input (see _finish_close)
let _EVENT_KIND_MESSAGE:  u64 = 4;
let _WAITER_READER:       u64 = 1;    // in the low byte of a _waiting_reads entry: a read waits for a pool buffer
let _WAITER_WRITER:       u64 = 2;    // a refused send waits for a pool buffer

let _MAX_GEN: u32 = 0xffffff;         // a generation is 24 bits in the completion data; a slot whose generations are used up is retired

let _PENDING_OPENED:   u8 = 1;
let _PENDING_DATA:     u8 = 2;
let _PENDING_WRITABLE: u8 = 3;
let _PENDING_CLOSED:   u8 = 4;
let _PENDING_EOF:      u8 = 5;        // the peer's end of stream was read; what it means is decided when this record is reached (keep-open window)

// --- statistics ----------------------------------------------------------------------------------

// A snapshot of one loop's counters. Flat. Read it on the loop's own thread (for example from a Tick); nothing is shared
// between loops, so there is no lock.
struct Stats {
    accepted:         i64,    // connections accepted
    open:             i64,    // links not yet released (open, dialling, and ended with their Closed still to be handled)
    closed:           i64[9], // links ended, by reason: PeerClosed, PeerReset, ConnectFailed, IdleTimeout, HalfCloseTimeout, Local, Shutdown, Error, LoopSpecific
    bytes_in:         i64,    // bytes read from peers
    bytes_out:        i64,    // bytes written to peers
    connect_failures: i64,    // dials that failed
    buffers_total:    i64,    // buffers in this loop's pool
    buffers_in_use:   i64,    // buffers borrowed right now
    buffer_waits:     i64,    // times a read, or a refused send, had to wait for a free buffer
    stale_events:     i64,    // events for a link that had ended and whose slot was reused
    bad_events:       i64,    // events whose slot index or kind was out of range
    accept_failures:  i64,    // accepts that failed for lack of a resource; the listener rests briefly
    batch_waits:      i64,    // batching windows entered: 0 unless limits.batch_min_events and batch_max_wait_us are both set
    batch_wait_us:    i64,    // microseconds slept in them
    waiting_len:      i64,    // entries in the list of reads waiting for a pool buffer (a gauge)
}

fn _reason_index(r: goose_io::CloseReason) -> i64 {
    return match r {
        PeerClosed => 0, PeerReset => 1, ConnectFailed f => 2, IdleTimeout => 3, HalfCloseTimeout => 4,
        Local => 5, Shutdown => 6, Error => 7, LoopSpecific p => 8,
    };
}

fn _is_local(r: goose_io::CloseReason) -> bool {
    return match r { Local => true, _ => false };
}

// --- the loop ------------------------------------------------------------------------------------

struct _Link {
    gen:               u32,
    state:             u8,
    fd:                i32,
    mask:              u32,      // the interest set now
    registered:        bool,     // the descriptor is watched by the ring
    listener:          u32,      // the listener that accepted it, 0 for a dialled link
    started_ms:        i64,      // when the link was accepted, or the dial began (the dial's timeout)
    last_active_ms:    i64,      // when bytes last moved in either direction (idle and half-close timeouts)
    half_closed_at_ms: i64,      // when the first direction ended, or 0
    closing_at_ms:     i64,      // when close was called, or 0
    connect_ms:        i64,      // a dial's own timeout, 0 = limits.connect_timeout_ms
    out_buffers:       i32[4],   // the send queue, oldest first, at most 4 pool buffers
    out_lengths:       i32[4],
    out_count:         u8,
    out_pos:           i32,      // bytes of the oldest queued buffer already written
    rbuf:              i32,      // the pool buffer that holds the bytes of the Data event not yet handled, or -1
    rlen:              i32,
    refused:           i32,      // the largest send refused since the last Writable (the size one Writable must make room for)
    paused:            bool,     // pause_reading
    data_pending:      bool,     // bytes are held in rbuf and not yet handled: nothing more is read
    data_queued:       bool,     // a Data record for them is on the queue
    waiting:           bool,     // queued for a pool buffer
    want_writable:     bool,     // a send was refused: tell the application when it would fit
    closing:           bool,     // flush the queue, then end with closing_reason
    closing_reason:    goose_io::CloseReason,
    orphan:            bool,     // its socket outlives it: see _finish_close
    eof_seen:          bool,     // the peer's end of stream was read
    eof_handled:       bool,     // ... and its record has been handled
    keep_open:         bool,     // set_keep_open_on_eof
    window:            bool,     // the Opened handler is running: set_keep_open_on_eof is recorded
    shut_wr_pending:   bool,     // shutdown_write was called; the write side is shut down once the queue is out
    wr_shut:           bool,     // the write side is shut down
    local_closed:      bool,     // the application called close or reset
    peer:              u8[18],   // the peer's address, kept when a reset has made the socket forget it (_end)
    peer_family:       i32,      // 4 or 6 when `peer` holds one, else 0
}

struct _Pending {
    kind:     u8,
    slot:     u32,
    gen:      u32,
    listener: u32,
    why:      goose_io::CloseReason,
}

struct _Listening {
    fd:        i32,
    active:    bool,
    resume_at: i64,              // boottime ms when a paused listener goes back on the ring, or 0
}

// A socket whose link has ended with Closed(Local) after `close` finished sending. Closing it at once could reset the connection if input
// is unread, and the peer would lose bytes it was sent, so the loop shuts the write side and keeps the descriptor here, reading and
// dropping input, until the peer ends its stream or the half-close limit passes. It takes no slot and no link event.
struct _Orphan {
    fd:       i32,
    since_ms: i64,
}

root struct Loop {
    limits:              Limits          = Limits { .. }, // set before the first listen, dial or serve
    index:               i64             = 0,             // which of several loops this is (see attach)
    _ring_fd:            i32             = -1,
    _started:            bool            = false,
    _stats:              Stats           = Stats { .. },
    _links:              _Link[>..]      = [], // one per link, reused through _free_links
    _free_links:         u32[>..<]       = [],
    _live:               i64             = 0,  // links not yet released
    _listeners:          _Listening[>..] = [], // never reused: a Listener's id is its index plus one
    _orphans:            _Orphan[>..]    = [], // fd -1 marks a free entry
    _orphans_open:       i64             = 0,
    _buffer_pool:        u8[>..]         = [], // buffers_per_loop buffers of buffer_size bytes, grown lazily
    _free_buffers:       i32[>..<]       = [],
    _waiting_reads:      u64[>..<]       = [], // reads and sends waiting for a buffer, oldest first from _waiting_reads_head
    _waiting_reads_head: i64             = 0,
    _scratch:            u8[16384]       = [0; 16384], // where the input of an orphan socket is dropped
    _pending:            _Pending[>..<]  = [], // events queued, drained by serve
    _pending_head:       i64             = 0,
    _message_fd:         i32             = -1,    // this loop's end of its message pipe (messages.goose)
    _attached:           bool            = false, // attach() was called: serve reports to drain() when it ends
    _draining:           bool            = false,
    _drain_deadline_ms:  i64             = 0,
    _finished:           bool            = false, // serve has returned: every operation does nothing
    _pass_delivered:     i64             = 0,     // link events delivered in this pass (for PassEnd)
    _dialing:            i64             = 0,     // dials whose connect is still in progress
    _dial_due:           i64             = 0,     // the earliest connect deadline among them (0 = none known)
    _wake_at:            i64             = 0,     // the earliest time asked for with tick_by, or 0
    _now_ms:             i64             = 0,     // CLOCK_BOOTTIME at the start of the current pass of the loop
    _next_query:         u32             = 1,
}

// --- small helpers -------------------------------------------------------------------------------

fn _pack_event_data(kind: u64, slot: i64, gen: u32) -> u64 {
    kind | ((gen as u64) << 8) | ((slot as u64) << 32)
}

fn _pause_milliseconds(ms: i64) { sleep_ms(ms); }

// The first operation (or serve) creates the ring. False when the system has none to give.
fn _ensure(io: Loop&) -> bool {
    if io._started { return io._ring_fd >= 0; }
    io._started = true;
    io._now_ms = c_boottime_ms();
    if io.limits.buffer_size < 1024 { io.limits.buffer_size = 1024; }            // max_send is four buffers: at least 4096
    if io.limits.buffers_per_loop < 4 { io.limits.buffers_per_loop = 4; }
    io._ring_fd = c_ring_create();
    io._ring_fd >= 0
}

fn _failed_with(kind: goose_io::ConnectFailure) -> goose_io::CloseReason {
    goose_io::CloseReason.ConnectFailed { kind: kind }
}

// What a failed connect or socket call reports, in the contract's kinds (docs/event-loops/tcprouter-engine.md, *Connect failures*).
fn _connect_kind(err: i32) -> goose_io::ConnectFailure {
    if err == c_ECONNREFUSED { return goose_io::ConnectFailure.Refused; }
    if err == c_ENETUNREACH || err == c_EHOSTUNREACH { return goose_io::ConnectFailure.Unreachable; }
    if err == c_EACCES || err == c_EPERM { return goose_io::ConnectFailure.Denied; }
    if err == c_EMFILE || err == c_ENFILE || err == c_ENOBUFS || err == c_ENOMEM || err == c_EADDRNOTAVAIL { return goose_io::ConnectFailure.NoResources; }
    goose_io::ConnectFailure.Other
}

fn _listen_kind(err: i32) -> goose_io::ListenErrorKind {
    if err == c_EADDRINUSE { return goose_io::ListenErrorKind.AddressInUse; }
    if err == c_EACCES || err == c_EPERM { return goose_io::ListenErrorKind.Denied; }
    if err == c_EMFILE || err == c_ENFILE || err == c_ENOBUFS || err == c_ENOMEM { return goose_io::ListenErrorKind.NoResources; }
    goose_io::ListenErrorKind.Other
}

// Fills io's error text for a start-up failure: "<what> <where>: <strerror(err)>", as tcprouter's `_fail_start` did.
// The text names the address and the system's error and no part of it aborts the process.
fn describe_listen_failure(address: const u8[:], err: goose_io::ListenError, out: u8[>..]&) {
    out.append("cannot listen on ");
    out.append(address);
    out.append(": ");
    c_strerror(out, err.code);
}

// --- the buffer pool -----------------------------------------------------------------------------

// The pool grows lazily, one buffer at a time, up to buffers_per_loop: an idle loop touches no buffer memory. Growth only
// appends, so a buffer the kernel is writing into never moves. Returns a buffer number, or -1 when all of them are in use.
fn _take_buffer(io: Loop&) -> i32 {
    var id: i32 = -1;
    if io._free_buffers.len > 0 {
        id = io._free_buffers.pop();
    } else if io._stats.buffers_total < io.limits.buffers_per_loop {
        id = io._stats.buffers_total as i32;
        for k in io.limits.buffer_size { io._buffer_pool.push(0); }
        io._stats.buffers_total++;
    }
    if id >= 0 { io._stats.buffers_in_use++; }
    id
}

fn _buffer_slice(io: Loop&, id: i32) -> u8[:] {
    let size = io.limits.buffer_size;
    io._buffer_pool[id * size..(id + 1) * size]
}

// Returns a buffer to the pool and lets the oldest read or send that was waiting for one try again.
fn _give_back_buffer(io: Loop&, id: i32) {
    io._free_buffers.push(id);
    io._stats.buffers_in_use--;
    io._wake_waiting();
}

fn _wake_waiting(io: Loop&) {
    while io._waiting_reads_head < io._waiting_reads.len {
        let e = io._waiting_reads[io._waiting_reads_head];
        io._waiting_reads_head++;
        let slot = (e >> 32) as i64;
        let gen = ((e >> 8) & 0xffffff) as u32;
        if slot < io._links.len {
            let s .= io._links[slot];
            if s.state == _OPEN && s.gen == gen {
                if (e & 0xff) == _WAITER_WRITER {
                    // A send refused for lack of pool buffers while its queue was empty (nothing else would ever wake it).
                    if s.want_writable && s.out_count == 0 && !s.closing {
                        if io._send_room_of(slot) >= s.refused {
                            s.want_writable = false;
                            s.refused = 0;
                            io._queue(_PENDING_WRITABLE, slot);
                            break;
                        }
                        io._waiting_reads.push(_pack_event_data(_WAITER_WRITER, slot, gen));     // not enough room yet: wait again
                    }
                } else if s.waiting {
                    s.waiting = false;
                    io._update_interest(slot);
                    break;
                }
            }
        }
    }
    // The list is emptied when everything was served; under a load that always leaves a waiter it is compacted instead, or it would
    // grow for as long as the load lasts. The copy is at most as long as the head, so it costs O(1) per entry served.
    let left = io._waiting_reads.len - io._waiting_reads_head;
    if left == 0 {
        io._waiting_reads.clear();
        io._waiting_reads_head = 0;
    } else if io._waiting_reads_head >= 64 && io._waiting_reads_head >= left {
        for k in left { io._waiting_reads[k] = io._waiting_reads[io._waiting_reads_head + k]; }
        io._waiting_reads.pop_n(io._waiting_reads_head);
        io._waiting_reads_head = 0;
    }
}

// --- the queue of events -------------------------------------------------------------------------

fn _queue(io: Loop&, kind: u8, i: i64) {
    let s .= io._links[i];
    io._pending.push(_Pending { kind: kind, slot: i as u32, gen: s.gen, listener: s.listener, why: goose_io::CloseReason.Local });
}

// The link of a handle: its index, or -1 when it was never issued, is stale, or the slot is free.
fn _find(io: Loop&, link: goose_io::Link) -> i64 {
    if link.gen == 0 || link.slot >= io._links.len { return -1; }
    let s .= io._links[link.slot];
    if s.state == _FREE || s.gen != link.gen { return -1; }
    link.slot
}

fn _link_of(io: Loop&, i: i64) -> goose_io::Link {
    goose_io::Link { slot: i as u32, gen: io._links[i].gen }
}

// A fresh link in a free slot (a freed slot comes before the table is extended: slots are dense), or a new slot.
fn _fresh_link(io: Loop&, fd: i32, listener: u32, state: u8) -> i64 {
    let now = io._now_ms;
    let link = _Link {
        gen:               1, state: state, fd: fd, mask: 0, registered: false, listener: listener, started_ms: now, last_active_ms: now,
        half_closed_at_ms: 0, closing_at_ms: 0, connect_ms: 0, out_buffers: [-1; 4], out_lengths: [0; 4], out_count: 0,
        out_pos:           0, rbuf: -1, rlen: 0, refused: 0, paused: false, data_pending: false, data_queued: false, waiting: false,
        want_writable:     false, closing: false, closing_reason: goose_io::CloseReason.Local, orphan: false, eof_seen: false,
        eof_handled:       false, keep_open: false, window: false, shut_wr_pending: false, wr_shut: false, local_closed: false,
        peer:              [0; 18], peer_family: 0,
    };
    var i: i64 = 0;
    if io._free_links.len > 0 {
        i = io._free_links.pop();
        let gen = io._links[i].gen;
        io._links[i] = link;
        io._links[i].gen = gen;
    } else {
        i = io._links.len;
        io._links.push(link);
    }
    io._live++;
    io._stats.open++;
    i
}

// --- ring interest ------------------------------------------------------------------------------

fn _wants_read(s: _Link&) -> bool {
    !s.closing && !s.paused && !s.eof_seen && !s.waiting
}

fn _set_interest(io: Loop&, i: i64, mask: u32) {
    let s .= io._links[i];
    if s.registered {
        if mask == s.mask { return; }
        c_ring_ctl(io._ring_fd, c_WATCH_MOD, s.fd, mask, _pack_event_data(_EVENT_KIND_LINK, i, s.gen));
    } else {
        if mask == 0 { return; }
        let rc = c_ring_ctl(io._ring_fd, c_WATCH_ADD, s.fd, mask, _pack_event_data(_EVENT_KIND_LINK, i, s.gen));
        if rc < 0 { return; }                               // out of ring memory: like a failed MOD, the idle timeout ends it
        s.registered = true;
    }
    s.mask = mask;
}

fn _update_interest(io: Loop&, i: i64) {
    let s .= io._links[i];
    if s.state != _OPEN { return; }
    var m: u32 = 0;
    // A pending Data record does not clear POLLIN: serve handles every queued record before the next ring wait, and
    // _read_link will not read while one is pending, so the mask change (a watch change per request, twice) would be for nothing.
    if s._wants_read() { m |= c_POLLIN; }
    if s.out_count > 0 { m |= c_POLLOUT; }
    io._set_interest(i, m);
}

// --- ending a link -------------------------------------------------------------------------------

// The link ends: its sending queue is returned, its Closed is queued, and the slot and the descriptor are KEPT until the
// handlers of that Closed have returned (_release). After this the caller must not use the link for anything but peer_address.
fn _end(io: Loop&, i: i64, reason: goose_io::CloseReason) {
    let s .= io._links[i];
    if s.state != _OPEN && s.state != _DIALING { return; }
    var why = reason;
    if s.closing && _is_local(s.closing_reason) { why = goose_io::CloseReason.Local; }       // contract: a link the application closed ends Local
    if s.state == _DIALING {
        io._dialing--;
        if io._dialing == 0 { io._dial_due = 0; }
    } else if s.fd >= 0 && (_reason_index(why) == 1 || _reason_index(why) == 7) {
        s._remember_peer();
    }
    var out_ids: i32[4] = [-1; 4];                                    // the send queue's buffers, returned after the link is dead
    let out_n = s.out_count as i64;
    for k in out_n { out_ids[k] = s.out_buffers[k]; }
    s.out_count = 0;
    s.out_pos = 0;
    s.state = _ENDED;
    s.want_writable = false;
    if s.local_closed && s.data_pending {                          // bytes the application will not be given
        let b = s.rbuf;
        s.rbuf = -1;
        s.data_pending = false;
        io._give_back_buffer(b);
    }
    io._pending.push(_Pending { kind: _PENDING_CLOSED, slot: i as u32, gen: s.gen, listener: s.listener, why: why });
    io._stats.closed[_reason_index(why)]++;
    for k in out_n { io._give_back_buffer(out_ids[k]); }
}

// contract: peer_address works inside the handler of Closed. A socket forgets its peer once it has been reset, or once both
// directions have finished, so the address is asked for before either can happen (a reset, a shutdown of the write side) and not on every link.
fn _remember_peer(s: _Link&) {
    if s.peer_family != 0 || s.fd < 0 { return; }
    s.peer_family = c_peer(s.fd, s.peer);
    if s.peer_family < 0 { s.peer_family = 0; }
}

// The Closed of link i has been handled: close the descriptor, free the slot and advance its generation. A slot whose
// generations are used up is retired and never issued again.
fn _release(io: Loop&, i: i64) {
    let s .= io._links[i];
    if s.fd >= 0 {
        if s.orphan { io._adopt_orphan(s.fd); } else { c_close(s.fd); }
        s.fd = -1;
    }
    if s.rbuf >= 0 { let b = s.rbuf; s.rbuf = -1; io._give_back_buffer(b); }
    s.state = _FREE;
    s.registered = false;
    s.mask = 0;
    s.gen = s.gen + 1;
    io._live--;
    io._stats.open--;
    if s.gen <= _MAX_GEN { io._free_links.push(i as u32); }
}

// --- listening and accepting ---------------------------------------------------------------------

// Plain listen: the port is this loop's alone (a second listener on it, plain or shared, is AddressInUse).
fn listen(io: Loop&, address: const u8[:]) -> goose_io::Listener, goose_io::ListenError { return io._listen(address, false); }

// Shared listen (the shared-listen capability): SO_REUSEPORT, so several loops (one per core) each hold the address and the
// kernel hands every connection to exactly one of them. A shared listener on a path is BadAddress.
fn listen_shared(io: Loop&, address: const u8[:]) -> goose_io::Listener, goose_io::ListenError { return io._listen(address, true); }

fn _listen(io: Loop&, address: const u8[:], shared: bool) -> goose_io::Listener, goose_io::ListenError {
    let none = goose_io::Listener { .. };
    if io._finished || io._draining { return none, goose_io::ListenError { kind: goose_io::ListenErrorKind.Other, code: 108 }; }
    var p = _ParsedAddress { false, [0; 16], 0 };
    if !_parse_address(address, p, true) { return none, goose_io::ListenError { kind: goose_io::ListenErrorKind.BadAddress, code: 22 }; }
    if !io._ensure() { return none, goose_io::ListenError { kind: goose_io::ListenErrorKind.NoResources, code: 24 }; }
    if io._listeners.len >= 0xfffffffe { return none, goose_io::ListenError { kind: goose_io::ListenErrorKind.NoResources, code: 24 }; }
    let fd = c_listen(p.v6, p.ip, p.port, shared, 1024);
    if fd < 0 { return none, goose_io::ListenError { kind: _listen_kind(-fd), code: -fd }; }
    let index = io._listeners.len;
    if c_ring_ctl(io._ring_fd, c_WATCH_ADD, fd, c_POLLIN, _pack_event_data(_EVENT_KIND_LISTENER, index, 0)) < 0 {
        c_close(fd);
        return none, goose_io::ListenError { kind: goose_io::ListenErrorKind.NoResources, code: 24 };
    }
    io._listeners.push(_Listening { fd: fd, active: true, resume_at: 0 });
    return goose_io::Listener { id: (index + 1) as u32 }, goose_io::ListenError { .. };
}

fn _listener_index(io: Loop&, l: goose_io::Listener) -> i64 {
    if l.id == 0 || l.id > io._listeners.len { return -1; }
    l.id - 1
}

fn listener_port(io: Loop&, l: goose_io::Listener) -> u16 {
    let k = io._listener_index(l);
    if k < 0 || io._listeners[k].fd < 0 { return 0; }
    let port = c_local_port(io._listeners[k].fd);
    if port < 0 { 0 } else { port as u16 }
}

// Closes the listener's socket and takes it off the ring; links it accepted stay. Its id is not issued again.
fn stop_listening(io: Loop&, l: goose_io::Listener) {
    if io._finished { return; }
    let k = io._listener_index(l);
    if k < 0 { return; }
    let lst .= io._listeners[k];
    if lst.fd >= 0 { c_close(lst.fd); lst.fd = -1; }
    lst.active = false;
    lst.resume_at = 0;
}

// How long a listener is left alone after accept failed for lack of a resource (descriptors, memory, buffers).
let _ACCEPT_PAUSE_MILLISECONDS: i64 = 100;

fn _accept_ready(io: Loop&, k: i64) {
    for n in 64 {                                        // a bounded batch per wakeup keeps the loop fair
        if io._listeners[k].fd < 0 { return; }
        let fd = c_accept(io._listeners[k].fd);
        if fd >= 0 { io._new_link(k, fd); continue; }
        let error = -fd;
        if error == c_EAGAIN || error == c_EINTR { return; }              // nobody is waiting
        if error == c_ECONNABORTED || error == c_EPROTO { continue; }     // that client left
        // Out of descriptors, memory or socket buffers (EMFILE, ENFILE, ENOMEM, ENOBUFS...): nothing we do right now can fix it, and the poll is
        // level-triggered, so the pending connection would wake us again at once and we would spin. Leave this listener off the ring for a
        // moment; the clients wait in the kernel's backlog and are served when descriptors come back.
        io._stats.accept_failures++;
        c_ring_ctl(io._ring_fd, c_WATCH_MOD, io._listeners[k].fd, 0, _pack_event_data(_EVENT_KIND_LISTENER, k, 0));
        io._listeners[k].resume_at = c_boottime_ms() + _ACCEPT_PAUSE_MILLISECONDS;
        return;
    }
}

// Puts back on the ring the listeners whose pause is over. Called once per pass of the loop.
fn _resume_paused_listeners(io: Loop&, now: i64) {
    for k in io._listeners.len {
        let lst .= io._listeners[k];
        if lst.resume_at != 0 && now >= lst.resume_at {
            lst.resume_at = 0;
            if lst.fd >= 0 { c_ring_ctl(io._ring_fd, c_WATCH_MOD, lst.fd, c_POLLIN, _pack_event_data(_EVENT_KIND_LISTENER, k, 0)); }
        }
    }
}

fn _new_link(io: Loop&, k: i64, fd: i32) {
    io._stats.accepted++;
    if io._live >= io.limits.max_links {                  // no slot for it: refused, never a link
        c_close(fd);
        return;
    }
    let i = io._fresh_link(fd, (k + 1) as u32, _OPEN);
    io._queue(_PENDING_OPENED, i);
    io._update_interest(i);
}

// --- dialling ------------------------------------------------------------------------------------

fn dial(io: Loop&, address: const u8[:], connect_ms: i64) -> goose_io::Link {
    if io._finished { return goose_io::Link { .. }; }                          // a finished loop issues no link
    let started = io._ensure();
    let i = io._fresh_link(-1, 0, _DIALING);
    let s .= io._links[i];
    s.connect_ms = connect_ms;
    io._dialing++;
    let link = goose_io::Link { slot: i as u32, gen: s.gen };
    if !started || io._live > io.limits.max_links {
        io._stats.connect_failures++;
        io._end(i, _failed_with(goose_io::ConnectFailure.NoResources));
        return link;
    }
    var p = _ParsedAddress { false, [0; 16], 0 };
    if !_parse_address(address, p, false) {
        io._stats.connect_failures++;
        io._end(i, _failed_with(goose_io::ConnectFailure.BadAddress));
        return link;
    }
    let fd = c_connect_start(p.v6, p.ip, p.port);
    if fd < 0 {
        io._stats.connect_failures++;
        io._end(i, _failed_with(_connect_kind(-fd)));
        return link;
    }
    s.fd = fd;
    s.mask = c_POLLOUT;                              // level-triggered: the connect finishing makes the socket writable (or errored)
    if c_ring_ctl(io._ring_fd, c_WATCH_ADD, fd, c_POLLOUT, _pack_event_data(_EVENT_KIND_LINK, i, s.gen)) < 0 {
        io._stats.connect_failures++;
        io._end(i, _failed_with(goose_io::ConnectFailure.NoResources));
        return link;
    }
    s.registered = true;
    io._note_dial_due(io._now_ms + (if connect_ms > 0 { connect_ms } else { io.limits.connect_timeout_ms }));
    link
}

// Connect timeouts of dials are per link, so a table of deadlines cannot express them. Instead the loop remembers the earliest
// connect deadline and sleeps no longer than that: nothing wakes it while no dial is pending.
fn _note_dial_due(io: Loop&, due: i64) {
    if io._dial_due == 0 || due < io._dial_due { io._dial_due = due; }
}

// Ends (ConnectFailed, TimedOut) the dials whose own connect timeout has passed, and finds the next deadline among the rest.
fn _expire_dials(io: Loop&, now: i64) {
    var next: i64 = 0;
    for i in io._links.len {
        let s .= io._links[i];
        if s.state != _DIALING { continue; }
        let allowed = if s.connect_ms > 0 { s.connect_ms } else { io.limits.connect_timeout_ms };
        let due = s.started_ms + allowed;
        if now >= due {
            io._stats.connect_failures++;
            io._end(i, _failed_with(goose_io::ConnectFailure.TimedOut));
        } else if next == 0 || due < next {
            next = due;
        }
    }
    io._dial_due = next;
}

// The connect of a dialled link finished (or failed): Opened, or the one Closed.
fn _dial_finished(io: Loop&, i: i64) {
    let s .= io._links[i];
    let err = c_connect_result(s.fd);
    if err != 0 {
        io._stats.connect_failures++;
        io._end(i, _failed_with(_connect_kind(err)));
        return;
    }
    io._dialing--;
    if io._dialing == 0 { io._dial_due = 0; }          // nothing left to time: no stale deadline for the next dial to inherit
    s.state = _OPEN;
    s.started_ms = io._now_ms;
    s.last_active_ms = io._now_ms;
    io._queue(_PENDING_OPENED, i);
    io._update_interest(i);                            // now input (and output when something is queued)
}

// --- the operations on a link --------------------------------------------------------------------

fn max_send(io: Loop&) -> i64 {
    io._ensure();
    io.limits.buffer_size * 4
}

// How many bytes `send` would accept right now: the free room of the 4-buffer queue, 0 when the link cannot send.
fn _send_room_of(io: Loop&, i: i64) -> i64 {
    let s .= io._links[i];
    if s.state != _OPEN || s.closing || s.wr_shut || s.shut_wr_pending { return 0; }
    let size = io.limits.buffer_size;
    let tail_room = if s.out_count > 0 { size - s.out_lengths[s.out_count - 1] } else { 0 };
    let queue_free = 4 - s.out_count;
    let available = io._free_buffers.len + (io.limits.buffers_per_loop - io._stats.buffers_total);
    let fresh = if queue_free < available { queue_free } else { available };
    tail_room + fresh * size
}

fn send_room(io: Loop&, link: goose_io::Link) -> i64 {
    if io._finished { return 0; }
    let i = io._find(link);
    if i < 0 { return 0; }
    io._send_room_of(i)
}

fn send(io: Loop&, link: goose_io::Link, bytes: const u8[:]) -> goose_io::SendStatus {
    return io._send(link, bytes, "");
}

// Both parts or neither: the sum of the two lengths is what must fit.
fn send2(io: Loop&, link: goose_io::Link, first: const u8[:], second: const u8[:]) -> goose_io::SendStatus {
    return io._send(link, first, second);
}

// Queues the bytes and writes what the socket takes at once. All or nothing: Closed, NotOpen, TooLarge or Full (and nothing queued).
// The queue holds at most 4 pool buffers (buffer_size x 4 = max_send), so a larger send is refused for good and a send that does not
// fit now is refused until the queue drains: after a Full there is exactly one Writable, once the largest refused send would fit.
fn _send(io: Loop&, link: goose_io::Link, first: const u8[:], second: const u8[:]) -> goose_io::SendStatus {
    if io._finished { return goose_io::SendStatus.Closed; }
    let i = io._find(link);
    if i < 0 { return goose_io::SendStatus.Closed; }
    let s .= io._links[i];
    if s.state == _DIALING { return goose_io::SendStatus.NotOpen; }
    if s.state != _OPEN || s.closing || s.wr_shut || s.shut_wr_pending { return goose_io::SendStatus.Closed; }
    let total = first.len + second.len;
    if total == 0 { return goose_io::SendStatus.Sent; }
    let size = io.limits.buffer_size;
    if total > size * 4 { return goose_io::SendStatus.TooLarge; }               // can never fit, however empty the queue gets: no Writable is promised
    // How many new buffers does it need, and can the queue and the pool supply them? Decide before copying anything.
    let tail_room = if s.out_count > 0 { size - s.out_lengths[s.out_count - 1] } else { 0 };
    let extra = total - tail_room;
    let new_buffers = if extra > 0 { (extra + size - 1) / size } else { 0 };
    let available = io._free_buffers.len + (io.limits.buffers_per_loop - io._stats.buffers_total);
    if s.out_count + new_buffers > 4 {
        s.want_writable = true;
        if total > s.refused { s.refused = total as i32; }
        return goose_io::SendStatus.Full;                      // the queue drains and _flush_output says Writable
    }
    if new_buffers > available {
        // The pool is empty and this link has nothing queued, so no flush will ever run for it: wait in the list of readers waiting for a
        // buffer, and _wake_waiting says Writable when one comes back. Once per episode (want_writable is the "already waiting" mark).
        if !s.want_writable && s.out_count == 0 { io._waiting_reads.push(_pack_event_data(_WAITER_WRITER, i, s.gen)); io._stats.buffer_waits++; }
        s.want_writable = true;
        if total > s.refused { s.refused = total as i32; }
        return goose_io::SendStatus.Full;
    }
    io._append(i, first);
    io._append(i, second);
    s.last_active_ms = io._now_ms;
    io._flush_output(i);
    goose_io::SendStatus.Sent
}

fn _append(io: Loop&, i: i64, bytes: const u8[:]) {
    let s .= io._links[i];
    let size = io.limits.buffer_size;
    var offset = 0;
    while offset < bytes.len {
        if s.out_count == 0 || s.out_lengths[s.out_count - 1] == size {
            s.out_buffers[s.out_count] = io._take_buffer();
            s.out_lengths[s.out_count] = 0;
            s.out_count++;
        }
        let k = s.out_count - 1;
        let used = s.out_lengths[k];
        let n = if size - used < bytes.len - offset { size - used } else { bytes.len - offset };
        let target = io._buffer_slice(s.out_buffers[k])[used..used + n];
        target.move_from(bytes[offset..offset + n]);
        s.out_lengths[k] += n as i32;
        offset += n;
    }
}

// Writes the queue as far as the socket takes it. When it is empty: finish a close or a shutdown that was waiting for it, and say
// Writable if a refused send would fit now.
fn _flush_output(io: Loop&, i: i64) {
    let s .= io._links[i];
    if s.state != _OPEN { return; }
    while s.out_count > 0 {
        let rest = io._buffer_slice(s.out_buffers[0])[s.out_pos..s.out_lengths[0]];
        let n = c_send(s.fd, rest[0], rest.len);
        if n > 0 {
            s.out_pos += n as i32;
            s.last_active_ms = io._now_ms;                 // bytes are moving: not idle, however long the queue takes to drain
            io._stats.bytes_out += n;
            if s.out_pos == s.out_lengths[0] {              // the oldest buffer is done: back to the pool, the others move up
                io._give_back_buffer(s.out_buffers[0]);
                for k in 1..4 { s.out_buffers[k - 1] = s.out_buffers[k]; s.out_lengths[k - 1] = s.out_lengths[k]; }
                s.out_count--;
                s.out_pos = 0;
            }
        } else if n == -(c_EINTR as i64) {
            continue;
        } else if n == -(c_EAGAIN as i64) {
            break;
        } else {
            io._end(i, goose_io::CloseReason.PeerReset);
            return;
        }
    }
    if s.out_count == 0 && s.closing { io._finish_close(i); return; }
    if s.out_count == 0 && s.shut_wr_pending {
        s._remember_peer();
        c_shutdown_wr(s.fd);
        s.wr_shut = true;
        s.shut_wr_pending = false;
        if s.half_closed_at_ms == 0 { s.half_closed_at_ms = io._now_ms; }
        if s.eof_handled { io._end(i, goose_io::CloseReason.PeerClosed); return; }      // both directions are finished
    }
    if s.want_writable && s.state == _OPEN && !s.closing && io._send_room_of(i) >= s.refused {
        s.want_writable = false;
        s.refused = 0;
        io._queue(_PENDING_WRITABLE, i);
    }
    io._update_interest(i);
}

fn pause_reading(io: Loop&, link: goose_io::Link, paused: bool) {
    if io._finished { return; }
    let i = io._find(link);
    if i < 0 { return; }
    let s .= io._links[i];
    if (s.state != _OPEN && s.state != _DIALING) || s.closing { return; }
    s.paused = paused;
    if !paused && s.data_pending && !s.data_queued && s.state == _OPEN {      // bytes read while it was paused: handed over now
        s.data_queued = true;
        io._queue(_PENDING_DATA, i);
    }
    io._update_interest(i);
}

// Sends what was accepted and ends the link cleanly. On a dial that has not opened it cancels the dial. The one Closed
// (reason Local) follows; it may come after the pass if the bytes are still going out or the peer has not yet ended.
fn close(io: Loop&, link: goose_io::Link) {
    if io._finished { return; }
    let i = io._find(link);
    if i < 0 { return; }
    let s .= io._links[i];
    if s.closing || s.state == _ENDED { return; }
    s.local_closed = true;
    if s.state == _DIALING {
        io._end(i, goose_io::CloseReason.Local);
        return;
    }
    io._begin_close(i, goose_io::CloseReason.Local);
}

// Close once the queue is empty (now, if it already is).
fn _begin_close(io: Loop&, i: i64, reason: goose_io::CloseReason) {
    let s .= io._links[i];
    if s.closing || s.state != _OPEN { return; }
    s.closing = true;
    s.closing_reason = reason;
    s.closing_at_ms = io._now_ms;
    io._flush_output(i);
}

// The queue is empty and a close was waiting for it. When the application closed and the peer has not finished sending, closing the
// socket now could reset the connection (input arrived and unread) and the peer would lose bytes it was sent. So the link ends
// now (Closed, reason Local, delivered after the handler that called close has returned), the write side is shut, and the socket
// becomes an orphan that reads and drops input until the peer's end of stream, or the half-close limit (_sweep_orphans).
fn _finish_close(io: Loop&, i: i64) {
    let s .= io._links[i];
    if _is_local(s.closing_reason) && !s.eof_seen {
        s._remember_peer();
        c_shutdown_wr(s.fd);
        s.wr_shut = true;
        s.orphan = true;
    }
    io._end(i, s.closing_reason);
}

fn _adopt_orphan(io: Loop&, fd: i32) {
    var k: i64 = -1;
    for j in io._orphans.len { if io._orphans[j].fd < 0 { k = j; break; } }
    if k < 0 { k = io._orphans.len; io._orphans.push(_Orphan { fd: -1, since_ms: 0 }); }
    if c_ring_ctl(io._ring_fd, c_WATCH_MOD, fd, c_POLLIN, _pack_event_data(_EVENT_KIND_ORPHAN, k, 0)) < 0 { c_close(fd); return; }
    io._orphans[k] = _Orphan { fd: fd, since_ms: io._now_ms };
    io._orphans_open++;
}

// Reads and drops what an orphan's peer sends; closes it at the peer's end of stream or on an error.
fn _drain_orphan(io: Loop&, k: i64) {
    if k >= io._orphans.len || io._orphans[k].fd < 0 { io._stats.bad_events++; return; }
    let o .= io._orphans[k];
    for n in 16 {
        let r = c_recv(o.fd, io._scratch[0], io._scratch.len);
        if r > 0 { continue; }
        if r == -(c_EAGAIN as i64) || r == -(c_EINTR as i64) { return; }
        c_close(o.fd);
        o.fd = -1;
        io._orphans_open--;
        return;
    }
}

fn _sweep_orphans(io: Loop&, now: i64) {
    if io._orphans_open == 0 { return; }
    for k in io._orphans.len {
        let o .= io._orphans[k];
        if o.fd >= 0 && now - o.since_ms >= io.limits.half_close_timeout_ms { c_close(o.fd); o.fd = -1; io._orphans_open--; }
    }
}

// Closes now with an RST and discards what was not yet sent.
fn reset(io: Loop&, link: goose_io::Link) {
    if io._finished { return; }
    let i = io._find(link);
    if i < 0 { return; }
    let s .= io._links[i];
    if s.state == _ENDED { return; }
    s.local_closed = true;
    if s.state == _OPEN && s.fd >= 0 { c_linger0(s.fd); }
    s.closing = true;
    s.closing_reason = goose_io::CloseReason.Local;
    io._end(i, goose_io::CloseReason.Local);
}

// Keeps the link open when the peer finishes sending: PeerEof once, and the application can go on sending. Recorded on a dial and
// while the link's Opened is handled; a later call has no effect.
fn set_keep_open_on_eof(io: Loop&, link: goose_io::Link, keep: bool) {
    if io._finished { return; }
    let i = io._find(link);
    if i < 0 { return; }
    let s .= io._links[i];
    if s.state == _DIALING || (s.state == _OPEN && s.window) { s.keep_open = keep; }
}

// Once the queued bytes are out, shuts down the write side: the peer reads end of stream after the last byte while the link can
// still receive. When the peer has also finished sending, the link closes with PeerClosed.
fn shutdown_write(io: Loop&, link: goose_io::Link) {
    if io._finished { return; }
    let i = io._find(link);
    if i < 0 { return; }
    let s .= io._links[i];
    if s.state != _OPEN || s.closing || s.wr_shut || s.shut_wr_pending { return; }
    s.shut_wr_pending = true;
    io._flush_output(i);
}

// The peer's address as the contract writes it, empty when the link has none (a dial in progress, a stale handle).
fn peer_address(io: Loop&, link: goose_io::Link) -> goose_io::Address {
    if io._finished { return goose_io::Address { .. }; }
    let i = io._find(link);
    if i < 0 { return goose_io::Address { .. }; }
    let s .= io._links[i];
    if s.state == _DIALING || s.fd < 0 { return goose_io::Address { .. }; }
    var raw: u8[18] = [0; 18];
    var family = s.peer_family;
    if family > 0 { for k in 18 { raw[k] = s.peer[k]; } } else { family = c_peer(s.fd, raw); }
    if family < 0 { return goose_io::Address { .. }; }
    var ip: u8[16] = [0; 16];
    for k in 16 { ip[k] = raw[k]; }
    let port = ((raw[16] as u16) << 8) | raw[17];
    _format_address(family == 6, ip, port)
}

// --- time and running ----------------------------------------------------------------------------

fn now_ms(io: Loop&) -> i64 {
    if io._now_ms == 0 { io._now_ms = c_boottime_ms(); }
    io._now_ms
}

// Asks for a Tick at or after at_ms. Only the earliest request is kept, and the first Tick at or after it clears it.
fn tick_by(io: Loop&, at_ms: i64) {
    if io._finished { return; }
    if io._wake_at == 0 || at_ms < io._wake_at { io._wake_at = if at_ms > 0 { at_ms } else { 1 }; }
}

// Every listener stops now; serve returns when the last link has ended, or grace_ms from now, after ending what is left with
// Shutdown. Of several calls the earliest end of grace stands.
fn stop(io: Loop&, grace_ms: i64) {
    if io._finished { return; }
    io._ensure();
    var deadline = io._now_ms + (if grace_ms > 0 { grace_ms } else { 0 });
    if !io._draining || deadline < io._drain_deadline_ms { io._drain_deadline_ms = deadline; }
    if io._draining { return; }
    io._draining = true;
    for k in io._listeners.len {
        if io._listeners[k].fd >= 0 { c_close(io._listeners[k].fd); io._listeners[k].fd = -1; }
        io._listeners[k].active = false;
    }
}

fn draining(io: Loop&) -> bool { io._draining || io._finished }

// The capabilities this loop supplies (contract 0.5): the six below. It does not supply unix, relay, peek, revoke, wake or limits.
fn supplies_half_close(io: Loop&) -> goose_io::Supplied { return goose_io::Supplied {}; }
fn supplies_sizing(io: Loop&) -> goose_io::Supplied { return goose_io::Supplied {}; }
fn supplies_abort(io: Loop&) -> goose_io::Supplied { return goose_io::Supplied {}; }
fn supplies_inspection(io: Loop&) -> goose_io::Supplied { return goose_io::Supplied {}; }
fn supplies_batching(io: Loop&) -> goose_io::Supplied { return goose_io::Supplied {}; }
fn supplies_shared_listen(io: Loop&) -> goose_io::Supplied { return goose_io::Supplied {}; }

// A snapshot of this loop's counters. Call it on the loop's thread, e.g. from a Tick.
fn stats(io: Loop&) -> Stats {
    var s = io._stats;
    s.waiting_len = io._waiting_reads.len;
    s
}

// At the end of the grace period: every link still there ends (Shutdown, or Local for one the application closed).
fn _enforce_stop(io: Loop&) {
    if !io._draining || io._live == 0 || io._now_ms < io._drain_deadline_ms { return; }
    for i in io._links.len {
        let s .= io._links[i];
        if s.state == _OPEN || s.state == _DIALING {
            if s.closing && _is_local(s.closing_reason) { io._end(i, goose_io::CloseReason.Local); }
            else { io._end(i, goose_io::CloseReason.Shutdown); }
        }
    }
}

// A Tick comes at least every 100 ms of the loop's clock; the loop aims at 90 so that the scheduler's slack and the
// millisecond rounding of the ring wait cannot make a gap longer than 100.
let _TICK_PERIOD_MS: i64 = 90;

// How long the ring wait may sleep: until the next Tick, or less when a dial's connect deadline or a time asked for with tick_by comes sooner.
fn _wait_limit(io: Loop&, last_tick: i64) -> i32 {
    let now = c_boottime_ms();
    var limit: i64 = last_tick + _TICK_PERIOD_MS - now;
    if io._dialing > 0 && io._dial_due != 0 && io._dial_due - now < limit { limit = io._dial_due - now; }
    if io._wake_at != 0 && io._wake_at - now < limit { limit = io._wake_at - now; }
    if io._draining && io._drain_deadline_ms - now < limit { limit = io._drain_deadline_ms - now; }
    if limit < 0 { 0 } else { limit as i32 }
}

// --- reading -------------------------------------------------------------------------------------

// One read; the bytes stay in the link's buffer and a Data record is queued, until the application has handled them.
fn _read_link(io: Loop&, i: i64) {
    let s .= io._links[i];
    if !s._wants_read() || s.data_pending { return; }
    let b = io._take_buffer();
    if b < 0 {                                           // pool empty: wait for a buffer, do not read
        s.waiting = true;
        io._waiting_reads.push(_pack_event_data(_WAITER_READER, i, s.gen));
        io._stats.buffer_waits++;
        io._update_interest(i);
        return;
    }
    let buffer = io._buffer_slice(b);
    let n = c_recv(s.fd, buffer[0], buffer.len);
    if n > 0 {
        s.rbuf = b;
        s.rlen = n as i32;
        s.last_active_ms = io._now_ms;
        s.data_pending = true;
        s.data_queued = true;
        io._stats.bytes_in += n;
        io._queue(_PENDING_DATA, i);
        io._update_interest(i);
        return;
    }
    io._give_back_buffer(b);
    if n == 0 {
        s.eof_seen = true;                       // no interest change here: the record decides, and the usual outcome is that the link ends
        io._queue(_PENDING_EOF, i);
        return;
    }
    if n == -(c_EAGAIN as i64) || n == -(c_EINTR as i64) { io._update_interest(i); return; }
    io._end(i, goose_io::CloseReason.PeerReset);
}

// The peer finished sending, and the record of it has been reached: end the link, or tell the application (keep-open).
fn _handle_eof<F>(io: Loop&, i: i64) {
    let s .= io._links[i];
    if s.state != _OPEN || s.closing || s.eof_handled { return; }
    s.eof_handled = true;
    if s.keep_open {                                       // the application wants to finish its answer
        if s.wr_shut { io._end(i, goose_io::CloseReason.PeerClosed); return; }
        s.half_closed_at_ms = io._now_ms;                   // the half-close limit bounds the wait for the application
        io._update_interest(i);                             // input has ended: stop watching it, or a level-triggered socket at end of stream would wake the loop for ever
        io._pass_delivered++;
        let e: goose_io::Event = goose_io::Event.PeerEof { link: io._link_of(i) };
        F(io, e);
        return;
    }
    io._begin_close(i, goose_io::CloseReason.PeerClosed);
}

// --- delivering events ---------------------------------------------------------------------------

// Calls the application's block for every queued event, in order, including those the block itself causes.
fn _deliver<F>(io: Loop&) {
    while io._pending_head < io._pending.len {
        let r = io._pending[io._pending_head];
        io._pending_head++;
        let i = r.slot as i64;
        if i >= io._links.len { continue; }
        if io._links[i].gen != r.gen || io._links[i].state == _FREE { continue; }
        let link = goose_io::Link { slot: r.slot, gen: r.gen };
        if r.kind == _PENDING_OPENED {
            if io._links[i].local_closed { continue; }                // closed before it opened: only the one Closed
            io._links[i].window = true;
            io._pass_delivered++;
            let e: goose_io::Event = goose_io::Event.Opened { link: link, listener: goose_io::Listener { id: r.listener } };
            F(io, e);
            io._links[i].window = false;
        } else if r.kind == _PENDING_DATA {
            let s .= io._links[i];
            s.data_queued = false;
            if !s.data_pending || s.local_closed || s.rbuf < 0 { continue; }
            if s.paused { continue; }                                // held in the buffer until reading is resumed
            let bytes = io._buffer_slice(s.rbuf)[..s.rlen];
            io._pass_delivered++;
            let e: goose_io::Event = goose_io::Event.Data { link: link, bytes: bytes };
            F(io, e);
            io._data_delivered(i, r.gen);
        } else if r.kind == _PENDING_WRITABLE {
            let s .= io._links[i];
            if s.state != _OPEN || s.closing || s.wr_shut || s.shut_wr_pending { continue; }     // nothing can fit now: the event would be untrue
            io._pass_delivered++;
            let e: goose_io::Event = goose_io::Event.Writable { link: link };
            F(io, e);
        } else if r.kind == _PENDING_EOF {
            io._handle_eof(i, F);
        } else {
            io._pass_delivered++;
            let e: goose_io::Event = goose_io::Event.Closed { link: link, why: r.why };
            F(io, e);
            io._release(i);
        }
    }
    io._pending.clear();
    io._pending_head = 0;
}

// The block has seen the bytes: the buffer goes back and the link may be read again.
fn _data_delivered(io: Loop&, i: i64, gen: u32) {
    if io._links[i].gen != gen { return; }
    let s .= io._links[i];
    let b = s.rbuf;
    s.rbuf = -1;
    s.rlen = 0;
    s.data_pending = false;
    if b >= 0 { io._give_back_buffer(b); }
    if s.state == _OPEN { io._update_interest(i); }
}

// --- timeouts ------------------------------------------------------------------------------------

// One pass over the slot table, every 250 ms: there is no timer per link. It ends a link with no bytes either way (idle), one
// left half-open (half-close), and one the application closed that the peer will not finish (the half-close limit).
fn _sweep_timeouts(io: Loop&, now: i64) {
    let limits = io.limits;
    for i in io._links.len {
        let s .= io._links[i];
        if s.state != _OPEN { continue; }
        if s.closing_at_ms != 0 && _is_local(s.closing_reason) {
            if now - s.closing_at_ms >= limits.half_close_timeout_ms { io._end(i, goose_io::CloseReason.Local); }
        } else if s.half_closed_at_ms != 0 {
            let since = if s.last_active_ms > s.half_closed_at_ms { s.last_active_ms } else { s.half_closed_at_ms };
            if now - since >= limits.half_close_timeout_ms { io._end(i, goose_io::CloseReason.HalfCloseTimeout); }
        } else if now - s.last_active_ms >= limits.idle_timeout_ms {
            io._end(i, goose_io::CloseReason.IdleTimeout);
        }
    }
}

// --- the event loop's dispatch -------------------------------------------------------------------

fn _dispatch_event(io: Loop&, data: u64, events: u32) {
    let kind = data & 0xff;
    let gen = ((data >> 8) & 0xffffff) as u32;
    let slot = (data >> 32) as i64;

    // The kind and the slot come from the kernel's event data: check them, never trust them.
    if kind == _EVENT_KIND_LISTENER {
        if slot < io._listeners.len { io._accept_ready(slot); } else { io._stats.bad_events++; }
        return;
    }
    if kind == _EVENT_KIND_ORPHAN { io._drain_orphan(slot); return; }
    if kind != _EVENT_KIND_LINK { io._stats.bad_events++; return; }
    if slot >= io._links.len { io._stats.bad_events++; return; }
    let s .= io._links[slot];
    if s.state == _FREE || s.gen != gen { io._stats.stale_events++; return; }     // an event for a link that has ended (and maybe whose slot is reused)
    if s.state == _ENDED { return; }                                 // Closed is queued: the descriptor is closed when it has been handled
    if s.state == _DIALING { io._dial_finished(slot); return; }      // success, refusal or error: the socket says which
    if (events & c_POLLERR) != 0 { io._end(slot, goose_io::CloseReason.PeerReset); return; }
    if (events & c_POLLOUT) != 0 {
        io._flush_output(slot);
        if s.state != _OPEN { return; }
    }
    if (events & (c_POLLIN | c_POLLHUP)) != 0 {
        if (events & c_POLLIN) == 0 && !s._wants_read() { io._end(slot, goose_io::CloseReason.PeerReset); return; }
        io._read_link(slot);
    }
}

// --- the batching window -------------------------------------------------------------------------

// Called when a pass found `n` events: if there are fewer than limits.batch_min_events, waits up to batch_max_wait_us for more and
// appends what arrived to `events`; returns how many it appended. Off (both limits 0) it returns at once and costs nothing.
// It never waits past a deadline (the next Tick, the next sweep, a tick_by time, a dial's deadline, a paused listener's resume),
// is not entered while draining, waits on the loop's message pipe so a posted message ends it at once, and reads the clock again.
fn _batch_window(io: Loop&, events: c_RingEvent[256]&, n: i32, last_tick: i64, last_sweep: i64) -> i32 {
    let min = io.limits.batch_min_events;
    var us = io.limits.batch_max_wait_us;
    if min <= 0 || us <= 0 || n >= min || n >= 256 || io._draining { return 0; }
    for k in n { if (events[k].data & 0xff) == _EVENT_KIND_MESSAGE { return 0; } }
    let now = c_boottime_ms();
    var next: i64 = last_tick + _TICK_PERIOD_MS;
    if last_sweep + 250 < next { next = last_sweep + 250; }
    if io._wake_at != 0 && io._wake_at < next { next = io._wake_at; }
    if io._dialing > 0 && io._dial_due != 0 && io._dial_due < next { next = io._dial_due; }
    for k in io._listeners.len { if io._listeners[k].resume_at != 0 && io._listeners[k].resume_at < next { next = io._listeners[k].resume_at; } }
    let room_us = (next - now) * 1000;
    if room_us < us { us = room_us; }
    if us <= 0 { return 0; }
    let slept = c_wait_readable_us(io._message_fd, us);
    io._stats.batch_waits++;
    if slept > 0 { io._stats.batch_wait_us += slept; }
    let m = c_ring_wait(io._ring_fd, events[n], 256 - n, 0);        // everything that arrived meanwhile
    io._now_ms = c_boottime_ms();
    if m > 0 { m } else { 0 }
}

// --- serve ---------------------------------------------------------------------------------------

// Runs the loop on the calling thread until it has been stopped and drained, calling the block once per event, one at a time.
// (A `return` in the block leaves the function that contains it: call a named function from the block.) A loop that has finished
// does nothing: a second serve returns at once and never calls the block.
fn serve<F>(io: Loop&) {
    if io._finished { return; }
    if !io._ensure() { io._finished = true; return; }
    var events:     c_RingEvent[256] = [c_RingEvent { 0, 0 }; 256];
    var last_tick:  i64              = 0;
    var last_sweep: i64              = 0;
    loop {
        // work left over (events queued by what was set up before serve, or by the last pass): do not sleep
        let wait_ms: i32 = if io._pending_head < io._pending.len { 0 } else { io._wait_limit(last_tick) };
        var n = c_ring_wait(io._ring_fd, events[0], 256, wait_ms);
        io._now_ms = c_boottime_ms();
        if n > 0 && wait_ms > 0 { n += io._batch_window(events, n, last_tick, last_sweep); }
        // Messages first, then events: a drain must not be outrun by traffic in the same pass.
        if n > 0 { for k in n { if (events[k].data & 0xff) == _EVENT_KIND_MESSAGE { io._read_messages(); } } }
        if n < 0 {
            _pause_milliseconds(1);                     // an unexpected ring error: do not spin
        } else {
            for k in n { if (events[k].data & 0xff) != _EVENT_KIND_MESSAGE { io._dispatch_event(events[k].data, events[k].events); } }
        }
        io._deliver(F);
        io._enforce_stop();
        io._deliver(F);
        let now = io._now_ms;
        if io._draining && io._live == 0 && io._pending_head >= io._pending.len {
            io._deliver(F);
            if io._live == 0 && io._pending_head >= io._pending.len {
                io._finish_pass(F);
                break;
            }
        }
        io._resume_paused_listeners(now);
        if io._dialing > 0 && io._dial_due != 0 && now >= io._dial_due { io._expire_dials(now); }
        if now - last_sweep >= 250 {
            last_sweep = now;
            io._sweep_timeouts(now);
            io._sweep_orphans(now);
        }
        if now - last_tick >= _TICK_PERIOD_MS || (io._wake_at != 0 && now >= io._wake_at) {
            last_tick = now;
            io._wake_at = 0;
            let tick: goose_io::Event = goose_io::Event.Tick { now_ms: now };
            F(io, tick);
        }
        io._deliver(F);
        io._finish_pass(F);
    }
    io._finished = true;
    for k in io._listeners.len {
        if io._listeners[k].fd >= 0 { c_close(io._listeners[k].fd); io._listeners[k].fd = -1; }
    }
    for k in io._orphans.len { if io._orphans[k].fd >= 0 { c_close(io._orphans[k].fd); io._orphans[k].fd = -1; } }
    c_ring_close(io._ring_fd);
    io._ring_fd = -1;
    if io._attached { qput(_DrainReport { drained: io._stats.closed[6] }); }
}

// Ends a pass: one PassEnd if the pass delivered link events. Always the last event of its pass.
fn _finish_pass<F>(io: Loop&) {
    if io._pass_delivered == 0 { return; }
    io._pass_delivered = 0;
    let e: goose_io::Event = goose_io::Event.PassEnd {};
    F(io, e);
    io._deliver(F);
    io._pass_delivered = 0;
}
