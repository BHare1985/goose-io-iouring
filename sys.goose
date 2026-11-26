// The C layer, bound for the io_uring event loop's own use. Not part of the contract. One place for every `extern fn`, with the
// struct layouts and constants they share with C. Everything here comes from C, so everything here is named c_...,
// exactly as in src/sys.c. Every function returns a negative errno on failure (-11, EAGAIN, means "not now"), which
// src/sys.c arranges because errno is thread-local. (This is tcprouter's sys.goose, less what the proxy used.)
namespace iouring;

// the event record the C layer fills in: packed, 12 bytes (checked there by _Static_assert).
struct c_RingEvent { events: u32, data: u64 }

let c_POLLIN:    u32 = 0x001;
let c_POLLOUT:   u32 = 0x004;
let c_POLLERR:   u32 = 0x008;
let c_POLLHUP:   u32 = 0x010;
let c_POLLRDHUP: u32 = 0x2000;
let c_WATCH_ADD: i32 = 1;
let c_WATCH_DEL: i32 = 2;
let c_WATCH_MOD: i32 = 3;

let c_EAGAIN:        i32 = 11;
let c_EINTR:         i32 = 4;
let c_EPERM:         i32 = 1;
let c_EACCES:        i32 = 13;
let c_ENOMEM:        i32 = 12;
let c_EMFILE:        i32 = 24;
let c_ENFILE:        i32 = 23;
let c_EADDRINUSE:    i32 = 98;
let c_EADDRNOTAVAIL: i32 = 99;
let c_ENETUNREACH:   i32 = 101;
let c_ENOBUFS:       i32 = 105;
let c_ECONNABORTED:  i32 = 103;
let c_ECONNREFUSED:  i32 = 111;
let c_EHOSTUNREACH:  i32 = 113;
let c_ETIMEDOUT:     i32 = 110;
let c_EPROTO:        i32 = 71;

extern fn c_ring_create() -> i32;
extern fn c_ring_close(ep: i32);
extern fn c_ring_ctl(ep: i32, op: i32, fd: i32, events: u32, data: u64) -> i32;
extern fn c_ring_wait(ep: i32, out: c_RingEvent&, max: i32, timeout_ms: i32) -> i32;

// Sockets. `ip` is 4 bytes (IPv4) or 16 (IPv6) in network order, `v6` says which.
extern fn c_listen(v6: bool, ip: const u8[16]&, port: u16, reuseport: bool, backlog: i32) -> i32;
extern fn c_local_port(fd: i32) -> i32;
extern fn c_peer(fd: i32, out: u8[18]&) -> i32;
extern fn c_accept(lfd: i32) -> i32;
extern fn c_connect_start(v6: bool, ip: const u8[16]&, port: u16) -> i32;
extern fn c_connect_result(fd: i32) -> i32;
extern fn c_recv(fd: i32, buf: u8&, len: i64) -> i64;
extern fn c_send(fd: i32, buf: const u8&, len: i64) -> i64;
extern fn c_shutdown_wr(fd: i32);
extern fn c_close(fd: i32);
extern fn c_linger0(fd: i32);
extern fn c_strerror(out: u8[>..]&, err: i32);

extern fn c_pipe(fds: i32[2]&) -> i32;
extern fn c_pipe_write(fd: i32, buf: const u8&, len: i64) -> i64;
extern fn c_pipe_read(fd: i32, buf: u8&, len: i64) -> i64;

extern fn c_wait_readable_us(fd: i32, us: i64) -> i64;
extern fn c_signals_open() -> i32;
extern fn c_signal_wait(fd: i32) -> i32;

// CLOCK_BOOTTIME in milliseconds: counts through suspend.
extern fn c_boottime_ms() -> i64;
