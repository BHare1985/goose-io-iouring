// Runs one part of the conformance suite against the io_uring event loop, on real sockets:
//
//     goose build conformance.goose -o conformance && ./conformance <part> <capability>
//
// The part is the first argument (default 0) and the capability the second (core, half-close, sizing, abort, inspection, batching or name-resolution;
// default core). Both ends of every link are on the loop under test, so the runner supplies what a contract call cannot make
// (docs/event-loops.md, the fixture), without privileges, on the loopback:
//
//   silent   a listener with a backlog of one whose accept queue is full and never drained: its kernel drops further connects, so
//            a dial to it is never answered and ends by its own timeout
//   held     a socket bound and listening that the runner keeps, so a listen on its address fails with AddressInUse
//   refused  a port that was bound and then released: nothing listens, so a dial is refused
//   answers  a listener the kernel keeps completing connects for, so a dial made while the loop is draining still opens
import std;
import os;
import goose_io from "github.com/BHare1985/goose-io@apply-name-resolution";
import goose_io.conformance.suite;
import iouring from ".";

// The runner's own use of the loop's C layer: declared again here, because a program may name the symbols it links.
extern fn c_listen(v6: bool, ip: const u8[16]&, port: u16, reuseport: bool, backlog: i32) -> i32;
extern fn c_local_port(fd: i32) -> i32;
extern fn c_connect_start(v6: bool, ip: const u8[16]&, port: u16) -> i32;
extern fn c_close(fd: i32);

var fixture_keep: i32[16] = [-1; 16];
var fixture_kept: i64     = 0;

fn keep(fd: i32) { fixture_keep[fixture_kept] = fd; fixture_kept++; }

fn loopback() -> u8[16] {
    var ip: u8[16] = [0; 16];
    ip[0] = 127; ip[3] = 1;
    return ip;
}

// Binds 127.0.0.1:0 with the backlog and returns the address text of what it got, keeping the socket open.
fn make_listener(backlog: i32, text: u8[>..]&) -> i32 {   // appends the address text
    var ip = loopback();
    let fd = c_listen(false, ip, 0, false, backlog);
    if fd < 0 { print("the runner could not listen: ", -fd); exit(2); }
    format(text, "127.0.0.1:", c_local_port(fd));
    return fd;
}

fn main() {
    var part = 0;
    if arg_count() > 1 {
        var text: u8[>..] = [];
        arg(1, text);
        for c in text { if c >= '0' && c <= '9' { part = part * 10 + (c - '0'); } }
    }
    var capability: u8[>..] = [];
    if arg_count() > 2 { arg(2, capability); }
    // Optional, for the name-resolution part: the name that has two addresses and the first and second of them, in the order the system gives them
    // (conformance.sh asks getent), as "ip:80" texts an address of `dial` takes. Without them the checks of the order are skipped.
    var pair_first:  u8[>..] = [];
    var pair_second: u8[>..] = [];
    var pair_name:   u8[>..] = [];
    if arg_count() > 5 { arg(3, pair_name); arg(4, pair_first); arg(5, pair_second); }

    // All four address texts live in one array: the suite is only allowed to call a loop that shrinks its own arrays when the views
    // it holds are of one array exactly (the language's aliasing rule), so the fixture is four slices of one.
    var texts: u8[>..] = [];

    // silent: fill the accept queue (a backlog of one holds two completed connections), then more connects are dropped
    let sfd = make_listener(1, texts);
    keep(sfd);
    let silent_end = texts.len;
    var ip = loopback();
    var port: u16 = c_local_port(sfd) as u16;
    for k in 6 { let c = c_connect_start(false, ip, port); if c >= 0 { keep(c); } }

    keep(make_listener(16, texts));
    let held_end = texts.len;

    keep(make_listener(1024, texts));
    let answers_end = texts.len;

    let rfd = make_listener(1, texts);
    c_close(rfd);

    var io = iouring::Loop { .. };
    io.limits.half_close_timeout_ms = 3000;
    goose_io::require_core(io);
    goose_io::require_half_close(io);
    goose_io::require_sizing(io);
    goose_io::require_abort(io);
    goose_io::require_inspection(io);
    goose_io::require_name_resolution(io);
    let fixture = goose_io_conformance::Fixture { silent: texts[..silent_end], held: texts[silent_end..held_end], refused: texts[answers_end..], answers: texts[held_end..answers_end], unix: "", found: "localhost", missing: "no-such-name.invalid", pair: pair_name[..], pair_first: pair_first[..], pair_second: pair_second[..] };
    if capability == "half-close" { exit(goose_io_conformance::conformance_half_close_part(io, fixture, part)); }
    if capability == "sizing" { exit(goose_io_conformance::conformance_sizing_part(io, fixture, part)); }
    if capability == "abort" { exit(goose_io_conformance::conformance_abort_part(io, fixture, part)); }
    if capability == "inspection" { exit(goose_io_conformance::conformance_inspection_part(io, fixture, part)); }
    if capability == "name-resolution" { exit(goose_io_conformance::conformance_name_resolution_part(io, fixture, part)); }
    if capability == "batching" { exit(goose_io_conformance::conformance_batching_part(io, fixture, part)); }
    exit(goose_io_conformance::conformance_core_part(io, fixture, part));
}
