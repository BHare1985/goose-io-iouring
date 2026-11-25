// The example line server on the io_uring event loop, for tests/net/lineserver_test.py: it answers each line with "echo " and the
// line (a line that is exactly "BIG" is answered with a reply of 20000 bytes). It listens on the address given as the first
// argument (and the half-close limit in milliseconds, the second argument, if given) and stops on SIGTERM or SIGINT, draining for
// up to ten seconds.
//
// Needs: core and half-close. It fails to build on a loop without them.
import std;
import os;
import goose_io from "github.com/BHare1985/goose-io@apply-name-resolution";
import goose_io.examples.lineserver.server;
import iouring from "..";

var address: u8[>..] = [];
var half_ms: i64     = 0;

fn answer(server: lineserver::LineServer&, io: iouring::Loop&) {
    loop {
        var line: u8[>..] = [];
        let r = server.take_request(io, line);
        if !r.found { break; }
        var text: u8[>..] = [];
        if line == "BIG" { for k in 20000 { text.push('a' + (k % 26) as u8); } } else { format(text, "echo ", line); }
        server.reply(io, r, text[..]);
    }
}

thread_fn worker(index: i64) {
    var io = iouring::Loop { .. };
    if half_ms > 0 { io.limits.half_close_timeout_ms = half_ms; }
    goose_io::require_core(io);
    goose_io::require_half_close(io);
    var server = lineserver::LineServer { .. };
    let l, lerr = io.listen_shared(address[..]);
    server.listener = l;
    if !goose_io::is_listener(l) || goose_io::failed(lerr) { print("cannot listen"); iouring::report_failed_start(); return; }
    io.attach(index);
    io.serve() { io, ev =>
        server.server_event(io, ev);
        answer(server, io);
    };
}

fn main() {
    if arg_count() > 1 { arg(1, address); } else { address.append("127.0.0.1:7000"); }
    if arg_count() > 2 { var t: u8[>..] = []; arg(2, t); for ch in t { if ch >= '0' && ch <= '9' { half_ms = half_ms * 10 + (ch - '0'); } } }
    iouring::spawn(1, worker);
    iouring::wait_for_signal_then_drain(10000);
}
