// The example echo server on the io_uring event loop, with the loop's limits set from the command line, for the network tests that
// need a limit (tests/net/*_test.py). Usage:
//
//     echo_server ADDRESS [workers] [name=value ...]
//
// Limits: max_links, buffers (buffers_per_loop), buffer_size, idle_ms, half_ms, connect_ms. `keep=1` keeps a link open when the
// peer finishes sending (and then never closes it, so only the half-close limit ends it). It stops on SIGTERM or SIGINT, and
// on SIGUSR1 prints the counters of every loop added up (`STATS accepted=.. open=.. buffers_total=.. buffer_waits=.. closed_*=..`)
// and carries on.
//
// Needs: core (and half-close for keep=1).
import std;
import os;
import goose_io from "github.com/BHare1985/goose-io@apply-name-resolution";
import goose_io.examples.echo.echo;
import iouring from "..";

var address: u8[>..] = [];
var workers: i64     = 1;
var limits = iouring::Limits { .. };
var keep: bool = false;

fn number(text: const u8[:]) -> i64 {
    var n = 0;
    for ch in text { if ch >= '0' && ch <= '9' { n = n * 10 + (ch - '0'); } }
    return n;
}

fn setting(text: const u8[:]) {
    let eq = find(text, "=");
    if eq < 0 { return; }
    let name = text[..eq];
    let v = number(text[eq + 1..]);
    if name == "max_links" { limits.max_links = v; }
    else if name == "buffers" { limits.buffers_per_loop = v; }
    else if name == "buffer_size" { limits.buffer_size = v; }
    else if name == "idle_ms" { limits.idle_timeout_ms = v; }
    else if name == "half_ms" { limits.half_close_timeout_ms = v; }
    else if name == "connect_ms" { limits.connect_timeout_ms = v; }
    else if name == "keep" { keep = v != 0; }
}

fn on_event(e: echo::Echo&, io: iouring::Loop&, ev: goose_io::Event) {
    if keep {
        match ev {
            Opened o => { io.set_keep_open_on_eof(o.link, true); },
            _ => {},
        }
    }
    e.echo_event(io, ev);
}

thread_fn worker(index: i64) {
    var io = iouring::Loop { .. };
    io.limits = limits;
    var e = echo::Echo { .. };
    if !e.start(io, address[..]) {
        print("cannot listen on ", address);
        iouring::report_failed_start();
        return;
    }
    io.attach(index);
    io.serve() { io, ev => on_event(e, io, ev); };
}

fn main() {
    if arg_count() > 1 { arg(1, address); } else { address.append("127.0.0.1:7000"); }
    if arg_count() > 2 { var t: u8[>..] = []; arg(2, t); workers = number(t[..]); }
    for k in 3..arg_count() { var t: u8[>..] = []; arg(k, t); setting(t[..]); }
    let probed = iouring::probe(address[..]);
    if goose_io::failed(probed) {
        var msg: u8[>..] = [];
        iouring::describe_listen_failure(address[..], probed, msg);
        print(msg);
        exit(2);
    }
    iouring::spawn(workers, worker);
    loop {
        let sig = iouring::wait_for_signal();
        if sig != 10 { break; }
        let s = iouring::gather_stats(workers);
        print("STATS accepted=", s.accepted, " open=", s.open, " buffers_total=", s.buffers_total, " buffer_waits=", s.buffer_waits,
              " closed_peer=", s.closed[0], " closed_reset=", s.closed[1], " closed_idle=", s.closed[3], " closed_half=", s.closed[4],
              " closed_local=", s.closed[5], " accept_failures=", s.accept_failures);
    }
    iouring::drain(10000);
}
