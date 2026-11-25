// A server that keeps no state of its own for a link and answers nothing: what the loop costs for an idle link. For bench/memory.py.
// It listens on 0.0.0.0 on the port given as the first argument, and drains for the number of milliseconds given as the second (30000 by default).
import std;
import os;
import goose_io from "github.com/BHare1985/goose-io@apply-name-resolution";
import iouring from "..";

var address:  u8[>..] = [];
var grace_ms: i64     = 30000;

thread_fn worker(index: i64) {
    var io = iouring::Loop { .. };
    let listener, err = io.listen(address[..]);
    if goose_io::failed(err) { print("cannot listen"); iouring::report_failed_start(); return; }
    io.attach(index);
    io.serve() { io, ev => };
}

fn main() {
    address.append("0.0.0.0:");
    if arg_count() > 1 { arg(1, address); } else { address.append("8080"); }
    if arg_count() > 2 {
        var t: u8[>..] = [];
        arg(2, t);
        grace_ms = 0;
        for ch in t { if ch >= '0' && ch <= '9' { grace_ms = grace_ms * 10 + (ch - '0'); } }
    }
    iouring::spawn(1, worker);
    iouring::wait_for_signal_then_drain(grace_ms);
}
