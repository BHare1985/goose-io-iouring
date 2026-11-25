// The example gateway on the io_uring event loop, for tests/net/shared_worker_test.py: the line server and the line client on one
// worker, neither naming the other. The front server listens on the first argument and answers each request with the reply
// the client fetches from a backend, a second line server on the same worker, which listens on the second argument.
// Stops on SIGTERM or SIGINT.
//
// Needs: core and half-close.
import std;
import os;
import goose_io from "github.com/BHare1985/goose-io@apply-name-resolution";
import goose_io.examples.gateway;
import goose_io.examples.lineserver.server;
import goose_io.examples.lineclient.client;
import iouring from "..";

var front_address: u8[>..] = [];
var back_address:  u8[>..] = [];

thread_fn worker(index: i64) {
    var io = iouring::Loop { .. };
    goose_io::require_core(io);
    goose_io::require_half_close(io);
    var front = lineserver::LineServer { .. };
    var back = lineserver::LineServer { .. };
    var client = lineclient::LineClient { .. };
    var table = gateway::Table { .. };
    if !front.start(io, front_address[..]) || !back.start(io, back_address[..]) { print("cannot listen"); iouring::report_failed_start(); return; }
    if !client.start(io, back_address[..], 2, 100) { print("bad address"); iouring::report_failed_start(); return; }
    io.attach(index);
    io.serve() { io, ev => gateway::step(front, back, client, table, io, ev); };
}

fn main() {
    if arg_count() > 2 { arg(1, front_address); arg(2, back_address); } else { front_address.append("127.0.0.1:7000"); back_address.append("127.0.0.1:7001"); }
    iouring::spawn(1, worker);
    iouring::wait_for_signal_then_drain(10000);
}
