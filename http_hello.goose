// A tiny HTTP/1.1 server on the io_uring event loop: every request gets "hello". Keep-alive works, and a request may arrive in
// pieces. It is tcprouter's examples/http_hello.goose, moved to the contract: the state of a connection is a `LinkMap` keyed by the
// link instead of an index given to the engine with set_user. It is the program the performance gates measure on both loops.
//
//     ./build.sh http_hello.goose http_hello
//     ./http_hello [port [workers]]        (port 8080 and one worker by default)
//
// Needs: core only.
import std;
import os;
import goose_io from "github.com/BHare1985/goose-io@apply-name-resolution";
import iouring from ".";

struct Session { inbox: u8[..2048] = [] }

// The response is small, so one send is enough.
let RESPONSE = "HTTP/1.1 200 OK\r\nContent-Length: 6\r\nContent-Type: text/plain\r\n\r\nhello\n";

// One request head ends at a blank line. Answers every complete head in the buffer.
// (A function of its own: a `return` inside the serve block would leave `worker`, not the block.)
fn on_data(io: iouring::Loop&, sessions: goose_io::LinkMap<Session>&, link: goose_io::Link, bytes: const u8[:]) {
    let found = sessions.find(link);
    guard found else { return; }
    var s .= found;
    if s.inbox.len + bytes.len > s.inbox.cap {            // request head too large
        io.close(link);
        return;
    }
    s.inbox.append(bytes);
    loop {
        let end = find(s.inbox, "\r\n\r\n");
        if end < 0 { break; }
        io.send(link, RESPONSE);
        let used = end + 4;
        let rest = s.inbox.len - used;
        for k in rest { s.inbox[k] = s.inbox[used + k]; }
        s.inbox.pop_n(used);
    }
}

var address: u8[>..] = [];

thread_fn worker(index: i64) {
    var sessions = goose_io::LinkMap<Session> { .. };
    var io = iouring::Loop { .. };
    let listener, err = io.listen_shared(address[..]);
    if goose_io::failed(err) {
        var msg: u8[>..] = [];
        iouring::describe_listen_failure(address[..], err, msg);
        print(msg);
        iouring::report_failed_start();
        return;
    }
    io.attach(index);
    io.serve() { io, ev =>
        match ev {
            Opened o => { sessions.put(o.link, Session { .. }); },
            Data d => { on_data(io, sessions, d.link, d.bytes); },
            Closed c => { sessions.drop(c.link); },
            _ => {},
        }
    };
}

fn main() {
    address.append("0.0.0.0:");
    if arg_count() > 1 { arg(1, address); } else { address.append("8080"); }
    var workers = 1;
    if arg_count() > 2 {
        var text: u8[>..] = [];
        arg(2, text);
        workers = 0;
        for c in text { if c >= '0' && c <= '9' { workers = workers * 10 + (c - '0'); } }
    }
    let probed = iouring::probe(address[..]);
    if goose_io::failed(probed) {
        var msg: u8[>..] = [];
        iouring::describe_listen_failure(address[..], probed, msg);
        print(msg);
        exit(2);
    }
    iouring::spawn(workers, worker);
    iouring::wait_for_signal_then_drain(30000);
}
