// The example line client on the io_uring event loop, for tests/net/lineclient_test.py: it dials a pool of two links to the address
// given as the first argument, makes one request every 40 ms ("req <n>", 500 ms to be answered) for the number of milliseconds
// given as the second, and prints one line for each result:
//
//     RESULT <id> <ok|timeout|lost|failed> <reply>
//
// and at the end `DIALS <n>`. It needs only the core.
import std;
import os;
import goose_io from "github.com/BHare1985/goose-io@apply-name-resolution";
import goose_io.examples.lineclient.client;
import iouring from "..";

fn drain_results(c: lineclient::LineClient&) {
    loop {
        var text: u8[>..] = [];
        let r = c.take_result(text);
        if !r.found { break; }
        var name: u8[>..] = [];
        match r.outcome {
            Ok => { name.append("ok"); },
            TimedOut => { name.append("timeout"); },
            LinkLost => { name.append("lost"); },
            Failed => { name.append("failed"); },
        }
        print("RESULT ", r.id, " ", name, " ", text);
    }
}

fn number(k: i64, default: i64) -> i64 {
    if arg_count() <= k { return default; }
    var text: u8[>..] = [];
    arg(k, text);
    var n = 0;
    for ch in text { if ch >= '0' && ch <= '9' { n = n * 10 + (ch - '0'); } }
    return n;
}

fn main() {
    var address: u8[>..] = [];
    if arg_count() > 1 { arg(1, address); } else { address.append("127.0.0.1:7000"); }
    let run_ms = number(2, 3000);
    var io = iouring::Loop { .. };
    goose_io::require_core(io);
    var client = lineclient::LineClient { .. };
    if !client.start(io, address[..], 2, 100) { print("bad address"); exit(2); }
    var next_request: i64 = 0;
    var started:      i64 = -1;
    var n = 0;
    io.serve() { io, ev =>
        client.client_event(io, ev);
        match ev {
            Tick t => {
                if started < 0 { started = t.now_ms; next_request = t.now_ms; }
                if !io.draining() && t.now_ms >= next_request {
                    next_request = t.now_ms + 40;
                    n++;
                    var text: u8[>..] = [];
                    format(text, "req ", n);
                    client.request(io, text[..], 500);
                }
                if !io.draining() && t.now_ms - started >= run_ms { io.stop(2000); }
            },
            _ => {},
        }
        drain_results(client);
    };
    drain_results(client);
    print("DIALS ", client.dial_count());
}
