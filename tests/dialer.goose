// Dials a number of addresses from one loop and reports what happened, for tests/net/dial_test.py. Usage:
//
//     dialer ADDRESS COUNT [name=value ...]        (limits as for echo_server)
//
// It dials ADDRESS COUNT times at once and prints one line for each event:
//
//     OPENED <n>    after which it sends "hi <n>\n"
//     DATA <n> <text>
//     CLOSED <n> <reason>      (reason: peer, reset, local, shutdown, error, idle, half, or failed:<kind>)
//
// then, when every link has ended or after 4 seconds, stops. A dial to a refused or unreachable address, past max_links, and a
// bad address each end in one CLOSED with `failed:` and the kind.
import std;
import os;
import goose_io from "github.com/BHare1985/goose-io@apply-name-resolution";
import iouring from "..";

struct Tag { n: i64 = 0 }

fn number(text: const u8[:]) -> i64 {
    var n = 0;
    for ch in text { if ch >= '0' && ch <= '9' { n = n * 10 + (ch - '0'); } }
    return n;
}

fn reason_name(why: goose_io::CloseReason) -> u8[>..] {
    var out: u8[>..] = [];
    match why {
        PeerClosed => { out.append("peer"); },
        PeerReset => { out.append("reset"); },
        ConnectFailed f => {
            out.append("failed:");
            match f.kind {
                Refused => { out.append("Refused"); },
                TimedOut => { out.append("TimedOut"); },
                Unreachable => { out.append("Unreachable"); },
                Denied => { out.append("Denied"); },
                NoResources => { out.append("NoResources"); },
                BadAddress => { out.append("BadAddress"); },
                Other => { out.append("Other"); },
            }
        },
        IdleTimeout => { out.append("idle"); },
        HalfCloseTimeout => { out.append("half"); },
        Local => { out.append("local"); },
        Shutdown => { out.append("shutdown"); },
        Error => { out.append("error"); },
        LoopSpecific p => { out.append("loop-specific"); },
    }
    return out;
}

fn main() {
    var address: u8[>..] = [];
    arg(1, address);
    var t: u8[>..] = [];
    arg(2, t);
    let count = number(t[..]);
    var io = iouring::Loop { .. };
    for k in 3..arg_count() {
        var s: u8[>..] = [];
        arg(k, s);
        let eq = find(s[..], "=");
        if eq < 0 { continue; }
        let name = s[..eq];
        let v = number(s[eq + 1..]);
        if name == "max_links" { io.limits.max_links = v; }
        else if name == "connect_ms" { io.limits.connect_timeout_ms = v; }
    }
    var links = goose_io::LinkMap<Tag> { .. };
    var open = 0;
    for n in count {
        let l = io.dial(address[..], 0);
        links.put(l, Tag { n: n });
        open++;
    }
    var started: i64 = -1;
    io.serve() { io, ev =>
        match ev {
            Opened o => {
                let n = links.find(o.link);
                if n != null {
                    print("OPENED ", n.n);
                    var msg: u8[>..] = [];
                    format(msg, "hi ", n.n, "\n");
                    io.send(o.link, msg[..]);
                }
            },
            Data d => {
                let n = links.find(d.link);
                if n != null { print("DATA ", n.n, " ", d.bytes); }
            },
            Closed c => {
                let n = links.find(c.link);
                if n != null {
                    print("CLOSED ", n.n, " ", reason_name(c.why));
                    open--;
                    if open == 0 && !io.draining() { io.stop(100); }
                }
            },
            Tick t => {
                if started < 0 { started = t.now_ms; }
                if t.now_ms - started > 4000 && !io.draining() { io.stop(100); }
            },
            _ => {},
        }
    };
}
