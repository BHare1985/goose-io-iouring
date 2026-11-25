#!/usr/bin/env python3
"""Peers and the kernel cannot crash a worker: thousands of connections that reset, send partial data and vanish, or half-close at
arbitrary points, against an echo server that keeps links open on the peer's end of stream. The server must stay up and answer a
well-behaved peer throughout, every link must end exactly once (open and the sums of the closed counters agree with accepted), no event
may be counted bad, and memory must not grow past the first round."""
import random, signal, socket, struct, threading, time
from netlib import *

port = free_port()
p = start("echo_server", f"127.0.0.1:{port}", 2, "keep=1", "half_ms=1000")
check(wait_listening(port), "the server listens")
rnd = random.Random(7)

def abort(s):
    s.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0)); s.close()

def hostile(i):
    try:
        s = socket.create_connection(("127.0.0.1", port), 3)
        kind = rnd.randrange(6)
        if kind == 0: abort(s)
        elif kind == 1: s.sendall(b"partial"); abort(s)
        elif kind == 2: s.sendall(b"x" * rnd.randrange(1, 70000)); s.close()
        elif kind == 3: s.sendall(b"half"); s.shutdown(socket.SHUT_WR); time.sleep(0.01); s.close()
        elif kind == 4: s.shutdown(socket.SHUT_RDWR); s.close()
        else: s.sendall(b"y" * 5000); s.shutdown(socket.SHUT_WR); abort(s)
    except OSError:
        pass

def rounds(n, base):
    threads = []
    for i in range(n):
        t = threading.Thread(target=hostile, args=(base + i,)); t.start(); threads.append(t)
        if len(threads) >= 40:
            for t in threads: t.join()
            threads = []
    for t in threads: t.join()

rounds(1500, 0)
good = socket.create_connection(("127.0.0.1", port), 3)
good.sendall(b"still here\n")
check(read_until(good, b"\n") == b"still here\n", "a well-behaved peer is answered after the first round of hostile ones")
good.close()
# A second warm-up round: on the io_uring loop the high-water mark of buffers and slots (roughly 10 MB) is reached in the second round, not the first;
# after it memory is flat (the third round of 3000 grows by about 0.5 MB).
rounds(3000, 5000)
time.sleep(1.5)
before = rss_kb(p.pid)
rounds(3000, 10000)
good = socket.create_connection(("127.0.0.1", port), 3)
good.sendall(b"again\n")
check(read_until(good, b"\n") == b"again\n", "and after three thousand more")
good.close()
time.sleep(1.5)                      # links left half-open end at the half-close limit (1 s)
check(p.poll() is None, "the server is still running")
growth = rss_kb(p.pid) - before
check(growth < 4096, f"memory does not grow with the number of hostile peers (grew {growth} kB)")
p.send_signal(signal.SIGUSR1)
time.sleep(0.5)
code, out = stop(p)
line = [l for l in out.splitlines() if l.startswith("STATS")]
check(bool(line), f"the counters were printed: {out!r}")
if line:
    st = {k: int(v) for k, v in (kv.split("=") for kv in line[0].split()[1:])}
    check(st["open"] == 0, f"no link is left open (open={st['open']})")
    ended = st["closed_peer"] + st["closed_reset"] + st["closed_idle"] + st["closed_half"] + st["closed_local"]
    check(ended == st["accepted"], f"every accepted link ended exactly once (accepted {st['accepted']}, ended {ended}: {st})")
check(code == 0, f"the server stops on SIGTERM (exit {code})")
finish()
