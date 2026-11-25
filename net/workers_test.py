#!/usr/bin/env python3
"""Share-nothing workers: four workers listen on one port and the kernel distributes four hundred connections, each accepted by
exactly one worker (the summed counters say 400). A port held by another process fails start-up with a message naming the
address, and the port is not shared with that process."""
import os, signal, socket, subprocess, time
from netlib import *

port = free_port()
p = start("echo_server", f"127.0.0.1:{port}", 4)
check(wait_listening(port), "four workers listen on one port")
time.sleep(0.3)
p.send_signal(signal.SIGUSR1)          # the counters before the test's own connections (wait_listening made one)
time.sleep(0.4)
conns = []
for i in range(400):
    c = socket.create_connection(("127.0.0.1", port), 3)
    c.sendall(b"hello %d\n" % i)
    conns.append(c)
ok = 0
for i, c in enumerate(conns):
    if read_until(c, b"\n", 3) == b"hello %d\n" % i: ok += 1
check(ok == 400, f"every one of 400 connections is answered (got {ok})")
p.send_signal(signal.SIGUSR1)
time.sleep(0.5)
for c in conns: c.close()
code, out = stop(p)
lines = [l for l in out.splitlines() if l.startswith("STATS")]
check(len(lines) == 2, f"the counters were printed on SIGUSR1: {out!r}")
if len(lines) == 2:
    before, after = (dict(kv.split("=") for kv in l.split()[1:]) for l in lines)
    got = int(after["accepted"]) - int(before["accepted"])
    check(got == 400, f"the accepted counts of the four workers sum to 400 (got {got})")
check(code == 0, f"the server stops on SIGTERM and exits 0 (exit {code})")

# a port held by another process
held = socket.socket(); held.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
held.bind(("127.0.0.1", 0)); held.listen(8)
hp = held.getsockname()[1]
p = start("echo_server", f"127.0.0.1:{hp}", 4)
try:
    out, _ = p.communicate(timeout=10)
except subprocess.TimeoutExpired:
    p.kill(); out, _ = p.communicate(); check(False, "start-up on a held port did not end")
check(p.returncode not in (0, None), f"start-up on a held port fails (exit {p.returncode})")
check(f"127.0.0.1:{hp}" in out and "Address already in use" in out, f"the message names the address and the system's error: {out!r}")
held.settimeout(1)
c = socket.create_connection(("127.0.0.1", hp), 3)
conn, _ = held.accept()
check(True, "the held port still belongs to the other process")
c.close(); conn.close(); held.close()
finish()
