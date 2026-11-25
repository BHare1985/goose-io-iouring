#!/usr/bin/env python3
"""Draining: SIGTERM stops the listeners at once, links that finish by themselves end cleanly, and links still open at the end of the
grace period end with Shutdown (the process exits after the grace period, not before and not much after)."""
import signal, socket, time
from netlib import *

port = free_port()
p = start("echo_server", f"127.0.0.1:{port}", 1)
check(wait_listening(port), "the server listens")
c = socket.create_connection(("127.0.0.1", port), 3)
c.sendall(b"hello\n")
check(read_until(c, b"\n") == b"hello\n", "a link is open and answered")
t0 = time.time()
p.send_signal(signal.SIGTERM)
time.sleep(0.5)
try:
    socket.create_connection(("127.0.0.1", port), 1).close()
    refused = False
except OSError:
    refused = True
check(refused, "after SIGTERM the listener no longer accepts")
# the echo library closes what it has no work on while the loop is stopping, so the link ends by itself and the process need not
# wait for the grace period
got, eof = read_all(c, 3)
check(eof, "the library ends its link while draining")
c.close()
try:
    out, _ = p.communicate(timeout=15)
except Exception:
    p.kill(); out, _ = p.communicate(); check(False, "the process did not exit")
took = time.time() - t0
check(p.returncode == 0, f"the process exits 0 (exit {p.returncode})")
check(took < 3.0, f"it exits when the last link has ended, not at the end of the grace period ({took:.1f} s)")

# a link that nobody closes is ended at the grace period (2 s here)
port = free_port()
p = start("idle_server", port, 2000)
check(wait_listening(port), "the server listens again")
c = socket.create_connection(("127.0.0.1", port), 3)
time.sleep(0.3)
t0 = time.time()
p.send_signal(signal.SIGTERM)
c.settimeout(20)
try:
    end = c.recv(10)
except OSError as e:
    end = b"error"
took = time.time() - t0
check(end == b"" and 1.9 <= took <= 3.0, f"a link still open is ended at the end of the grace period ({took:.1f} s, got {end!r})")
code, out = stop(p)
check(code == 0, f"the process exits 0 after the grace period (exit {code})")
finish()
