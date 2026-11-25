#!/usr/bin/env python3
"""Limits and exhaustion: a loop out of descriptors rests its listener and does not spin, and serves the connections waiting in the
kernel's queue once descriptors come back; a loop at its link limit ends a dial with ConnectFailed NoResources and leaves its
other links alone; a pool of six buffers is shared by sixteen links without losing a byte; the idle and half-close limits end a
link between the limit and the limit plus 250 ms."""
import os, resource, signal, socket, subprocess, sys, threading, time
from netlib import *

def cpu_seconds(pid):
    with open(f"/proc/{pid}/stat") as f:
        parts = f.read().rsplit(")", 1)[1].split()
    return (int(parts[11]) + int(parts[12])) / os.sysconf("SC_CLK_TCK")

# --- descriptor exhaustion -------------------------------------------------------------------------------------------------
port = free_port()
def limit_files():
    resource.setrlimit(resource.RLIMIT_NOFILE, (40, 40))
p = subprocess.Popen([program("echo_server"), f"127.0.0.1:{port}", "1"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, preexec_fn=limit_files)
check(wait_listening(port), "the server listens")
held = []
for i in range(120):                       # far more than its descriptors: the rest wait in the kernel's queue
    try:
        held.append(socket.create_connection(("127.0.0.1", port), 3))
    except OSError:
        break
time.sleep(0.5)
c0 = cpu_seconds(p.pid); t0 = time.time()
time.sleep(2.0)
used = (cpu_seconds(p.pid) - c0) / (time.time() - t0)
check(used < 0.25, f"a loop out of descriptors does not spin (CPU {used:.0%} of one core over two seconds)")
# the first connections are served; close some, and the waiting ones are served
served = 0
for c in held[:20]:
    c.sendall(b"x\n")
    if read_until(c, b"\n", 1) == b"x\n": served += 1
check(served >= 10, f"connections accepted before descriptors ran out are served ({served})")
for c in held[:30]: c.close()
time.sleep(0.8)
late = 0
for c in held[30:50]:
    try:
        c.sendall(b"late\n")
        if read_until(c, b"\n", 2) == b"late\n": late += 1
    except OSError:
        pass
check(late >= 10, f"connections that waited in the queue are served once descriptors return ({late} of 20)")
for c in held: c.close()
code, out = stop(p)
check(code == 0, f"the server stops (exit {code})")

# --- the link limit -------------------------------------------------------------------------------------------------------
port = free_port()
srv = socket.socket(); srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1); srv.bind(("127.0.0.1", port)); srv.listen(32)
def accept_forever():
    while True:
        try: c, _ = srv.accept()
        except OSError: return
        threading.Thread(target=lambda c=c: [c.sendall(b"re:" + d) for d in iter(lambda: c.recv(4096), b"")], daemon=True).start()
threading.Thread(target=accept_forever, daemon=True).start()
r = subprocess.run([program("dialer"), f"127.0.0.1:{port}", "6", "max_links=4"], capture_output=True, text=True, timeout=20)
lines = r.stdout.splitlines()
opened = [l for l in lines if l.startswith("OPENED")]
failed = [l for l in lines if "failed:NoResources" in l]
check(len(opened) == 4 and len(failed) == 2, f"at max_links 4 four dials open and two end ConnectFailed NoResources (opened {len(opened)}, failed {len(failed)}): {lines}")
check(sum(1 for l in lines if l.startswith("DATA")) == 4, "the links that opened are unaffected: each is answered")
r = subprocess.run([program("dialer"), "not-an-address", "1"], capture_output=True, text=True, timeout=20)
check("CLOSED 0 failed:BadAddress" in r.stdout, f"a bad address ends in ConnectFailed BadAddress: {r.stdout!r}")
r = subprocess.run([program("dialer"), f"127.0.0.1:{free_port()}", "1"], capture_output=True, text=True, timeout=20)
check("CLOSED 0 failed:Refused" in r.stdout, f"a port nobody listens on ends in ConnectFailed Refused: {r.stdout!r}")
srv.close()

# --- pool exhaustion ------------------------------------------------------------------------------------------------------
port = free_port()
p = start("echo_server", f"127.0.0.1:{port}", 1, "buffers=6", "buffer_size=1024")
check(wait_listening(port), "a server with a pool of six buffers listens")
errors = []
release = threading.Event()
def client(i):
    # every client sends and does not read until all have: the loop's replies queue up and the six buffers run out
    try:
        c = socket.socket()
        c.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)     # a small window, so the server's kernel buffer fills and the loop's queue is used
        c.settimeout(20)
        c.connect(("127.0.0.1", port))
        data = bytes((i * 7 + k) % 251 for k in range(6000000))
        sender = threading.Thread(target=lambda: c.sendall(data)); sender.start()
        release.wait()
        got = b""
        c.settimeout(20)
        while len(got) < len(data):
            d = c.recv(65536)
            if not d: break
            got += d
        sender.join()
        if got != data: errors.append(i)
        c.close()
    except OSError as e:
        errors.append((i, str(e)))
threads = [threading.Thread(target=client, args=(i,)) for i in range(16)]
for t in threads: t.start()
time.sleep(3)
release.set()
for t in threads: t.join()
check(not errors, f"sixteen links share six buffers and every byte comes back intact (bad: {errors[:3]})")
p.send_signal(signal.SIGUSR1); time.sleep(0.5)
code, out = stop(p)
line = [l for l in out.splitlines() if l.startswith("STATS")]
waits = int(dict(kv.split("=") for kv in line[0].split()[1:])["buffer_waits"]) if line else -1
check(waits > 0, f"the waits for a buffer were counted ({waits})")
check(line and int(dict(kv.split("=") for kv in line[0].split()[1:])["buffers_total"]) <= 6, "the pool never grew past its limit")

# --- timeouts -------------------------------------------------------------------------------------------------------------
port = free_port()
p = start("echo_server", f"127.0.0.1:{port}", 1, "idle_ms=1000", "half_ms=1500", "keep=1")
check(wait_listening(port), "a server with short limits listens")
c = socket.create_connection(("127.0.0.1", port), 3)
t0 = time.time()
c.settimeout(5)
try: d = c.recv(10)
except OSError: d = b""
idle = time.time() - t0
check(d == b"" and 1.0 <= idle <= 1.35, f"an idle link ends between the limit and the limit plus 250 ms ({idle:.3f} s)")
c.close()
# activity resets the idle timer: a byte at 0.6 s moves the end from 1.0 s to about 1.6 s
c = socket.create_connection(("127.0.0.1", port), 3)
t0 = time.time()
time.sleep(0.6)
c.sendall(b"ping\n")
check(read_until(c, b"\n", 2) == b"ping\n", "the link answers before its idle limit")
c.settimeout(5)
try: d = c.recv(10)
except OSError: d = b"error"
total = time.time() - t0
check(d == b"" and 1.6 <= total <= 1.95, f"activity resets the idle timer: the link ends between 1.6 s and 1.85 s after it was opened, not at 1.0 s ({total:.3f} s)")
c.close()
c = socket.create_connection(("127.0.0.1", port), 3)
c.sendall(b"abc"); c.shutdown(socket.SHUT_WR)
t0 = time.time()
got = b""
c.settimeout(5)
try:
    while True:
        d = c.recv(100)
        if not d: break
        got += d
except OSError: pass
half = time.time() - t0
check(got == b"abc" and 1.5 <= half <= 1.85, f"a half-closed link ends between the limit and the limit plus 250 ms ({half:.3f} s)")
c.close()
stop(p)
finish()
