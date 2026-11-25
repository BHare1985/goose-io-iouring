#!/usr/bin/env python3
"""The example line server on the epoll event loop, against real peers: split requests, pipelined requests, a peer that
half-closes, a peer that never reads, and a clean stop. The same scenarios run on the in-memory loop in tests/examples."""
import socket, sys, time
from netlib import *

port = free_port()
p = start("lineserver_server", f"127.0.0.1:{port}")
check(wait_listening(port), "the server listens")

def conn():
    return socket.create_connection(("127.0.0.1", port), 3)

# a request split over three sends, with pauses
c = conn()
for piece in (b"hel", b"lo wor", b"ld\n"):
    c.sendall(piece); time.sleep(0.05)
check(read_until(c, b"\n") == b"echo hello world\n", "a request split over three sends is answered once")
c.close()

# pipelined requests are answered in order
c = conn()
c.sendall(b"".join(b"line %d\n" % i for i in range(200)))
got = read_until(c, b"echo line 199\n", 8)
check(got == b"".join(b"echo line %d\n" % i for i in range(200)), "200 pipelined requests are answered in order")
c.close()

# a peer that sends its requests and half-closes still gets its answers (the server keeps the link open to answer)
c = conn()
c.sendall(b"one\ntwo\nthree\n")
c.shutdown(socket.SHUT_WR)
got, eof = read_all(c, 5)
check(got == b"echo one\necho two\necho three\n", "a peer that half-closes after its requests gets every answer")
check(eof, "and then the server ends the connection")
c.close()

# a peer that never reads does not block the others and does not make the server grow
slow = conn()
slow.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
slow.sendall(b"BIG\n" * 50)         # 50 replies of 20000 bytes, never read
time.sleep(0.5)
rss_before = rss_kb(p.pid)
other = conn()
other.sendall(b"still alive\n")
check(read_until(other, b"\n") == b"echo still alive\n", "another peer is answered while one never reads")
other.close()
for k in range(5):
    x = conn(); x.sendall(b"BIG\n" * 50); time.sleep(0.1); x.close()
time.sleep(0.5)
check(rss_kb(p.pid) - rss_before < 4096, f"memory stays bounded against peers that never read (grew {rss_kb(p.pid) - rss_before} kB)")
slow.close()

# a line that is too long closes the connection
c = conn()
c.sendall(b"x" * 5000)
got, eof = read_all(c, 3)
check(eof, "an over-long line closes the connection")
c.close()

code, out = stop(p)
check(code == 0, f"the server stops on SIGTERM and exits 0 (exit {code}) {out!r}")
finish()
