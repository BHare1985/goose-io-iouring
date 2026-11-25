#!/usr/bin/env python3
"""Closing: a close with unread input does not reset the peer (it would lose bytes it was sent); an unfinished reply is sent in full
before a close; and a peer that never reads is released at the loop's limit."""
import socket, time
from netlib import *

port = free_port()
p = start("lineserver_server", f"127.0.0.1:{port}")
check(wait_listening(port), "the server listens")

# a line longer than the server allows closes the connection; what the peer sent is more than the server read, so the close
# happens with unread input. The peer must see an end of stream, not a reset
c = socket.create_connection(("127.0.0.1", port), 3)
c.sendall(b"x" * 60000)
c.settimeout(5)
try:
    while True:
        d = c.recv(65536)
        if not d: end = "eof"; break
except ConnectionResetError:
    end = "reset"
except OSError as e:
    end = f"error {e}"
check(end == "eof", f"a close with unread input ends the peer's stream cleanly, not with a reset (saw {end})")
c.close()

# 20000 bytes of reply, then the peer closes right away: nothing was lost on the way out
c = socket.create_connection(("127.0.0.1", port), 3)
c.sendall(b"BIG\n")
got = read_until(c, b"\n", 5)
check(len(got) == 20001, f"a reply is sent in full ({len(got)} bytes)")
c.close()

# a peer that never reads: it sends requests for big replies, half-closes (so the server closes after answering) and never reads. The
# server's closes cannot finish sending; the link ends at the loop's half-close limit, and the server stays well
code, out = stop(p)
port = free_port()
p = start("lineserver_server", f"127.0.0.1:{port}", 1200)
check(wait_listening(port), "a server with a half-close limit of 1.2 s listens")
c = socket.socket()
c.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
c.connect(("127.0.0.1", port))
c.sendall(b"BIG\n" * 40)
c.shutdown(socket.SHUT_WR)
time.sleep(0.5)
x = socket.create_connection(("127.0.0.1", port), 3)
x.sendall(b"alive\n")
check(read_until(x, b"\n") == b"echo alive\n", "another peer is answered while one never reads")
x.close()
time.sleep(2.0)
# after the limit the server has given up on the link: reading now gets what the kernel had and then the end of the stream or a reset
c.settimeout(5)
total = 0
ended = False
try:
    while True:
        d = c.recv(65536)
        if not d: ended = True; break
        total += len(d)
except OSError:
    ended = True
check(ended, "the link of a peer that never reads is released at the loop's limit")
c.close()
code, out = stop(p)
check(code == 0, f"the server stops (exit {code})")
finish()
