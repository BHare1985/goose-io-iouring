#!/usr/bin/env python3
"""The example gateway on the epoll event loop, against real peers: the line server and the line client on one worker, tagged
differently, each handling only its own events. A request to the front is answered with the reply the client fetched from the
backend on the same worker."""
import socket, time
from netlib import *

front, back = free_port(), free_port()
p = start("gateway_server", f"127.0.0.1:{front}", f"127.0.0.1:{back}")
check(wait_listening(front) and wait_listening(back), "both servers listen")
time.sleep(0.3)

c = socket.create_connection(("127.0.0.1", front), 3)
c.sendall(b"".join(b"request %d\n" % i for i in range(50)))
got = read_until(c, b"backend: request 49\n", 8)
check(got == b"".join(b"backend: request %d\n" % i for i in range(50)), "every request is answered, in order, with the backend's reply")
c.close()

# the backend answers real peers too, and its own clients are not the front's
b = socket.create_connection(("127.0.0.1", back), 3)
b.sendall(b"direct\n")
check(read_until(b, b"\n") == b"backend: direct\n", "the backend is a line server of its own")
b.close()

code, out = stop(p)
check(code == 0, f"the gateway stops on SIGTERM (exit {code}) {out!r}")
finish()
