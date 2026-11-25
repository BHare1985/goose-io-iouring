#!/usr/bin/env python3
"""http_hello.goose on the epoll event loop, behaving as tcprouter's http_hello does (the checks of its
examples_test.py): every request is answered with the same response, a request may arrive in pieces, requests may be pipelined, a
request head over 2 KB drops the connection, and a load of keep-alive requests from 100 clients gets no error."""
import re, shutil, socket, subprocess, threading, time
from netlib import *

RESPONSE = b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\nContent-Type: text/plain\r\n\r\nhello\n"
port = free_port()
p = start("http_hello", port)
check(wait_listening(port), "http_hello listens")

c = socket.create_connection(("127.0.0.1", port), 3)
c.sendall(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n")
check(read_until(c, RESPONSE[-6:]) == RESPONSE, "a request is answered with the response of tcprouter's http_hello, byte for byte")

# a request head split at every byte, with pauses
for cut in (1, 7, 20, 30):
    req = b"GET /a HTTP/1.1\r\nHost: x\r\n\r\n"
    c.sendall(req[:cut]); time.sleep(0.05); c.sendall(req[cut:])
    check(read_until(c, RESPONSE[-6:]) == RESPONSE, f"a request split after {cut} bytes is answered once")

# pipelined: ten requests in one send, answered in order, nothing more
c.sendall(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n" * 10)
got, _ = read_all(c, 1.0)
check(got == RESPONSE * 10, f"ten pipelined requests get ten responses ({len(got)} bytes)")
c.close()

# a head over 2 KB drops the connection
c = socket.create_connection(("127.0.0.1", port), 3)
c.sendall(b"GET / HTTP/1.1\r\nX: " + b"a" * 3000)
got, eof = read_all(c, 2.0)
check(eof, "a request head over 2 KB makes the server drop that connection")
c.close()

# 100 clients, keep-alive, 100,000 requests in all
errors = []
def client(n):
    try:
        s = socket.create_connection(("127.0.0.1", port), 10)
        req = b"GET / HTTP/1.1\r\nHost: x\r\n\r\n" * 10
        for _ in range(n // 10):
            s.sendall(req)
            got = b""
            while len(got) < len(RESPONSE) * 10:
                d = s.recv(65536)
                if not d: errors.append("eof"); return
                got += d
            if got != RESPONSE * 10: errors.append("bad"); return
        s.close()
    except OSError as e:
        errors.append(str(e))
threads = [threading.Thread(target=client, args=(1000,)) for _ in range(100)]
t0 = time.time()
for t in threads: t.start()
for t in threads: t.join()
check(not errors, f"100,000 keep-alive requests from 100 clients get no error ({errors[:2]}, {time.time() - t0:.1f} s)")
if shutil.which("wrk"):
    r = subprocess.run(["wrk", "-t2", "-c100", "-d3s", f"http://127.0.0.1:{port}/"], capture_output=True, text=True)
    check("Socket errors" not in r.stdout and "Non-2xx" not in r.stdout and "requests in" in r.stdout, f"wrk sees no socket error and no non-2xx: {r.stdout.strip().splitlines()[-3:]}")
check(p.poll() is None, "http_hello is still running after the load")
code, out = stop(p)
check(code == 0 and out.strip() == "", f"it stops on SIGTERM, exits 0 and writes nothing (exit {code}, {out!r})")
finish()
