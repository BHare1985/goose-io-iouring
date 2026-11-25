#!/usr/bin/env python3
"""The example line client on the epoll event loop, against a real line-echo service that is restarted mid-run: requests are
answered while the service is up, fail or time out while it is down, and the client dials again after its backoff and is
answered again. The same scenarios run on the in-memory loop in tests/examples."""
import socket, subprocess, threading, time
from netlib import *

port = free_port()

class Service:
    """A line-echo service: answers each line with 'pong <line>'."""
    def __init__(self):
        self.conns = []; self.sock = None; self.stop_flag = False
    def up(self):
        s = socket.socket(); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("127.0.0.1", port)); s.listen(16); s.settimeout(0.1)
        self.sock = s; self.stop_flag = False
        threading.Thread(target=self.accept_loop, daemon=True).start()
    def accept_loop(self):
        while not self.stop_flag:
            try:
                c, _ = self.sock.accept()
            except OSError:
                continue
            self.conns.append(c)
            threading.Thread(target=self.serve, args=(c,), daemon=True).start()
    def serve(self, c):
        buf = b""
        try:
            while True:
                d = c.recv(4096)
                if not d: return
                buf += d
                while b"\n" in buf:
                    line, buf = buf.split(b"\n", 1)
                    c.sendall(b"pong " + line + b"\n")
        except OSError:
            return
    def down(self):
        self.stop_flag = True
        self.sock.close()
        for c in self.conns:
            try: c.shutdown(socket.SHUT_RDWR)
            except OSError: pass
            c.close()
        self.conns = []

svc = Service(); svc.up()
p = subprocess.Popen([program("lineclient_runner"), f"127.0.0.1:{port}", "3600"], stdout=subprocess.PIPE, text=True)
time.sleep(1.0)
svc.down()                       # the service goes away: its links reset or end
time.sleep(1.0)
svc.up()                         # and comes back: the client dials again after its backoff
out, _ = p.communicate(timeout=20)
rows = [l.split(" ", 3) for l in out.splitlines() if l.startswith("RESULT")]
ok = [r for r in rows if r[2] == "ok"]
bad = [r for r in rows if r[2] != "ok"]
check(p.returncode == 0, f"the client exits cleanly (exit {p.returncode})")
check(len(ok) > 10, f"requests were answered while the service was up (got {len(ok)})")
check(all(r[3].startswith("pong req ") for r in ok), "every reply is the reply to a request")
ids = [int(r[1]) for r in rows]
check(len(set(ids)) == len(ids), "every request is reported once")
check(len(bad) > 5 and all(r[2] in ("timeout", "lost", "failed") for r in bad), f"requests made while the service was down fail or time out (got {len(bad)})")
last_ok = max((int(r[1]) for r in ok), default=0)
first_bad = min((int(r[1]) for r in bad), default=0)
check(last_ok > max((int(r[1]) for r in bad), default=0), "after the service came back the client was answered again")
dials = [int(l.split()[1]) for l in out.splitlines() if l.startswith("DIALS")]
check(dials and dials[0] > 2, f"the client dialled again after its backoff (dials: {dials})")
finish()
