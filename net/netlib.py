"""Helpers for the network tests: free ports, waiting for a listener, running a built program, reading its output."""
import os, signal, socket, subprocess, sys, time

FAILS = 0
CHECKS = 0


def check(ok, what):
    global CHECKS, FAILS
    CHECKS += 1
    if not ok:
        FAILS += 1
        print("FAIL:", what, flush=True)


def finish():
    print(f"{CHECKS} checks, {FAILS} failed")
    sys.exit(1 if FAILS else 0)


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def wait_listening(port, seconds=5.0):
    end = time.time() + seconds
    while time.time() < end:
        try:
            socket.create_connection(("127.0.0.1", port), 0.2).close()
            return True
        except OSError:
            time.sleep(0.02)
    return False


def program(name):
    """The path of a built program (tests/net/run.sh builds them into $NET_BIN)."""
    path = os.path.join(os.environ["NET_BIN"], name)
    if not os.path.exists(path):
        print(f"missing program {path}")
        sys.exit(2)
    return path


def start(name, *args):
    p = subprocess.Popen([program(name), *map(str, args)], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    return p


def stop(p, timeout=15):
    """SIGTERM, then wait; returns the exit code and what it printed."""
    if p.poll() is None:
        p.send_signal(signal.SIGTERM)
    try:
        out, _ = p.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        p.kill()
        out, _ = p.communicate()
        check(False, "the program did not stop after SIGTERM")
    return p.returncode, out


def rss_kb(pid):
    with open(f"/proc/{pid}/status") as f:
        for line in f:
            if line.startswith("VmRSS:"):
                return int(line.split()[1])
    return 0


def read_until(sock, marker, seconds=5.0):
    sock.settimeout(0.2)
    buf = b""
    end = time.time() + seconds
    while time.time() < end and marker not in buf:
        try:
            d = sock.recv(65536)
        except socket.timeout:
            continue
        except OSError:
            break
        if not d:
            break
        buf += d
    return buf


def read_all(sock, seconds=5.0):
    sock.settimeout(0.2)
    buf = b""
    end = time.time() + seconds
    while time.time() < end:
        try:
            d = sock.recv(65536)
        except socket.timeout:
            continue
        except OSError:
            break
        if not d:
            return buf, True
        buf += d
    return buf, False
