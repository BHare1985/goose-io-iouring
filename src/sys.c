/* The C layer of the io_uring event loop: only what Goose cannot bind straight to libc. It is tcprouter's C layer
 * (src/c_sys.c), with the peeking and receive-low-water calls of the proxy removed and the calls the contract needs
 * added: the local port of a listener, the peer's address, and a reset that does not close.
 *
 * Why C at all (the rules are in goose-kb/GOOSE_PACKAGES_AND_C.md):
 *   - struct epoll_event is packed on x86-64 and NOT on arm64, so Goose (whose structs are always packed)
 *     fills the same 12-byte record on both and this file copies to and from the real type;
 *   - errno is thread-local and libc returns -1: Goose wants the error as a negative number, so every call
 *     here returns -errno on failure;
 *   - sockaddr is built here from (family, ip bytes, port), so no layout is shared with Goose;
 *   - signalfd and sigprocmask take opaque sigset_t values;
 *   - CLOCK_BOOTTIME and struct timespec are constants and a struct, not plain integers.
 * Everything here is a thin, thread-safe wrapper over one syscall. Nothing keeps a pointer.
 * Descriptors are non-blocking and close-on-exec from birth (SOCK_NONBLOCK, SOCK_CLOEXEC).
 */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <poll.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/signalfd.h>
#include <sys/socket.h>

#include "goose.h"

/* Must match `c_RingEvent` in sys.goose: { events: u32, data: u64 }, packed. */
typedef struct __attribute__((packed)) { uint32_t events; uint64_t data; } c_ring_event;
_Static_assert(sizeof(c_ring_event) == 12, "c_ring_event must be 12 bytes, as Goose lays out RingEvent");

#define C_MAX_EVENTS 256

/* --- the ring ----------------------------------------------------------------------------------------
 * The loop asks for readiness, as goose-io-epoll does, but the kernel is asked through io_uring: every descriptor the
 * loop watches has one single-shot IORING_OP_POLL_ADD in flight while its interest is not empty. A completion is one
 * event and disarms that poll; the next wait arms it again, so a descriptor that is still ready is reported again,
 * the level-triggered behaviour the engine is written for. Arming, re-arming, removing and the wait are submitted
 * together, in one io_uring_enter. The "ring descriptor" the engine holds is the ring's own fd; per-ring state is
 * found by it. A loop is one thread's, so c_close finds its ring through a thread-local pointer. */
#include <liburing.h>
#include <stdlib.h>

#define C_RING_ENTRIES 4096
#define C_REMOVED ((uint64_t)0xFFFFFFFFFFFFFFFFull)

typedef struct { uint32_t events; uint64_t data; uint32_t gen; uint8_t active, armed, dirty; } c_watch;
typedef struct {
    struct io_uring ring;
    c_watch *w; int32_t cap;             /* indexed by fd */
    int32_t *dirty; int32_t ndirty, dirty_cap;
} c_ring;

static c_ring *c_rings[65536];
static __thread c_ring *c_current;

static c_ring *ring_of(int32_t ep) { return (ep >= 0 && ep < 65536) ? c_rings[ep] : NULL; }

static void mark_dirty(c_ring *r, int32_t fd) {
    c_watch *w = &r->w[fd];
    if (w->dirty) return;
    if (r->ndirty == r->dirty_cap) {
        r->dirty_cap = r->dirty_cap ? r->dirty_cap * 2 : 256;
        r->dirty = realloc(r->dirty, sizeof(int32_t) * (size_t)r->dirty_cap);
    }
    r->dirty[r->ndirty++] = fd;
    w->dirty = 1;
}

static struct io_uring_sqe *sqe_get(c_ring *r) {
    struct io_uring_sqe *s = io_uring_get_sqe(&r->ring);
    if (!s) { io_uring_submit(&r->ring); s = io_uring_get_sqe(&r->ring); }
    return s;
}

static void unarm(c_ring *r, int32_t fd) {      /* cancels the poll in flight for fd, if any */
    c_watch *w = &r->w[fd];
    if (!w->armed) return;
    struct io_uring_sqe *s = sqe_get(r);
    if (s) { io_uring_prep_poll_remove(s, ((uint64_t)w->gen << 32) | (uint32_t)fd); io_uring_sqe_set_data64(s, C_REMOVED); }
    w->armed = 0;
}

int32_t c_ring_create(void) {
    c_ring *r = calloc(1, sizeof *r);
    if (!r) return -ENOMEM;
    int rc = io_uring_queue_init(C_RING_ENTRIES, &r->ring, 0);
    if (rc < 0) { free(r); return rc; }
    int fd = r->ring.ring_fd;
    if (fd >= 65536) { io_uring_queue_exit(&r->ring); free(r); return -EMFILE; }
    c_rings[fd] = r;
    c_current = r;
    return fd;
}

/* The engine's c_ring_ctl calls, with the values of epoll for op (1 add, 2 delete, 3 modify) and for the event bits. */
int32_t c_ring_ctl(int32_t ep, int32_t op, int32_t fd, uint32_t events, uint64_t data) {
    c_ring *r = ring_of(ep);
    if (!r || fd < 0) return -EBADF;
    if (fd >= r->cap) {
        int32_t cap = r->cap ? r->cap : 1024;
        while (cap <= fd) cap *= 2;
        r->w = realloc(r->w, sizeof(c_watch) * (size_t)cap);
        memset(r->w + r->cap, 0, sizeof(c_watch) * (size_t)(cap - r->cap));
        r->cap = cap;
    }
    c_watch *w = &r->w[fd];
    if (op == 1) {
        if (w->active) return -EEXIST;
        w->active = 1; w->gen++; w->armed = 0;
    } else {
        if (!w->active) return -ENOENT;
        unarm(r, fd);
        if (op == 2) { w->active = 0; w->gen++; return 0; }
    }
    w->events = events; w->data = data;
    mark_dirty(r, fd);
    return 0;
}

static void arm_dirty(c_ring *r) {
    for (int32_t i = 0; i < r->ndirty; i++) {
        int32_t fd = r->dirty[i];
        c_watch *w = &r->w[fd];
        w->dirty = 0;
        if (!w->active || w->armed || w->events == 0) continue;
        struct io_uring_sqe *s = sqe_get(r);
        if (!s) continue;
        io_uring_prep_poll_add(s, fd, w->events | POLLERR | POLLHUP);
        io_uring_sqe_set_data64(s, ((uint64_t)w->gen << 32) | (uint32_t)fd);
        w->armed = 1;
    }
    r->ndirty = 0;
}

/* Returns the number of events written to `out` (at most `max` and C_MAX_EVENTS), 0 when interrupted, or -errno. */
int32_t c_ring_wait(int32_t ep, c_ring_event *out, int32_t max, int32_t timeout_ms) {
    c_ring *r = ring_of(ep);
    if (!r) return -EBADF;
    if (max > C_MAX_EVENTS) max = C_MAX_EVENTS;
    if (max < 1) return -EINVAL;
    arm_dirty(r);
    struct io_uring_cqe *cqe;
    if (timeout_ms == 0) {
        io_uring_submit(&r->ring);
    } else if (io_uring_cq_ready(&r->ring) == 0) {
        struct __kernel_timespec ts = { timeout_ms / 1000, (long)(timeout_ms % 1000) * 1000000L };
        int rc = io_uring_submit_and_wait_timeout(&r->ring, &cqe, 1, timeout_ms < 0 ? NULL : &ts, NULL);
        if (rc < 0 && rc != -ETIME && rc != -EINTR && rc != -EAGAIN && rc != -EBUSY) return rc;
    } else {
        io_uring_submit(&r->ring);
    }
    int32_t n = 0;
    unsigned head, seen = 0;
    io_uring_for_each_cqe(&r->ring, head, cqe) {
        if (n >= max) break;
        seen++;
        uint64_t ud = cqe->user_data;
        if (ud == C_REMOVED) continue;
        int32_t fd = (int32_t)(uint32_t)ud;
        c_watch *w = (fd >= 0 && fd < r->cap) ? &r->w[fd] : NULL;
        if (!w || !w->active || !w->armed || w->gen != (uint32_t)(ud >> 32)) continue;   /* a cancelled or stale poll */
        w->armed = 0;
        if (cqe->res < 0) {
            if (cqe->res == -ECANCELED) { mark_dirty(r, fd); continue; }
            out[n].events = POLLERR;
        } else {
            out[n].events = (uint32_t)cqe->res;
        }
        out[n].data = w->data;
        n++;
        mark_dirty(r, fd);
    }
    io_uring_cq_advance(&r->ring, seen);
    return n;
}

/* Closes a ring made by c_ring_create. */
void c_ring_close(int32_t ep) {
    c_ring *r = ring_of(ep);
    if (!r) return;
    c_rings[ep] = NULL;
    if (c_current == r) c_current = NULL;
    io_uring_queue_exit(&r->ring);
    free(r->w); free(r->dirty); free(r);
}

/* --- sockets ----------------------------------------------------------------------------------------- */

static socklen_t fill_addr(struct sockaddr_storage *ss, uint8_t v6, const uint8_t *ip, uint16_t port) {
    memset(ss, 0, sizeof *ss);
    if (v6) {
        struct sockaddr_in6 *a = (struct sockaddr_in6 *)ss;
        a->sin6_family = AF_INET6;
        a->sin6_port = htons(port);
        memcpy(&a->sin6_addr, ip, 16);
        return sizeof *a;
    }
    struct sockaddr_in *a = (struct sockaddr_in *)ss;
    a->sin_family = AF_INET;
    a->sin_port = htons(port);
    memcpy(&a->sin_addr, ip, 4);
    return sizeof *a;
}

/* A listening socket: returns its fd, or -errno. IPv6 listeners are v6-only (v4 has its own listener). */
int32_t c_listen(uint8_t v6, const uint8_t *ip, uint16_t port, uint8_t reuseport, int32_t backlog) {
    int fd = socket(v6 ? AF_INET6 : AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (fd < 0) return -errno;
    int one = 1, err = 0;
    struct sockaddr_storage ss;
    socklen_t len = fill_addr(&ss, v6, ip, port);
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one) < 0) err = errno;
    else if (reuseport && setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof one) < 0) err = errno;
    else if (v6 && setsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY, &one, sizeof one) < 0) err = errno;
    else if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one) < 0) err = errno;   /* accepted sockets inherit it */
    else if (bind(fd, (struct sockaddr *)&ss, len) < 0) err = errno;
    else if (listen(fd, backlog) < 0) err = errno;
    if (err) { close(fd); return -err; }
    return fd;
}

/* Accepts one connection: its fd (non-blocking, TCP_NODELAY), or -errno (-EAGAIN when none is waiting). */
int32_t c_accept(int32_t lfd) {
    int fd = accept4(lfd, NULL, NULL, SOCK_NONBLOCK | SOCK_CLOEXEC);
    if (fd < 0) return -errno;
    return fd;                                   /* TCP_NODELAY is inherited from the listening socket (c_listen) */
}

/* Starts a non-blocking connect: the socket's fd with the connect in progress (or already done), or -errno.
   Writability of the fd, then c_connect_result, tells how it ended. */
int32_t c_connect_start(uint8_t v6, const uint8_t *ip, uint16_t port) {
    int fd = socket(v6 ? AF_INET6 : AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (fd < 0) return -errno;
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
    struct sockaddr_storage ss;
    socklen_t len = fill_addr(&ss, v6, ip, port);
    if (connect(fd, (struct sockaddr *)&ss, len) < 0 && errno != EINPROGRESS) {
        int err = errno;
        close(fd);
        return -err;
    }
    return fd;
}

/* How a connect ended: 0 for success, else the errno. */
int32_t c_connect_result(int32_t fd) {
    int err = 0;
    socklen_t len = sizeof err;
    if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len) < 0) return errno;
    return err;
}

/* Bytes read, 0 at end of stream, or -errno (-EAGAIN when nothing is waiting). */
int64_t c_recv(int32_t fd, uint8_t *buf, int64_t len) {
    ssize_t n = recv(fd, buf, (size_t)len, 0);
    return n < 0 ? -errno : n;
}

/* Bytes sent, or -errno. MSG_NOSIGNAL: a closed peer is an error return, never SIGPIPE. */
int64_t c_send(int32_t fd, const uint8_t *buf, int64_t len) {
    ssize_t n = send(fd, buf, (size_t)len, MSG_NOSIGNAL);
    return n < 0 ? -errno : n;
}

void c_shutdown_wr(int32_t fd) { shutdown(fd, SHUT_WR); }
/* A poll in flight holds its file open, so the poll goes first: a ring, unlike epoll, does not forget a descriptor when it is closed. */
void c_close(int32_t fd) {
    c_ring *r = c_current;
    if (r && fd >= 0 && fd < r->cap && r->w[fd].active) {
        unarm(r, fd);
        r->w[fd].active = 0; r->w[fd].gen++;
        io_uring_submit(&r->ring);
    }
    close(fd);
}

/* Makes the next close of the socket send an RST instead of a FIN (SO_LINGER 1, 0). Does not close. */
void c_linger0(int32_t fd) {
    struct linger lg = { 1, 0 };
    setsockopt(fd, SOL_SOCKET, SO_LINGER, &lg, sizeof lg);
}

/* The bound port of a socket (getsockname), or -errno. */
int32_t c_local_port(int32_t fd) {
    struct sockaddr_storage ss;
    socklen_t len = sizeof ss;
    if (getsockname(fd, (struct sockaddr *)&ss, &len) < 0) return -errno;
    if (ss.ss_family == AF_INET6) return ntohs(((struct sockaddr_in6 *)&ss)->sin6_port);
    return ntohs(((struct sockaddr_in *)&ss)->sin_port);
}

/* The peer of a connected socket (getpeername): 4 or 6 (the family), with `out` holding the address in network order
   in bytes 0 to 15 and the port in bytes 16 and 17 (big endian); or -errno. */
int32_t c_peer(int32_t fd, uint8_t *out) {
    struct sockaddr_storage ss;
    socklen_t len = sizeof ss;
    memset(out, 0, 18);
    if (getpeername(fd, (struct sockaddr *)&ss, &len) < 0) return -errno;
    if (ss.ss_family == AF_INET6) {
        struct sockaddr_in6 *a = (struct sockaddr_in6 *)&ss;
        memcpy(out, &a->sin6_addr, 16);
        out[16] = ntohs(a->sin6_port) >> 8; out[17] = ntohs(a->sin6_port) & 0xff;
        return 6;
    }
    if (ss.ss_family == AF_INET) {
        struct sockaddr_in *a = (struct sockaddr_in *)&ss;
        memcpy(out, &a->sin_addr, 4);
        out[16] = ntohs(a->sin_port) >> 8; out[17] = ntohs(a->sin_port) & 0xff;
        return 4;
    }
    return -EAFNOSUPPORT;
}

/* Appends strerror(err) to a Goose byte builder. */
void c_strerror(gs_rref out, int32_t err) {
    const char *s = strerror(err);
    gs_bld_append(out, s, (int64_t)strlen(s));
}

/* --- the batching window (RT-16) ---------------------------------------------------------------------- */

/* Waits up to `us` microseconds for `fd` to become readable (a worker's message pipe), and returns the microseconds actually waited (-1 on error).
   A message arriving ends the wait at once: that is the point of waiting on the pipe and not sleeping. */
int64_t c_wait_readable_us(int32_t fd, int64_t us) {
    struct timespec t0, t1, to;
    struct pollfd p;
    p.fd = fd; p.events = POLLIN; p.revents = 0;
    to.tv_sec = us / 1000000; to.tv_nsec = (us % 1000000) * 1000;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    int rc = ppoll(&p, 1, &to, NULL);
    clock_gettime(CLOCK_MONOTONIC, &t1);
    if (rc < 0 && errno != EINTR) return -1;
    return (int64_t)(t1.tv_sec - t0.tv_sec) * 1000000 + (t1.tv_nsec - t0.tv_nsec) / 1000;
}

/* --- signals ----------------------------------------------------------------------------------------- */

/* Blocks SIGTERM and SIGINT in the calling thread (threads started later inherit that) and returns a
   signalfd that reports them, or -errno. Call it before starting any worker. */
int32_t c_signals_open(void) {
    sigset_t set;
    sigemptyset(&set);
    sigaddset(&set, SIGTERM);
    sigaddset(&set, SIGINT);
    sigaddset(&set, SIGUSR1);          /* a program may drain on SIGUSR1 (wait_for_signal returns the number) */
    int rc = pthread_sigmask(SIG_BLOCK, &set, NULL);
    if (rc != 0) return -rc;
    int fd = signalfd(-1, &set, SFD_CLOEXEC);
    return fd < 0 ? -errno : fd;
}

/* Waits until a signal arrives on the signalfd; returns its number, or -errno. */
int32_t c_signal_wait(int32_t fd) {
    struct signalfd_siginfo si;
    for (;;) {
        ssize_t n = read(fd, &si, sizeof si);
        if (n == (ssize_t)sizeof si) return (int32_t)si.ssi_signo;
        if (n < 0 && errno != EINTR) return -errno;
    }
}

/* --- pipes (messages to workers) ---------------------------------------------------------------------- */

/* A pipe, close-on-exec, whose READ end is non-blocking (a worker polls it from the ring); the write end blocks. 0 or -errno. */
int32_t c_pipe(int32_t *fds) {
    if (pipe2(fds, O_CLOEXEC) < 0) return -errno;
    return fcntl(fds[0], F_SETFL, O_NONBLOCK) < 0 ? -errno : 0;
}

int64_t c_pipe_write(int32_t fd, const uint8_t *buf, int64_t len) {
    ssize_t n = write(fd, buf, (size_t)len);
    return n < 0 ? -errno : n;
}

int64_t c_pipe_read(int32_t fd, uint8_t *buf, int64_t len) {
    ssize_t n = read(fd, buf, (size_t)len);
    return n < 0 ? -errno : n;
}

/* --- clock ------------------------------------------------------------------------------------------- */

/* CLOCK_BOOTTIME in milliseconds: counts through suspend (RT-6c). */
int64_t c_boottime_ms(void) {
    struct timespec ts;
    if (clock_gettime(CLOCK_BOOTTIME, &ts) != 0) return -1;
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}
