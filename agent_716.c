/*
 * agent_716.c - RemoteOps Agent (server)
 * IE3090 Network Programming - Registration number: IT24101716
 *
 * Personalised values (see README / report):
 *   Port      = 7000 + 2410      = 9410
 *   SID tag   = reverse("1716")  = 6171
 *   Token     = OPS-1716
 *   Log file  = remoteops_IT24101716.log
 *   Storage   = ./agentfiles/IT24101716/<filename>
 *
 * Concurrency model: one detached POSIX thread per Controller connection.
 */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <ctype.h>
#include <errno.h>
#include <unistd.h>
#include <signal.h>
#include <time.h>
#include <pthread.h>
#include <stdatomic.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <arpa/inet.h>

/* ---- personalised configuration ---- */
#define REGNO       "IT24101716"
#define PORT        9410
#define SID         "6171"
#define AUTH_TOKEN  "OPS-1716"
#define LOG_FILE    "remoteops_IT24101716.log"
#define STORE_ROOT  "./agentfiles"
#define STORE_DIR   "./agentfiles/IT24101716"

/* ---- tunables ---- */
#define MAX_FILE_SIZE   (10ULL * 1024 * 1024)   /* 10 MB upload limit        */
#define DRAIN_LIMIT     (100ULL * 1024 * 1024)  /* above this, close instead */
#define MON_INTERVAL_SEC 2                      /* UDP datagram interval     */
#define MAX_LINE        1024
#define BUF_SIZE        4096
#define MAX_AUTH_TRIES  3

/* ---- error replies: ERR <code> <REASON>  (SID is appended by reply()) ---- */
#define E_AUTH_FAILED   "ERR 001 AUTH_FAILED"
#define E_NOT_ALLOWED   "ERR 002 COMMAND_NOT_ALLOWED"
#define E_NOT_AUTH      "ERR 003 NOT_AUTHENTICATED"
#define E_TOO_LARGE     "ERR 004 FILE_TOO_LARGE"
#define E_NOT_FOUND     "ERR 005 FILE_NOT_FOUND"
#define E_BAD_REQUEST   "ERR 006 BAD_REQUEST"
#define E_UNKNOWN_CMD   "ERR 007 UNKNOWN_COMMAND"
#define E_MON_STATE     "ERR 008 MONITOR_STATE_INVALID"
#define E_INTERNAL      "ERR 009 INTERNAL_ERROR"
#define E_BAD_FILENAME  "ERR 010 BAD_FILENAME"

/* ---- per-connection session state ---- */
typedef struct {
    int fd;
    struct sockaddr_in peer;
    char peer_str[48];              /* "ip:port" for logging */
    char buf[BUF_SIZE];             /* receive buffer (framing) */
    size_t len;                     /* bytes currently in buf */
    int authed;
    /* UDP monitoring */
    int mon_on;
    atomic_int mon_run;
    pthread_t mon_th;
    int udp_fd;
    struct sockaddr_in mon_addr;
} session_t;

/* ======================= logging ======================= */
static pthread_mutex_t log_mtx = PTHREAD_MUTEX_INITIALIZER;

static void log_event(const char *fmt, ...)
{
    char ts[32], msg[1024];
    time_t t = time(NULL);
    struct tm tm;
    localtime_r(&t, &tm);
    strftime(ts, sizeof ts, "%Y-%m-%d %H:%M:%S", &tm);

    va_list ap;
    va_start(ap, fmt);
    vsnprintf(msg, sizeof msg, fmt, ap);
    va_end(ap);

    pthread_mutex_lock(&log_mtx);
    FILE *f = fopen(LOG_FILE, "a");
    if (f) {
        fprintf(f, "[%s] %s\n", ts, msg);
        fclose(f);
    }
    printf("[%s] %s\n", ts, msg);
    fflush(stdout);
    pthread_mutex_unlock(&log_mtx);
}

/* ======================= socket helpers ======================= */
static int send_all(int fd, const void *buf, size_t n)
{
    const char *p = buf;
    while (n > 0) {
        ssize_t r = send(fd, p, n, MSG_NOSIGNAL);
        if (r < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        p += r;
        n -= (size_t)r;
    }
    return 0;
}

/* Send one response line; always appends " SID:<sid>\n". Returns 0 / -1. */
static int reply(session_t *s, const char *fmt, ...)
{
    static const size_t BODY = 60000;
    char *out = malloc(BODY + 32);
    if (!out) return -1;

    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(out, BODY, fmt, ap);
    va_end(ap);
    if (n < 0) { free(out); return -1; }
    if ((size_t)n >= BODY) n = (int)BODY - 1;
    n += snprintf(out + n, 32, " SID:%s\n", SID);

    int rc = send_all(s->fd, out, (size_t)n);
    free(out);
    return rc;
}

/*
 * Framing: read one '\n'-terminated line from the socket.
 * Handles partial lines (keeps reading) and several lines in one recv()
 * (leftover bytes stay in s->buf for the next call).
 * Returns 1 = line read, 0 = peer closed, -1 = error, -2 = line too long.
 */
static int read_line(session_t *s, char *out, size_t outsz)
{
    for (;;) {
        char *nl = memchr(s->buf, '\n', s->len);
        if (nl) {
            size_t n = (size_t)(nl - s->buf);
            size_t copy = n < outsz - 1 ? n : outsz - 1;
            memcpy(out, s->buf, copy);
            out[copy] = '\0';
            if (copy > 0 && out[copy - 1] == '\r') out[copy - 1] = '\0';
            size_t used = n + 1;
            memmove(s->buf, s->buf + used, s->len - used);
            s->len -= used;
            return 1;
        }
        if (s->len == sizeof s->buf) return -2;
        ssize_t r = recv(s->fd, s->buf + s->len, sizeof s->buf - s->len, 0);
        if (r == 0) return 0;
        if (r < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        s->len += (size_t)r;
    }
}

/*
 * Receive exactly n raw bytes (PUT body). Bytes already sitting in s->buf
 * (they arrived together with the PUT line) are consumed first.
 * f == NULL discards the data. Returns 1 ok, 0 peer closed, -1 error.
 */
static int recv_bytes(session_t *s, FILE *f, unsigned long long n)
{
    char tmp[8192];
    while (n > 0) {
        size_t chunk;
        if (s->len > 0) {
            chunk = s->len < n ? s->len : (size_t)n;
            memcpy(tmp, s->buf, chunk);
            memmove(s->buf, s->buf + chunk, s->len - chunk);
            s->len -= chunk;
        } else {
            size_t want = n < sizeof tmp ? (size_t)n : sizeof tmp;
            ssize_t r = recv(s->fd, tmp, want, 0);
            if (r == 0) return 0;
            if (r < 0) {
                if (errno == EINTR) continue;
                return -1;
            }
            chunk = (size_t)r;
        }
        if (f && fwrite(tmp, 1, chunk, f) != chunk) return -1;
        n -= chunk;
    }
    return 1;
}

/* ======================= system info helpers ======================= */
static void get_sysinfo(char *out, size_t n)
{
    double load = 0.0, up = 0.0;
    long total = 0, avail = -1, memfree = 0, buffers = 0, cached = 0, v;
    char line[256];

    FILE *f = fopen("/proc/loadavg", "r");
    if (f) { if (fscanf(f, "%lf", &load) != 1) load = 0.0; fclose(f); }

    f = fopen("/proc/meminfo", "r");
    if (f) {
        while (fgets(line, sizeof line, f)) {
            if (sscanf(line, "MemTotal: %ld", &v) == 1) total = v;
            else if (sscanf(line, "MemAvailable: %ld", &v) == 1) avail = v;
            else if (sscanf(line, "MemFree: %ld", &v) == 1) memfree = v;
            else if (sscanf(line, "Buffers: %ld", &v) == 1) buffers = v;
            else if (sscanf(line, "Cached: %ld", &v) == 1) cached = v;
        }
        fclose(f);
    }
    if (avail < 0) avail = memfree + buffers + cached;   /* old kernels */
    long used_mb = (total - avail) / 1024;

    f = fopen("/proc/uptime", "r");
    if (f) { if (fscanf(f, "%lf", &up) != 1) up = 0.0; fclose(f); }

    snprintf(out, n, "%.2f %ld %ld", load, used_mb, (long)up);
}

/* Run a FIXED command string (never built from user input) and capture output
 * as a single line (newlines -> spaces). */
static void run_fixed(const char *cmd, char *out, size_t outsz)
{
    size_t n = 0;
    FILE *p = popen(cmd, "r");
    out[0] = '\0';
    if (!p) { snprintf(out, outsz, "error"); return; }
    int c;
    while ((c = fgetc(p)) != EOF && n < outsz - 1)
        out[n++] = (c == '\n' || c == '\r') ? ' ' : (char)c;
    out[n] = '\0';
    pclose(p);
    while (n > 0 && out[n - 1] == ' ') out[--n] = '\0';   /* trim */
}

static void get_proclist(char *out, size_t outsz)
{
    size_t n = 0;
    out[0] = '\0';
    FILE *p = popen("ps -eo pid=,comm=", "r");
    if (!p) return;
    char line[256];
    while (fgets(line, sizeof line, p)) {
        int pid;
        char name[128];
        if (sscanf(line, "%d %127[^\n]", &pid, name) != 2) continue;
        for (char *q = name; *q; q++)
            if (*q == ' ' || *q == ',') *q = '_';
        int w = snprintf(out + n, outsz - n, "%s%d/%s", n ? "," : "", pid, name);
        if (w < 0 || (size_t)w >= outsz - n) break;       /* buffer full */
        n += (size_t)w;
    }
    pclose(p);
}

/* ======================= UDP monitoring ======================= */
static void *monitor_thread(void *arg)
{
    session_t *s = arg;
    while (atomic_load(&s->mon_run)) {
        char info[96], msg[160];
        get_sysinfo(info, sizeof info);
        int n = snprintf(msg, sizeof msg, "SYSINFO %s SID:%s", info, SID);
        sendto(s->udp_fd, msg, (size_t)n, 0,
               (struct sockaddr *)&s->mon_addr, sizeof s->mon_addr);
        for (int i = 0; i < MON_INTERVAL_SEC * 10 && atomic_load(&s->mon_run); i++)
            usleep(100000);
    }
    return NULL;
}

static int start_monitor(session_t *s, int udp_port)
{
    s->udp_fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (s->udp_fd < 0) return -1;
    memset(&s->mon_addr, 0, sizeof s->mon_addr);
    s->mon_addr.sin_family = AF_INET;
    s->mon_addr.sin_addr = s->peer.sin_addr;      /* Controller's IP */
    s->mon_addr.sin_port = htons((uint16_t)udp_port);
    atomic_store(&s->mon_run, 1);
    if (pthread_create(&s->mon_th, NULL, monitor_thread, s) != 0) {
        close(s->udp_fd);
        return -1;
    }
    s->mon_on = 1;
    return 0;
}

static void stop_monitor(session_t *s)
{
    if (!s->mon_on) return;
    atomic_store(&s->mon_run, 0);
    pthread_join(s->mon_th, NULL);
    close(s->udp_fd);
    s->mon_on = 0;
}

/* ======================= file transfer ======================= */
static int valid_filename(const char *n)
{
    size_t len = strlen(n);
    if (len == 0 || len > 100 || n[0] == '.') return 0;
    for (size_t i = 0; i < len; i++) {
        char c = n[i];
        if (!(isalnum((unsigned char)c) || c == '.' || c == '_' || c == '-'))
            return 0;                              /* blocks '/' and '..' tricks */
    }
    return 1;
}

static int parse_size(const char *str, unsigned long long *out)
{
    char *end;
    if (!*str || *str == '-') return 0;
    errno = 0;
    unsigned long long v = strtoull(str, &end, 10);
    if (errno || *end) return 0;
    *out = v;
    return 1;
}

static double elapsed(struct timespec a, struct timespec b)
{
    return (double)(b.tv_sec - a.tv_sec) + (double)(b.tv_nsec - a.tv_nsec) / 1e9;
}

/* returns 0 = keep session, -1 = close session */
static int handle_put(session_t *s, char *args)
{
    char *sp;
    char *fname = strtok_r(args, " ", &sp);
    char *szs = strtok_r(NULL, " ", &sp);
    unsigned long long size;

    if (!fname || !szs || strtok_r(NULL, " ", &sp) || !parse_size(szs, &size))
        return reply(s, E_BAD_REQUEST);

    if (size > DRAIN_LIMIT) {                      /* absurd size: refuse + close */
        log_event("%s PUT %s rejected (size %llu too large)", s->peer_str, fname, size);
        reply(s, E_TOO_LARGE);
        return -1;
    }
    if (size > MAX_FILE_SIZE || !valid_filename(fname)) {
        /* keep the stream framed: discard the announced bytes, then error */
        if (recv_bytes(s, NULL, size) <= 0) return -1;
        if (!valid_filename(fname)) {
            log_event("%s PUT rejected (bad filename)", s->peer_str);
            return reply(s, E_BAD_FILENAME);
        }
        log_event("%s PUT %s rejected (%llu bytes > limit)", s->peer_str, fname, size);
        return reply(s, E_TOO_LARGE);
    }

    char path[256];
    snprintf(path, sizeof path, "%s/%s", STORE_DIR, fname);
    FILE *f = fopen(path, "wb");
    if (!f) {
        if (recv_bytes(s, NULL, size) <= 0) return -1;
        log_event("%s PUT %s failed: cannot open %s", s->peer_str, fname, path);
        return reply(s, E_INTERNAL);
    }

    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    int rc = recv_bytes(s, f, size);
    fclose(f);
    if (rc <= 0) {                                 /* disconnect mid-transfer */
        unlink(path);
        log_event("%s PUT %s aborted (connection lost), partial file removed",
                  s->peer_str, fname);
        return -1;
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);
    double secs = elapsed(t0, t1);
    log_event("%s PUT %s %llu bytes stored at %s (%.0f B/s)", s->peer_str, fname,
              size, path, secs > 0 ? (double)size / secs : (double)size);
    return reply(s, "OK FILE_RECEIVED %s", fname);
}

static int handle_get(session_t *s, char *args)
{
    char *sp;
    char *fname = strtok_r(args, " ", &sp);
    if (!fname || strtok_r(NULL, " ", &sp)) return reply(s, E_BAD_REQUEST);

    char path[256];
    struct stat st;
    snprintf(path, sizeof path, "%s/%s", STORE_DIR, fname);
    FILE *f = NULL;
    if (valid_filename(fname) && stat(path, &st) == 0 && S_ISREG(st.st_mode))
        f = fopen(path, "rb");
    if (!f) {
        log_event("%s GET %s -> not found", s->peer_str, fname);
        return reply(s, E_NOT_FOUND);
    }

    unsigned long long size = (unsigned long long)st.st_size, left = size;
    if (reply(s, "OK FILE_SEND %s %llu", fname, size) < 0) { fclose(f); return -1; }

    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    char tmp[8192];
    while (left > 0) {
        size_t want = left < sizeof tmp ? (size_t)left : sizeof tmp;
        size_t r = fread(tmp, 1, want, f);
        if (r == 0 || send_all(s->fd, tmp, r) < 0) {
            fclose(f);
            log_event("%s GET %s aborted", s->peer_str, fname);
            return -1;
        }
        left -= r;
    }
    fclose(f);
    clock_gettime(CLOCK_MONOTONIC, &t1);
    double secs = elapsed(t0, t1);
    log_event("%s GET %s %llu bytes sent (%.0f B/s)", s->peer_str, fname, size,
              secs > 0 ? (double)size / secs : (double)size);
    return 0;
}

/* ======================= command handlers ======================= */
static int handle_exec(session_t *s, char *args)
{
    static const struct { const char *name, *cmd; } wl[] = {
        { "DATE",     "date" },
        { "UPTIME",   "uptime" },
        { "DISKFREE", "df -h /" },
        { "HOSTNAME", "hostname" },
        { "WHOAMI",   "whoami" },
    };
    /* trim spaces */
    while (*args == ' ') args++;
    size_t l = strlen(args);
    while (l > 0 && args[l - 1] == ' ') args[--l] = '\0';

    for (size_t i = 0; i < sizeof wl / sizeof wl[0]; i++) {
        if (strcmp(args, wl[i].name) == 0) {
            char out[2048];
            run_fixed(wl[i].cmd, out, sizeof out);
            log_event("%s EXEC %s", s->peer_str, wl[i].name);
            return reply(s, "OK EXEC_RESULT %s", out);
        }
    }
    log_event("%s EXEC '%s' REJECTED (not in whitelist)", s->peer_str, args);
    return reply(s, E_NOT_ALLOWED);
}

static int handle_monitor(session_t *s, char *args)
{
    char *sp;
    char *sub = strtok_r(args, " ", &sp);
    if (!sub) return reply(s, E_BAD_REQUEST);

    if (strcmp(sub, "START") == 0) {
        char *ps = strtok_r(NULL, " ", &sp);
        unsigned long long port;
        if (!ps || strtok_r(NULL, " ", &sp) || !parse_size(ps, &port) ||
            port < 1 || port > 65535)
            return reply(s, E_BAD_REQUEST);
        if (s->mon_on) return reply(s, E_MON_STATE);
        if (start_monitor(s, (int)port) < 0) return reply(s, E_INTERNAL);
        log_event("%s MONITOR START -> UDP %llu every %ds", s->peer_str, port,
                  MON_INTERVAL_SEC);
        return reply(s, "OK MONITOR_STARTED");
    }
    if (strcmp(sub, "STOP") == 0) {
        if (!s->mon_on) return reply(s, E_MON_STATE);
        stop_monitor(s);
        log_event("%s MONITOR STOP", s->peer_str);
        return reply(s, "OK MONITOR_STOPPED");
    }
    return reply(s, E_BAD_REQUEST);
}

/* ======================= per-client thread ======================= */
static void *client_thread(void *arg)
{
    session_t *s = arg;
    char line[MAX_LINE];
    int auth_fail = 0;

    log_event("%s CONNECTED", s->peer_str);

    for (;;) {
        int r = read_line(s, line, sizeof line);
        if (r == 0) { log_event("%s DISCONNECTED (client closed)", s->peer_str); break; }
        if (r == -1) { log_event("%s DISCONNECTED (recv error: %s)", s->peer_str,
                                 strerror(errno)); break; }
        if (r == -2) { reply(s, E_BAD_REQUEST);
                       log_event("%s closed: line too long", s->peer_str); break; }

        char *args;
        char *cmd = strtok_r(line, " ", &args);
        if (!cmd) { if (reply(s, E_BAD_REQUEST) < 0) break; continue; }

        int rc = 0;
        if (strcmp(cmd, "AUTH") == 0) {
            while (*args == ' ') args++;
            if (strcmp(args, AUTH_TOKEN) == 0) {
                s->authed = 1;
                log_event("%s AUTH success", s->peer_str);
                rc = reply(s, "OK AUTHENTICATED");
            } else {
                auth_fail++;
                log_event("%s AUTH failed (attempt %d/%d)", s->peer_str,
                          auth_fail, MAX_AUTH_TRIES);
                rc = reply(s, E_AUTH_FAILED);
                if (auth_fail >= MAX_AUTH_TRIES) {
                    log_event("%s closed: too many failed AUTH attempts", s->peer_str);
                    break;
                }
            }
        } else if (!s->authed) {
            log_event("%s '%s' refused: not authenticated", s->peer_str, cmd);
            rc = reply(s, E_NOT_AUTH);
        } else if (strcmp(cmd, "SYSINFO") == 0) {
            char info[96];
            get_sysinfo(info, sizeof info);
            log_event("%s SYSINFO", s->peer_str);
            rc = reply(s, "OK SYSINFO %s", info);
        } else if (strcmp(cmd, "LISTPROC") == 0) {
            char *pl = malloc(50000);
            if (!pl) { rc = reply(s, E_INTERNAL); }
            else {
                get_proclist(pl, 50000);
                log_event("%s LISTPROC", s->peer_str);
                rc = reply(s, "OK PROCS %s", pl);
                free(pl);
            }
        } else if (strcmp(cmd, "EXEC") == 0) {
            rc = handle_exec(s, args);
        } else if (strcmp(cmd, "PUT") == 0) {
            rc = handle_put(s, args);
        } else if (strcmp(cmd, "GET") == 0) {
            rc = handle_get(s, args);
        } else if (strcmp(cmd, "MONITOR") == 0) {
            rc = handle_monitor(s, args);
        } else if (strcmp(cmd, "QUIT") == 0) {
            stop_monitor(s);
            reply(s, "OK BYE");
            log_event("%s QUIT", s->peer_str);
            break;
        } else {
            log_event("%s unknown command '%s'", s->peer_str, cmd);
            rc = reply(s, E_UNKNOWN_CMD);
        }
        if (rc < 0) {
            log_event("%s DISCONNECTED (send failed)", s->peer_str);
            break;
        }
    }

    stop_monitor(s);          /* always stop UDP stream when session ends */
    close(s->fd);
    free(s);
    return NULL;
}

/* ======================= main ======================= */
int main(void)
{
    signal(SIGPIPE, SIG_IGN);     /* a dead client must not kill the Agent */

    mkdir(STORE_ROOT, 0755);
    mkdir(STORE_DIR, 0755);

    int lfd = socket(AF_INET, SOCK_STREAM, 0);
    if (lfd < 0) { perror("socket"); return 1; }
    int on = 1;
    setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof on);

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof addr);
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(PORT);
    if (bind(lfd, (struct sockaddr *)&addr, sizeof addr) < 0) { perror("bind"); return 1; }
    if (listen(lfd, 16) < 0) { perror("listen"); return 1; }

    log_event("Agent started for %s: listening on TCP port %d (SID:%s)", REGNO, PORT, SID);

    for (;;) {
        struct sockaddr_in peer;
        socklen_t plen = sizeof peer;
        int cfd = accept(lfd, (struct sockaddr *)&peer, &plen);
        if (cfd < 0) {
            if (errno == EINTR) continue;
            perror("accept");
            continue;
        }
        session_t *s = calloc(1, sizeof *s);
        if (!s) { close(cfd); continue; }
        s->fd = cfd;
        s->peer = peer;
        snprintf(s->peer_str, sizeof s->peer_str, "%s:%d",
                 inet_ntoa(peer.sin_addr), ntohs(peer.sin_port));
        atomic_init(&s->mon_run, 0);

        pthread_t th;
        if (pthread_create(&th, NULL, client_thread, s) != 0) {
            log_event("pthread_create failed for %s", s->peer_str);
            close(cfd);
            free(s);
            continue;
        }
        pthread_detach(th);
    }
}
