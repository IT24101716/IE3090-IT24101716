/*
 * controller_716.c - RemoteOps Controller (client)
 * IE3090 Network Programming - Registration number: IT24101716
 *
 * Usage: ./controller_716 [agent_host] [agent_port]     (defaults: 127.0.0.1 9410)
 *
 * Type protocol commands at the prompt:
 *   AUTH OPS-1716 | SYSINFO | LISTPROC | EXEC DATE | QUIT
 *   PUT <local_path>         uploads the file (sent as: PUT <name> <size> + bytes)
 *   GET <name>               downloads into ./downloads/<name>
 *   MONITOR START [udp_port] starts UDP listener (default 5410) then tells Agent
 *   MONITOR STOP
 */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <signal.h>
#include <pthread.h>
#include <stdatomic.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/select.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>

#define DEFAULT_HOST     "127.0.0.1"
#define DEFAULT_PORT     "9410"
#define DEFAULT_UDP_PORT 5410
#define BUF_SIZE         65536

static int tcp_fd = -1;
static char rbuf[BUF_SIZE];
static size_t rlen = 0;

/* UDP monitor listener */
static int udp_fd = -1;
static atomic_int udp_run;
static pthread_t udp_th;
static int udp_on = 0;

static int send_all(int fd, const void *buf, size_t n)
{
    const char *p = buf;
    while (n > 0) {
        ssize_t r = send(fd, p, n, MSG_NOSIGNAL);
        if (r < 0) { if (errno == EINTR) continue; return -1; }
        p += r;
        n -= (size_t)r;
    }
    return 0;
}

/* 1 = line, 0 = closed, -1 = error */
static int read_line(char *out, size_t outsz)
{
    for (;;) {
        char *nl = memchr(rbuf, '\n', rlen);
        if (nl) {
            size_t n = (size_t)(nl - rbuf);
            size_t copy = n < outsz - 1 ? n : outsz - 1;
            memcpy(out, rbuf, copy);
            out[copy] = '\0';
            size_t used = n + 1;
            memmove(rbuf, rbuf + used, rlen - used);
            rlen -= used;
            return 1;
        }
        if (rlen == sizeof rbuf) return -1;
        ssize_t r = recv(tcp_fd, rbuf + rlen, sizeof rbuf - rlen, 0);
        if (r == 0) return 0;
        if (r < 0) { if (errno == EINTR) continue; return -1; }
        rlen += (size_t)r;
    }
}

/* receive exactly n bytes into f (uses leftover bytes in rbuf first) */
static int recv_bytes(FILE *f, unsigned long long n)
{
    char tmp[8192];
    while (n > 0) {
        size_t chunk;
        if (rlen > 0) {
            chunk = rlen < n ? rlen : (size_t)n;
            if (chunk > sizeof tmp) chunk = sizeof tmp;
            memcpy(tmp, rbuf, chunk);
            memmove(rbuf, rbuf + chunk, rlen - chunk);
            rlen -= chunk;
        } else {
            size_t want = n < sizeof tmp ? (size_t)n : sizeof tmp;
            ssize_t r = recv(tcp_fd, tmp, want, 0);
            if (r == 0) return 0;
            if (r < 0) { if (errno == EINTR) continue; return -1; }
            chunk = (size_t)r;
        }
        if (fwrite(tmp, 1, chunk, f) != chunk) return -1;
        n -= chunk;
    }
    return 1;
}

/* ---------- UDP listener ---------- */
static void *udp_listener(void *arg)
{
    (void)arg;
    char b[512];
    while (atomic_load(&udp_run)) {
        fd_set rf;
        struct timeval tv = { 1, 0 };
        FD_ZERO(&rf);
        FD_SET(udp_fd, &rf);
        if (select(udp_fd + 1, &rf, NULL, NULL, &tv) > 0) {
            ssize_t n = recvfrom(udp_fd, b, sizeof b - 1, 0, NULL, NULL);
            if (n > 0) {
                b[n] = '\0';
                printf("\n[UDP] %s\nremoteops> ", b);
                fflush(stdout);
            }
        }
    }
    return NULL;
}

static int udp_start(int port)
{
    udp_fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (udp_fd < 0) return -1;
    int on = 1;
    setsockopt(udp_fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof on);
    struct sockaddr_in a;
    memset(&a, 0, sizeof a);
    a.sin_family = AF_INET;
    a.sin_addr.s_addr = htonl(INADDR_ANY);
    a.sin_port = htons((uint16_t)port);
    if (bind(udp_fd, (struct sockaddr *)&a, sizeof a) < 0) { close(udp_fd); return -1; }
    atomic_store(&udp_run, 1);
    if (pthread_create(&udp_th, NULL, udp_listener, NULL) != 0) { close(udp_fd); return -1; }
    udp_on = 1;
    return 0;
}

static void udp_stop(void)
{
    if (!udp_on) return;
    atomic_store(&udp_run, 0);
    pthread_join(udp_th, NULL);
    close(udp_fd);
    udp_on = 0;
}

/* ---------- commands ---------- */
static int send_line(const char *s)
{
    char out[2048];
    int n = snprintf(out, sizeof out, "%s\n", s);
    return send_all(tcp_fd, out, (size_t)n);
}

/* read + print one response line; returns line status */
static int show_response(char *line, size_t sz)
{
    int r = read_line(line, sz);
    if (r == 1) printf("%s\n", line);
    else printf("[connection closed by Agent]\n");
    return r;
}

static int do_put(const char *path)
{
    struct stat st;
    if (stat(path, &st) < 0 || !S_ISREG(st.st_mode)) {
        printf("cannot open local file '%s'\n", path);
        return 1;
    }
    const char *name = strrchr(path, '/');
    name = name ? name + 1 : path;
    FILE *f = fopen(path, "rb");
    if (!f) { printf("cannot open '%s'\n", path); return 1; }

    char hdr[1200];
    snprintf(hdr, sizeof hdr, "PUT %s %lld", name, (long long)st.st_size);
    if (send_line(hdr) < 0) { fclose(f); return -1; }

    char tmp[8192];
    size_t r;
    while ((r = fread(tmp, 1, sizeof tmp, f)) > 0)
        if (send_all(tcp_fd, tmp, r) < 0) { fclose(f); return -1; }
    fclose(f);

    char line[BUF_SIZE];
    return show_response(line, sizeof line);
}

static int do_get(const char *name)
{
    char req[1100], line[BUF_SIZE];
    snprintf(req, sizeof req, "GET %s", name);
    if (send_line(req) < 0) return -1;

    int r = show_response(line, sizeof line);
    if (r != 1) return r;

    char fname[256];
    unsigned long long size;
    if (sscanf(line, "OK FILE_SEND %255s %llu", fname, &size) != 2)
        return 1;                                   /* an ERR line */

    mkdir("downloads", 0755);
    const char *base = strrchr(fname, '/');
    base = base ? base + 1 : fname;
    char path[512];
    snprintf(path, sizeof path, "downloads/%s", base);
    FILE *f = fopen(path, "wb");
    if (!f) { printf("cannot create %s\n", path); return -1; }
    int rc = recv_bytes(f, size);
    fclose(f);
    if (rc <= 0) { printf("download failed\n"); return -1; }
    printf("[saved %llu bytes to %s]\n", size, path);
    return 1;
}

static int do_monitor(char *args)
{
    char line[BUF_SIZE];
    char *sp;
    char *sub = strtok_r(args, " ", &sp);
    if (!sub) { printf("usage: MONITOR START [udp_port] | MONITOR STOP\n"); return 1; }

    if (strcmp(sub, "START") == 0) {
        char *ps = strtok_r(NULL, " ", &sp);
        int port = ps ? atoi(ps) : DEFAULT_UDP_PORT;
        if (udp_on) { printf("monitor already running locally\n"); return 1; }
        if (udp_start(port) < 0) { printf("cannot bind UDP port %d\n", port); return 1; }
        char req[64];
        snprintf(req, sizeof req, "MONITOR START %d", port);
        if (send_line(req) < 0) return -1;
        int r = show_response(line, sizeof line);
        if (r != 1) return r;
        if (strncmp(line, "OK", 2) != 0) udp_stop();
        return 1;
    }
    if (strcmp(sub, "STOP") == 0) {
        if (send_line("MONITOR STOP") < 0) return -1;
        int r = show_response(line, sizeof line);
        udp_stop();
        return r;
    }
    printf("usage: MONITOR START [udp_port] | MONITOR STOP\n");
    return 1;
}

int main(int argc, char **argv)
{
    signal(SIGPIPE, SIG_IGN);
    const char *host = argc > 1 ? argv[1] : DEFAULT_HOST;
    const char *port = argc > 2 ? argv[2] : DEFAULT_PORT;

    struct addrinfo hints, *res;
    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    if (getaddrinfo(host, port, &hints, &res) != 0) {
        fprintf(stderr, "cannot resolve %s\n", host);
        return 1;
    }
    tcp_fd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (tcp_fd < 0 || connect(tcp_fd, res->ai_addr, res->ai_addrlen) < 0) {
        perror("connect");
        return 1;
    }
    freeaddrinfo(res);
    printf("Connected to %s:%s\n", host, port);

    char in[1024], line[BUF_SIZE];
    for (;;) {
        printf("remoteops> ");
        fflush(stdout);
        if (!fgets(in, sizeof in, stdin)) break;
        in[strcspn(in, "\r\n")] = '\0';
        if (!in[0]) continue;

        int rc;
        if (strncmp(in, "PUT ", 4) == 0) {
            rc = do_put(in + 4);
        } else if (strncmp(in, "GET ", 4) == 0) {
            rc = do_get(in + 4);
        } else if (strncmp(in, "MONITOR", 7) == 0 && (in[7] == ' ' || !in[7])) {
            rc = do_monitor(in[7] ? in + 8 : in + 7);
        } else {
            if (send_line(in) < 0) { printf("send failed\n"); break; }
            rc = show_response(line, sizeof line);
            if (rc == 1 && strncmp(line, "OK BYE", 6) == 0) break;
        }
        if (rc != 1) break;
    }
    udp_stop();
    close(tcp_fd);
    return 0;
}
