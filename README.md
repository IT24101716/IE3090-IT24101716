# IE3090 – RemoteOps (Remote System Monitoring & Management Tool)

**Registration number:** IT24101716
**Module:** IE3090 Network Programming – SLIIT

RemoteOps is a TCP/IP Agent (server) + Controller (client) written in C using the
BSD sockets API, with a UDP channel for periodic monitoring.

## Personalised values (calculated from IT24101716)

| Item | Formula | Value |
|---|---|---|
| Agent listening port | 7000 + first four digits of 24101716 (2410) | **9410** |
| Source files | last three digits (716) | `agent_716.c`, `controller_716.c`, `Makefile_716` |
| Session ID tag | last four digits (1716) reversed | **SID:6171** |
| Auth token | "OPS-" + last four digits | **OPS-1716** |
| Log file | `remoteops_<regno>.log` | `remoteops_IT24101716.log` |
| File storage path | `./agentfiles/<regno>/<filename>` | `./agentfiles/IT24101716/<filename>` |
| Submission archive | `IE3090_<regno>.zip` | `IE3090_IT24101716.zip` |

## Build

```
make -f Makefile_716
```

Requires `gcc` and `make` (on CentOS: `sudo dnf install gcc make`).

## Run

Terminal 1 (Agent):
```
./agent_716
```

Terminal 2 (Controller; defaults to 127.0.0.1 port 9410):
```
./controller_716 [agent_host] [agent_port]
```

Controller commands:

| Command | Description |
|---|---|
| `AUTH OPS-1716` | Authenticate (must be first) |
| `SYSINFO` | CPU load, memory used (MB), uptime (s) |
| `LISTPROC` | Snapshot of running processes (`pid/name,...`) |
| `EXEC <name>` | Whitelist only: DATE, UPTIME, DISKFREE, HOSTNAME, WHOAMI |
| `PUT <local_path>` | Upload a file to `./agentfiles/IT24101716/` |
| `GET <name>` | Download into `./downloads/<name>` |
| `MONITOR START [udp_port]` | Start UDP stats stream (default local port 5410) |
| `MONITOR STOP` | Stop the UDP stream |
| `QUIT` | Close the connection cleanly |

Every Agent response ends with ` SID:6171`.

## Design summary

- **Concurrency:** one detached POSIX thread per Controller connection.
- **Framing:** a per-connection buffer; `read_line()` handles partial lines and
  multiple lines per `recv()`. PUT/GET transfer exactly `<filesize>` bytes.
- **Security:** AUTH gate before any other command; EXEC uses a fixed whitelist
  and never builds a shell command from user input; uploaded filenames are
  restricted to `[A-Za-z0-9._-]` (no path traversal); upload size limit 10 MB.
- **Monitoring:** one UDP sender thread per session, one datagram every 2 s,
  stopped on `MONITOR STOP`, `QUIT` or disconnect.
- **Robustness:** SIGPIPE ignored; disconnects (including mid-PUT) are logged and
  partial files removed.
- **Logging:** all connections, commands and file transfers are written with
  timestamps to `remoteops_IT24101716.log` (PUT/GET also log throughput in B/s).

## Error codes

| Code | Reason |
|---|---|
| 001 | AUTH_FAILED |
| 002 | COMMAND_NOT_ALLOWED |
| 003 | NOT_AUTHENTICATED |
| 004 | FILE_TOO_LARGE |
| 005 | FILE_NOT_FOUND |
| 006 | BAD_REQUEST |
| 007 | UNKNOWN_COMMAND |
| 008 | MONITOR_STATE_INVALID |
| 009 | INTERNAL_ERROR |
| 010 | BAD_FILENAME |

## Repository contents

`agent_716.c`, `controller_716.c`, `Makefile_716`, `README.md`, design diary,
sample log excerpt, and commit history.
