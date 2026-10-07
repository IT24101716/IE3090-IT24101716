# Design Diary – IE3090 RemoteOps (IT24101716)

## 7 October 2026

**Planning.**
Read the assignment brief and worked out the personalised values from my
registration number IT24101716: port 9410 (7000 + 2410), SID 6171 (1716 reversed),
token OPS-1716, files `agent_716.c` / `controller_716.c` / `Makefile_716`, log
`remoteops_IT24101716.log`, storage `./agentfiles/IT24101716/`.

**Key design decisions.**
- *Concurrency:* one detached pthread per Controller connection. Chosen because it
  is simple to reason about, each session keeps its own state (auth flag, receive
  buffer, UDP monitor), and 5+ clients is well within thread limits. `select()`
  would avoid threads but makes per-session state and file transfers more complex.
- *Framing:* each connection has its own buffer; `read_line()` keeps reading until
  a `\n` is present and leaves extra bytes for the next call, so partial lines and
  several lines in one `recv()` both work. For PUT, bytes that arrived with the
  command line are consumed first, then exactly `<filesize>` bytes are read.
- *Security:* AUTH required first; EXEC uses a fixed table of five commands and
  never builds a shell string from user input; filenames limited to
  `[A-Za-z0-9._-]` to block path traversal; 10 MB upload limit.
- *UDP monitoring:* one sender thread per session, one datagram every 2 seconds,
  stopped on MONITOR STOP, QUIT or disconnect.
- *Robustness:* SIGPIPE ignored; a disconnect mid-PUT removes the partial file.

**Testing and obstacles.**
- Compiled on CentOS and tested every command against the Agent (screenshots in
  the report).
- Typed a command (`ss`) into the Controller by mistake; the Agent correctly
  refused it as not authenticated.
- While the UDP stream was running, the `[UDP]` lines interrupted my typing and
  some fragments were sent as junk commands (`ERR 007 UNKNOWN_COMMAND`). The Agent
  handled them correctly. Possible improvement: print UDP output without
  disturbing the input line.
- Commands are case-sensitive: `Get report.txt` returned `ERR 007`.
- GitHub rejected my account password when pushing; I had to create a personal
  access token.
- **Process note (honest):** I started the implementation late, on the deadline
  day, so my commit history is concentrated on 7 October instead of spread across
  the assignment period.

**Not done / future work.** [Add: e.g. TLS, history snapshots, compression were not implemented.]
