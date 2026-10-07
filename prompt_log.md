
# AI Prompt Log – IE3090 RemoteOps (IT24101716)

Tool used: **Claude (Anthropic), claude.ai chat**. Date: 7 October 2026.

| # | Prompt (summary) | What the AI did | How I used / changed it |
|---|---|---|---|
| 1 | Uploaded the assignment brief and asked what to do step by step | Summarised the deadline (7 Oct, 10:59 PM) and gave a prioritised plan: personalised values, GitHub, Agent, Controller, testing, report, process documents | Used as a checklist. [add what you followed/ignored] |
| 2 | Gave my registration number IT24101716 and said I work on a CentOS terminal | Calculated port 9410, SID 6171, token OPS-1716, file/log/storage names. Wrote `agent_716.c`, `controller_716.c`, `Makefile_716` and tested them in its own Ubuntu sandbox | Downloaded, compiled and ran on my CentOS machine. [state any edits you made, or "none"] |
| 3 | (During AI's testing) | The AI's first test crashed the Controller on GET (buffer overflow: leftover receive buffer larger than the copy buffer). It fixed `recv_bytes` in the Controller and retested PUT/GET (byte-identical), UDP monitor, 6 concurrent clients, and an abrupt mid-PUT disconnect | Final code includes this fix. This is an example of AI output being wrong until tested. |
| 4 | Asked what to do next and about terminal errors (file not found, `ss` typed into the Controller, `ls -l` typos) | Explained the mistakes and gave the correct commands | Followed the corrected commands. |
| 5 | Asked for help setting up GitHub (repo creation, git init/commit, push) and fixing "Authentication failed" | Explained commits, remote setup and personal access tokens | Created the repo and a token; pushed commits. |
| 6 | Asked for README, design diary and prompt log | Drafted templates based on the work done in this chat | I reviewed and edited them: [describe your edits]. |

## Notes
- The Agent/Controller source code was largely AI-generated. I read through it,
  ran every command myself, and took all screenshots from my own runs.
  [Add what you studied/understood, e.g. read_line framing, PUT byte counting,
  thread-per-connection, monitor thread.]
