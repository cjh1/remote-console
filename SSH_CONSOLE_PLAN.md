# Plan: Replace Conman with Direct Go SSH for SSH Consoles

## Context

The service currently uses conman (a C daemon) to manage all remote consoles — both IPMI
and SSH-based. For SSH, conman delegates to external `expect` scripts (`ssh-key-console`,
`ssh-pwd-console`). This adds two layers of indirection (conman → expect → ssh) that we
can eliminate by managing SSH connections directly in Go.

Goal: for SSH-type nodes, maintain a persistent SSH connection in Go that:
- Logs all output to the same file path conman uses (`/var/log/conman/conman/console.<nodeID>`)
- Broadcasts output to all attached interactive clients simultaneously (shared session)
- Accepts input from any attached interactive client
- Reconnects automatically on disconnect

IPMI nodes continue to use conman unchanged.

## SSH Library

Use `golang.org/x/crypto/ssh` — the official Go extended crypto library, maintained by the
Go team. Standard choice for SSH in Go (used by Terraform, the Go toolchain, etc.). No
third-party dependency.

---

## Architecture

```
SSH nodes:
  SSHConsoleManager
    └─ SSHConsoleNode (one per node, runs a persistent goroutine)
         ├─ persistent SSH session → remote host  (PTY requested; stderr merged via PTY)
         ├─ writes output → /var/log/conman/conman/console.<nodeID>
         ├─ broadcasts output → chan []byte per attached interactive client
         └─ handles reconnection internally (clients see a pause, not a disconnect)

IPMI nodes:
  conman (unchanged)
```

Interactive WebSocket sessions for SSH nodes attach to `SSHConsoleNode` via a `chan []byte`.
Tail WebSocket sessions read the same log file path — no changes needed.

Both backends (`conmanConsoleIO` for conman, `sshConsoleIO` for SSH) handle reconnection
internally. The `interactiveConsoleSession` only sees a clean output channel — it never
needs to reconnect, restart goroutines, or swap the IO object.

**Package boundaries (no circular imports):**
- `internal/ssh` — `SSHConsoleManager` + `SSHConsoleNode` only; does NOT import `internal/console`
- `internal/console` — does NOT import `internal/ssh`; defines `SSHManager` interface in
  `ssh_backend.go` that `*ssh.SSHConsoleManager` satisfies implicitly; receives the manager
  via `SetupRoutes` parameter — no global, no setter function
- `cmd/remote-console` — imports both; passes `*ssh.SSHConsoleManager` to `console.SetupRoutes()`

---

## New Package: `internal/ssh`

### `internal/ssh/config.go`

```go
type SSHConfig struct {
    TerminalType         string        // default: "xterm-256color"
    TCPKeepAlive         time.Duration // default: 180s; 0 disables (matches ServerAliveInterval=180)
    ReconnectMinInterval time.Duration // default: 1s
    ReconnectMaxInterval time.Duration // default: 30s
}
```

Dead connection detection uses standard TCP keepalives set on the underlying `net.Conn`
via `net.Dialer.KeepAlive`. No application-level keepalive goroutine is needed.

No credential-fetching interface here. Credentials are fetched by `service.go` (which already
has `credsService`) and passed in as `map[string]compcredentials.CompCredentials` — the same
pattern used by `runConman → ConfigureConman`.

### `internal/ssh/node.go` — `SSHConsoleNode`

Manages one persistent SSH connection per node. Its `Run` goroutine is the sole writer
for both the log file and the client output channels, so no lock is needed for those writes.

**Key fields:**
```go
type SSHConsoleNode struct {
    nodeID  string
    info    *nodes.NodeConsoleInfo
    keyPath string
    cfg     SSHConfig

    credsMu sync.RWMutex
    creds   compcredentials.CompCredentials

    clientsMu sync.RWMutex
    clients   map[string]chan []byte  // closed and deleted from map on Detach or Run exit

    connMu    sync.Mutex        // protects sshClient and stdin together
    sshClient *gossh.Client     // nil when disconnected
    stdin     io.WriteCloser    // nil when disconnected

    // logFile is opened at the start of Run and only written from the Run goroutine.
    // reopenLogCh (depth 1) signals broadcast() to reopen the file after log rotation.
    logFile     *os.File
    logPath     string           // logsPath + "/console." + nodeID
    reopenLogCh chan struct{}     // buffered, depth 1

    // cancel is called by the manager to stop the Run goroutine.
    // ctx is NOT stored in the struct (go vet antipattern); it is only used as
    // a local variable inside Run() and passed into connect().
    cancel context.CancelFunc
}
```

**`Run(ctx context.Context)` — main loop:**
```
1. Open log file at logPath (O_WRONLY|O_CREATE|O_APPEND, 0644); log error and continue
   with logFile=nil if it fails (broadcast silently skips log write when nil)
2. defer: close logFile; under clientsMu.Lock() close and delete all client channels
   (serialises with Detach() — whichever runs first closes and removes; the other is a no-op)
3. Loop:
   a. Check ctx.Done() — if cancelled, return
   b. Check nodes.IsCurrentNode(nodeID) — if gone, return
   c. Call connect(ctx) — attempt one SSH connection
   d. If connect succeeds: read SSH stdout in a tight loop, call broadcast() for each chunk
   e. On read exit: write disconnect marker to log; exponential backoff; go to (a)
```
Step 2's deferred close-and-delete of all client channels causes attached `streamOutput`
goroutines to see `ok=false` and exit cleanly.

**`connect(ctx context.Context) (io.Reader, error)`**

```
1. Read creds under credsMu
2. Build gossh.ClientConfig (see Auth section)
3. Dial using net.Dialer with TCPKeepAlive set, then upgrade to SSH:
       netConn, err := (&net.Dialer{KeepAlive: cfg.TCPKeepAlive}).DialContext(ctx, "tcp", host:port)
       sshConn, chans, reqs, err := gossh.NewClientConn(netConn, host:port, config)
       client := gossh.NewClient(sshConn, chans, reqs)
   Port defaults to 22 if ConnectionPort == 0. TCP keepalives are handled by the OS —
   no application-level keepalive goroutine needed.
4. defer cleanup: on any failure, call client.Close(), call stdin.Close() if non-nil,
   clear sshClient/stdin under connMu
5. Acquire connMu; store sshClient; release connMu
6. client.NewSession()
7. session.RequestPty(terminalType, 80, 24, gossh.TerminalModes{
       gossh.ECHO:             1,
       gossh.TTY_OP_ISPEED:    115200,  // matches conman seropts="115200,8n1"
       gossh.TTY_OP_OSPEED:    115200,
   })
8. stdout = session.StdoutPipe(); stdin = session.StdinPipe()
   (both must be called before Shell/Start)
9. Acquire connMu; store stdin; release connMu
10. Entry command dispatch:
    - ConsoleEntryCommand == "": session.Shell()      — interactive shell
    - ConsoleEntryCommand != "": session.Start(nci.ConsoleEntryCommand)
      SSH runs the command directly (equivalent to `ssh -t user@host <cmd>`)
      No base64 decoding needed — ConsoleEntryCommand is stored as a plain string.
      The base64 in the legacy expect scripts was a conman.conf transport artefact
      (conman can't handle special characters in quoted shell arguments); it does not
      apply here.
11. Write connect marker to log
12. return stdout, nil
```

On any failure in steps 4–11 the deferred cleanup closes client+stdin and clears `connMu`
fields.

**`broadcast(data []byte)` — called only from Run goroutine:**
1. Check `reopenLogCh` with a non-blocking select; if a signal is pending, close and reopen
   `logFile` by name (sole writer, no lock needed; channel depth-1 debounces multiple signals)
2. Write data to `logFile` if non-nil; ignore write errors (best-effort logging)
3. Acquire `clientsMu.RLock()`; for each client chan: non-blocking send; drop and log if full; release

**`Attach(clientID string) chan []byte`:**
Creates a buffered channel (depth 64) in `clients` under `clientsMu.Lock()`, returns it.
Never returns an error — the manager guards against unknown nodes before calling this.

**`Detach(clientID string)`:**
Under `clientsMu.Lock()`: close and delete the channel. No-op if not found (idempotent).

**`Write(p []byte) (int, error)`:**
Acquires `connMu`; writes to `stdin`; if `stdin` is nil, returns `len(p), nil` (pretends
success). Returning `0, nil` would confuse callers into thinking zero bytes were written.
Silent drop prevents `streamInput` from closing the session during transient reconnect.

**`UpdateCreds(creds compcredentials.CompCredentials)`:**
Acquires `credsMu`, updates creds, releases. Then acquires `connMu`, reads `sshClient`,
releases `connMu`, calls `sshClient.Close()` if non-nil — causes Run goroutine's stdout
read to error, triggering a reconnect with the new credentials.

**`ReopenLog()`:**
Non-blocking send on `reopenLogCh`. Run goroutine receives this, closes current `logFile`,
reopens by name — handles log rotation correctly without a lock.

**Auth in `connect()`** — `gossh.ClientConfig` always sets `User: creds.Username` (the
username comes from the credential store, not from `NodeConsoleInfo` — matching the
existing comment in `conman.go`: "Key based auth, note that we still use the username
from the secure store"). The `Auth` field is populated using one of three modes, checked
in order:

1. **Certificate + key**: `keyPath` exists AND `keyPath+"-cert.pub"` exists →
   `gossh.ParsePrivateKey()` + `gossh.ParsePublicKey()` + `gossh.NewCertSigner()` →
   `gossh.PublicKeys(certSigner)`
2. **Key only**: `keyPath` exists, no cert → `gossh.PublicKeys(signer)`
3. **Password**: `creds.Password != ""` → `gossh.Password(creds.Password)`

This matches what `EnsureConsoleKeysPresent` writes: private key at `keyPath`, optional
certificate at `keyPath+"-cert.pub"`.

**Host key verification:**
`gossh.InsecureIgnoreHostKey()` — matches current expect script (`StrictHostKeyChecking=no`),
appropriate for BMC management networks. Deliberate choice; future enhancement could add
per-node host-key pinning.

**Log format:**
`\n[Console <nodeID> connected at <RFC3339>]\n` on connect.
`\n[Console <nodeID> disconnected at <RFC3339>]\n` on disconnect.
Raw SSH output bytes written between markers. Keeps tail mode working unchanged.

### `internal/ssh/manager.go` — `SSHConsoleManager`

```go
type SSHConsoleManager struct {
    cfg      SSHConfig
    keyPath  string
    logsPath string           // e.g. /var/log/conman/conman
    nodesMu  sync.RWMutex
    nodes    map[string]*SSHConsoleNode
}

func NewSSHConsoleManager(cfg SSHConfig, keyPath, logsPath string) *SSHConsoleManager
```

**Lock ordering** — must be respected everywhere to prevent deadlocks:
```
nodesMu → clientsMu   (manager acquires nodesMu, then node acquires clientsMu)
nodesMu → connMu      (manager acquires nodesMu, then node acquires connMu)
credsMu is independent (never held while acquiring any other lock)
```
Note: `connMu` now only protects `sshClient` and `stdin` — `sshSession` was removed when
`WindowResize` was dropped from scope.

**Key methods:**

`UpdateNodes(ctx context.Context, sshNodes map[string]*nodes.NodeConsoleInfo, passwords map[string]compcredentials.CompCredentials) error`
Diff against `nodes` map. For new nodes: create `nodeCtx, nodeCancel := context.WithCancel(ctx)`,
initialise node with creds from `passwords[nodeID]`, store `nodeCancel` in the node,
start `go node.Run(nodeCtx)`.
For removed nodes: `node.cancel()`. For changed nodes (host/port/entryCommand): cancel old,
create+start new. For unchanged nodes where passwords differ: call `node.UpdateCreds(newCreds)`.
Context passed here must be the long-lived service context — node goroutines live as long
as the service does.

`UpdateCredentials(passwords map[string]compcredentials.CompCredentials)`
For each node where creds in `passwords` differ from stored creds, call `node.UpdateCreds(newCreds)`.
No ctx, no fetch — passwords are pre-fetched by the caller.

`ReopenLogs()`
Iterates all nodes, calls `node.ReopenLog()`. Called by `logRotate` after log rotation.

`Attach(nodeID, clientID string) (chan []byte, error)`
Finds node, calls `node.Attach(clientID)`. Returns error if node unknown.

`Detach(nodeID, clientID string)` — delegates to `node.Detach(clientID)`.

`Write(nodeID string, p []byte) (int, error)` — delegates to `node.Write(p)`.

---

## `consoleIO` Interface and Backends (all in `internal/console/`)

### `consoleIO` interface (`internal/console/consoleio.go`)

Both backends handle reconnection internally. The interface is uniform and simple:

```go
// consoleIO abstracts the underlying console backend (PTY/conman or SSH).
// Implementations handle reconnection; Output() is closed only on permanent shutdown.
type consoleIO interface {
    Output() <-chan []byte        // closed on permanent shutdown
    Write(p []byte) (n int, err error)
    Close() error                 // must be idempotent (safe to call multiple times)
}
```

### `conmanConsoleIO` — conman PTY backend (`internal/console/conman_backend.go`)

Extracted from `interactiveConsoleSession`. Encapsulates all conman-specific logic including
reconnection:

```go
type conmanConsoleIO struct {
    nodeID    string
    mu        sync.Mutex
    cmd       *exec.Cmd
    ptmx      *os.File
    out       chan []byte
    ctx       context.Context
    cancel    context.CancelFunc
    closeOnce sync.Once
}
```

**Internal goroutine** started by `newConmanConsoleIO(nodeID string) (*conmanConsoleIO, error)`:
Returns immediately with a non-nil error only if the node is unknown at construction time
(sanity check). Otherwise the goroutine starts and any later `startConmanProcess` failures
are handled inside the loop.
```
Loop:
  1. Check ctx.Done() — if cancelled: close(out) and return
  2. startConmanProcess(): exec.Command("conman", nodeID) + pty.Start(); store under mu.
     MUST also launch a goroutine calling cmd.Wait() to reap the child process — without
     it, exited conman processes become zombies (matches existing startConmanProcess logic).
  3. Read loop: waitForPTYReadable + ptmx.Read; send chunks to out channel
  4. On read error (process exited):
     a. Send "\n[Reconnecting to <nodeID>...]\n" to out
     b. Wait 1s, checking ctx.Done()
     c. Check nodes.IsCurrentNode(nodeID) — if gone: close(out) and return
     d. Go to step 1
```
Step 1 before startConmanProcess ensures Close() leaves no zombie conman processes.

**`Close()`** — idempotent via `closeOnce`:
- Acquires `mu`; if ptmx is non-nil: sends `&.` escape, closes ptmx, sets ptmx to nil;
  if cmd.Process is non-nil: signals SIGTERM; releases `mu`
- Calls `cancel()` — goroutine sees ctx.Done() on next step-1 check, closes `out`, exits

`Output() <-chan []byte` → returns `out`

`Write(p []byte)` — acquires `mu`; writes to ptmx. Returns nil if ptmx is nil.

### `sshConsoleIO` — SSH backend adapter (`internal/console/ssh_backend.go`)

A narrow exported interface decouples `internal/console` from `internal/ssh` entirely —
neither package imports the other. `*ssh.SSHConsoleManager` satisfies it implicitly.
Exported because it appears as a parameter to `SetupRoutes`.

```go
// SSHManager is satisfied by *ssh.SSHConsoleManager without importing that package.
type SSHManager interface {
    Attach(nodeID, clientID string) (chan []byte, error)
    Detach(nodeID, clientID string)
    Write(nodeID string, p []byte) (int, error)
}

type sshConsoleIO struct {
    nodeID    string
    clientID  string
    out       chan []byte
    manager   SSHManager
    closeOnce sync.Once
}

func newSSHConsoleIO(nodeID string, manager SSHManager) (*sshConsoleIO, error) {
    clientID := generateClientID()  // atomic counter or similar
    ch, err := manager.Attach(nodeID, clientID)
    if err != nil {
        return nil, err
    }
    return &sshConsoleIO{nodeID: nodeID, clientID: clientID, out: ch, manager: manager}, nil
}
```

- `Output()` → `s.out`
- `Write(p)` → `s.manager.Write(s.nodeID, p)`
- `Close()` — idempotent via `closeOnce`: calls `s.manager.Detach(s.nodeID, s.clientID)`

---

## Changes to Existing Files

### `internal/console/interactive.go`

**Struct — replace conman-specific fields with single `consoleIO`:**
```go
type interactiveConsoleSession struct {
    io          consoleIO
    nodeID      string
    cancel      context.CancelFunc
    ws          *webSocketSession
    rateLimiter *ratelimiter.LeakyBucket
    wg          sync.WaitGroup
}
```

**`close()`** — must do three things (all idempotent):
1. `s.io.Close()` — shuts down the backend (SSH detach or PTY SIGTERM)
2. `s.cancel()` — signals `sessionCtx.Done()` to both goroutines
3. `s.ws.close(sessionCloseNormal, "")` — sends a WebSocket close frame via `writePump`,
   which closes the underlying TCP connection, which unblocks `streamInput`'s
   `ws.conn.ReadMessage()` call. Without this, `streamInput` hangs and `wg.Wait()` never returns.

`s.io.Close()` is idempotent (both backends use `sync.Once`). `s.cancel()` and
`s.ws.close()` are also safe to call multiple times.

**`streamOutput()`** — replace PTY-fd polling with channel select:
```go
func (s *interactiveConsoleSession) streamOutput(ctx context.Context) {
    defer s.wg.Done()
    for {
        select {
        case <-ctx.Done():
            return
        case <-s.ws.Done():
            return
        case data, ok := <-s.io.Output():
            if !ok {
                s.close()
                return
            }
            // existing rate limit + ws.Write logic unchanged
        }
    }
}
```

**`streamInput()`** — replace `s.ptmx.Write()` with `s.io.Write()`. Remove `ptmxMutex`
lock/unlock. Remove `closeWithReason` on write error — both backends return `len(p), nil`
silently when disconnected, so write errors no longer indicate a fatal session problem.

**Delete from `interactive.go`:** `monitorProcess()`, `reconnect()`, `startConmanProcess()`,
all `ptmxMutex` usage, `processExited` field.

**Move to `conman_backend.go`** (do NOT delete): `waitForPTYReadable()`, `isEIO()` — both are
still needed by the `conmanConsoleIO` read loop.

**`Start()`** — creates the session context, launches both goroutines, waits:
```go
func (s *interactiveConsoleSession) Start() {
    sessionCtx, cancel := context.WithCancel(context.Background())
    s.cancel = cancel
    s.ws.Start()   // starts writePump goroutine
    s.wg.Add(2)
    go s.streamInput(sessionCtx)
    go s.streamOutput(sessionCtx)
    s.wg.Wait()
}
```
`s.wg.Wait()` returns when both goroutines exit — guaranteed because `close()` sends a
WebSocket close frame (unblocks `streamInput`) and cancels `sessionCtx` (unblocks `streamOutput`).

**`newInteractiveConsoleSession()`** — add `cio consoleIO` parameter:
```go
func newInteractiveConsoleSession(nodeID string, conn *websocket.Conn, cio consoleIO) *interactiveConsoleSession
```

**`SetupRoutes`** — add `sshMgr SSHManager` parameter; capture in handler closure:
```go
func SetupRoutes(conmanLogsPath string, sshMgr SSHManager) *mux.Router {
    // ...
    router.HandleFunc("/console/{nodeID}/interactive", func(w http.ResponseWriter, r *http.Request) {
        doInteractiveConsole(w, r, sshMgr)
    })
    // ...
}
```
No global, no `SetSSHManager`. The manager is wired at startup and flows down the call
chain. `*ssh.SSHConsoleManager` satisfies `SSHManager` implicitly at the call site in
`service.go`.

**`doInteractiveConsole`** — add `sshMgr SSHManager` parameter; backend selection before
WebSocket upgrade:
```go
func doInteractiveConsole(w http.ResponseWriter, r *http.Request, sshMgr SSHManager) {
    node := nodes.CurrentNodes()[nodeID]
    useSSH := node != nil && node.IsSSH()

    // ...existing IsCurrentNode check and WebSocket upgrade...

    var cio consoleIO
    var err error
    if useSSH {
        cio, err = newSSHConsoleIO(nodeID, sshMgr)
    } else {
        cio, err = newConmanConsoleIO(nodeID)
    }
    if err != nil {
        // send error via WebSocket close frame and return (upgrade already done)
    }
    session := newInteractiveConsoleSession(nodeID, conn, cio)
```
No nil guard needed — `sshMgr` is always the concrete manager passed from `runService`.

### `internal/conman/conman.go`

Skip SSH nodes in `updateConfigFile()`:
```go
case nodes.SSH:
    continue  // managed by SSHConsoleManager
```

Delete `generateSSHConsoleConfig()` entirely — it becomes dead code once all SSH nodes
are skipped. Keeping it would confuse future readers.

Count only IPMI nodes for `hasNodes` so `runConman` doesn't spin when all nodes are SSH:
```go
return ipmiNodeCount > 0, nil
```

`ConfigureConman` still receives all nodes (IPMI + SSH) from the caller. The SSH filtering
happens inside `updateConfigFile`. Do not change the `ConfigureConman` signature —
`watchForNodesUpdates` passes all nodes, and the conman service ignores SSH ones internally.

Note: `UpdateLogRotateConf` (in `internal/logs`) also receives all nodes and should continue
to do so — logrotate must monitor SSH console log files too, so that `LogRotate()` returns
true (triggering `sshService.ReopenLogs()`) when SSH log files are rotated.

### `cmd/remote-console/service.go`

**`SSHConsoleService` interface** — management only (`Attach`/`Detach`/`Write` are accessed
via the `SSHManager` interface passed through `SetupRoutes` into the handler closure):
```go
type SSHConsoleService interface {
    UpdateNodes(ctx context.Context, nodes map[string]*nodes.NodeConsoleInfo,
        passwords map[string]compcredentials.CompCredentials) error
    UpdateCredentials(passwords map[string]compcredentials.CompCredentials)
    ReopenLogs()
}
```

**`watchForNodesUpdates()`** — add `credsService CredsService` and `sshService SSHConsoleService`
parameters. When nodes change, fetch creds for SSH nodes and update the SSH manager.
`runConman` stays IPMI-only and is not involved in SSH node lifecycle:
```go
if changed {
    conmanService.SignalConmanTERM()  // existing behaviour — restarts conman for IPMI

    sshNodes := filterSSHNodes(nodes.CurrentNodes())
    if len(sshNodes) > 0 {
        sshXNames := filterSSHXNames(sshNodes)
        passwords, err := credsService.GetPasswordsWithRetries(ctx, sshXNames, 15, 10)
        if err != nil {
            slog.Error("Failed to fetch credentials for SSH nodes", "error", err)
        }
        if err := sshService.UpdateNodes(ctx, sshNodes, passwords); err != nil {
            slog.Error("Failed to update SSH nodes", "error", err)
        }
    }
}
```
`filterSSHNodes` and `filterSSHXNames` are private helpers in `service.go`.

**`watchForCredUpdates()`** — add `sshService SSHConsoleService` parameter. When creds
change, fetch fresh passwords for SSH nodes and pass them to the SSH manager:
```go
changed, err := credsService.CheckForUpdates()
if changed {
    conmanService.SignalConmanTERM()  // existing behaviour

    sshXNames := filterSSHXNames(nodes.CurrentNodes())
    if len(sshXNames) > 0 {
        passwords, err := credsService.GetPasswordsWithRetries(ctx, sshXNames, 3, 5)
        if err != nil {
            slog.Error("Failed to fetch credentials for SSH nodes", "error", err)
        } else {
            sshService.UpdateCredentials(passwords)
        }
    }
}
```

**`runConman()`** — no changes. Remains focused on IPMI nodes only; no `sshService` parameter.

**`logRotate()`** — add `sshService SSHConsoleService` parameter; call `ReopenLogs()`
whenever console logs were rotated. `LogRotate()` returns true when any console log file
changed (covering both IPMI and SSH files, since `UpdateLogRotateConf` includes all nodes):
```go
consoleLogsRotated := logsService.LogRotate(conmanLogsPath)
if consoleLogsRotated {
    slog.Info("Log files rotated, signalling conmand and SSH manager")
    if err := conmanService.SignalConmanHUP(); err != nil {
        slog.Error("Failed to signal conman with SIGHUP", "error", err)
    }
    sshService.ReopenLogs()
}
```
Rename the local variable from `restartConman` to `consoleLogsRotated` to reflect that
it now covers SSH log files too (not just conman's).

**`runService()`** — construct SSH manager, make initial `UpdateNodes` call, then start
goroutines:
```go
sshManager := ssh.NewSSHConsoleManager(config.SSH,
    config.Creds.SshConsoleKeyPath, conmanLogsPath)

router := console.SetupRoutes(conmanLogsPath, sshManager)  // satisfies console.SSHManager

// Seed SSH manager immediately — watchForNodesUpdates only fires after the first tick.
initialSSHNodes := filterSSHNodes(nodes.CurrentNodes())
if len(initialSSHNodes) > 0 {
    initialXNames := filterSSHXNames(initialSSHNodes)
    if passwords, err := credsService.GetPasswordsWithRetries(ctx, initialXNames, 15, 10); err != nil {
        slog.Warn("Failed to fetch initial SSH credentials", "error", err)
    } else if err := sshManager.UpdateNodes(serviceCtx, initialSSHNodes, passwords); err != nil {
        slog.Error("Failed initial SSH manager node update", "error", err)
    }
}
```
Pass `sshManager` to `watchForNodesUpdates`, `watchForCredUpdates`, and `logRotate`.

**`watchForNodesUpdates()` signature** (add `credsService` and `sshService` parameters):
```go
func watchForNodesUpdates(ctx context.Context, config remoteConsoleConfig,
    httpClient *http.Client, conmanService ConmanService, logsService LogsService,
    credsService CredsService, sshService SSHConsoleService)
```

**`logRotate()` signature** (add `sshService` parameter):
```go
func logRotate(ctx context.Context, config remoteConsoleConfig,
    conmanService ConmanService, logsService LogsService, sshService SSHConsoleService)
```

### `cmd/remote-console/config.go`

Add `SSH ssh.SSHConfig` to `remoteConsoleConfig` and `DefaultConfig()`. Expose via `RCS_SSH_`
env var prefix matching the existing flag generation pattern.

### `cmd/remote-console/command.go`

Add CLI flags for `SSHConfig` fields, mirroring the conman/creds flag patterns.

### `internal/nodes/node_console_info.go`

```go
func (n *NodeConsoleInfo) IsSSH() bool {
    return n.ConnectionType == SSH
}
```

---

## New Files

| File | Contents |
|---|---|
| `internal/ssh/config.go` | `SSHConfig` |
| `internal/ssh/node.go` | `SSHConsoleNode` |
| `internal/ssh/manager.go` | `SSHConsoleManager` |
| `internal/console/consoleio.go` | `consoleIO` interface |
| `internal/console/conman_backend.go` | `conmanConsoleIO` (extracted from `interactive.go`) |
| `internal/console/ssh_backend.go` | `sshConsoleIO` adapter |

---

## Files Unchanged

- `internal/console/tail.go` — reads same log file path; no changes needed
- `internal/console/router.go` — HTTP routing unchanged
- `internal/console/websocket.go` — WebSocket session management unchanged
- `internal/nodes/nodes.go` — SMD discovery unchanged
- `internal/creds/` — credential retrieval unchanged
- `internal/logs/` — log rotation unchanged; still rotates same file paths
- `internal/conman/zombies.go` — zombie killer unchanged; SSH uses goroutines only

## Files to Delete

- `scripts/ssh-key-console` — replaced by `SSHConsoleNode` key/cert auth
- `scripts/ssh-pwd-console` — replaced by `SSHConsoleNode` password auth

## Known Limitations

**Initial PTY window size:** The persistent SSH connection requests a PTY at 80×24. Window
resize is not in scope for this iteration — PTY dimensions are fixed for the lifetime of
the connection. This matches the existing expect scripts, which also did not negotiate
terminal dimensions. Follow-up work can add resize support by extending the `SSHManager`
and `consoleIO` interfaces with a `Resize(rows, cols uint32)` method.

---

## Test Strategy

The existing test suite has good coverage and the implementation should make it pass.
**Avoid modifying tests unless the behaviour being tested has genuinely changed.**

### Tests that require changes (unavoidable)

**`internal/conman/conman_test.go` — `TestConfigureConman`**
Currently asserts that SSH nodes appear in the generated conman.conf:
```
console name="x0c0s2b0" dev="/usr/bin/ssh-key-console ..."
console name="x0c0s3b0" dev="/usr/bin/ssh-pwd-console ..."
```
After our change these lines will be absent (SSH nodes are skipped in `updateConfigFile`).
The expected string must be updated to include only the IPMI node. This is a direct test
of the behaviour change — the update is intentional and the diff tells the story clearly.

**`test/interactive_test.go` — `TestConsoleInteractiveReconnect`**
This test:
1. Connects to `consoleFixtures["ssh-password"]` (nodeID `x0c0s0b0`, an SSH node)
2. Adds a new SSH node to trigger a conman restart
3. Waits for `[Reconnecting to x0c0s0b0...]` then `<ConMan> Connection to console [x0c0s0b0] opened`

Both assertions will fail after our change:
- SSH nodes no longer route through conman, so adding a new SSH node does not trigger a
  conman restart that would disconnect the existing SSH console
- The reconnect banner is now `[Console <nodeID> connected at <RFC3339>]`, not the conman banner

The test must be redesigned to exercise the new reconnect mechanism: stop the SSH container
for `x0c0s0b0`, wait for `[Console x0c0s0b0 disconnected at ...]`, restart the SSH container,
and wait for `[Console x0c0s0b0 connected at ...]`.

### Tests that should pass unchanged

- `TestConsoleInteractive` — tests the WebSocket API end-to-end; no change in observable
  behaviour for SSH nodes
- `TestConsoleInteractiveTail` — same; tail reads from the log file which we still write
- `TestConsoleInteractiveClientClose` — client disconnect/reconnect behaviour is unchanged
- `TestConsoleInteractiveServerClose` — node removal → WebSocket close behaviour unchanged
- `TestConsoleInteractiveInvalidNode` — 404 for unknown nodes unchanged
- All tail tests (`test/tail_test.go`) — `readyLogMarker` for SSH fixtures is
  `"Welcome to OpenSSH Server"` which comes from the SSH server's MOTD, written as raw
  output by our implementation just as conman did; for IPMI, `<ConMan> Console [node] connected`
  still applies since IPMI nodes remain on conman
- All logrotate tests (`test/logrotate_test.go`) — same log file paths, same rotation trigger
- All credential tests (`test/credentials_test.go`)
- All unit tests outside `conman_test.go` — none touch the changed code paths

---

## Verification

1. **Unit tests** (`internal/ssh/node_test.go`):
   - Mock SSH server via `golang.org/x/crypto/ssh` server API + local `net.Listener`
   - Connect/disconnect/reconnect lifecycle; verify markers written to log file
   - Fan-out: two attached channels both receive identical data
   - Slow client: full channel causes data drop, not deadlock in broadcast
   - Dead connection: close the underlying net.Conn; verify Run() detects the error and reconnects
   - Cert auth: mock server requires certificate; verify `CertSigner` is used
   - Node removed during reconnect: verify all client channels are closed cleanly
   - Log rotation: `ReopenLog()` causes node to write to new file after rotation

2. **Unit tests** (`internal/console/conman_backend_test.go`):
   - `Close()` is idempotent (safe to call twice)
   - Node removed: output channel closes, goroutine exits without zombie conman process

3. **Integration** (`integration_test/`):
   - SSH-type node pointing at a containerised SSH target
   - Two simultaneous WebSocket clients receive the same output
   - Input from either client reaches the remote host
   - Force-drop the SSH connection; verify reconnect and output resumption
   - Output present in `/var/log/conman/conman/console.<nodeID>` during and after session
   - Entry command node: `session.Start(cmd)` path exercised end-to-end

4. **IPMI regression:**
   - IPMI nodes appear in `conman.conf`; SSH nodes do not
   - Mixed IPMI+SSH inventory: conman starts correctly for IPMI-only config

5. **Tail mode:**
   - Tail on an SSH node returns log file content written by the SSH manager

6. **Credential rotation:**
   - Update credentials; verify SSH node reconnects with new creds within one monitor interval

7. **Certificate auth:**
   - Node requires cert-signed key; verify connection succeeds using `keyPath+"-cert.pub"`

8. **Log rotation:**
   - Trigger log rotation; verify SSH nodes reopen log files and continue writing correctly
