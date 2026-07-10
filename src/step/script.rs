use std::process::Stdio;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};

use crate::directive::Directive;
use crate::envelope::Envelope;
use crate::step::StepError;

/// Hard ceiling on a single worker response frame's declared length, checked
/// against the `u32-le` length prefix BEFORE allocating the payload buffer.
/// The prefix is untrusted input (a buggy worker, or one accidentally writing
/// its frames big-endian, can put a wildly wrong value there); without this
/// cap a garbage prefix like `0xFFFFFFxx` would drive a multi-GiB
/// `vec![0; resp_len]` allocation and OOM the gateway before the (equally
/// doomed) `read_exact` ever failed. 64 MiB is far above any legitimate
/// directive frame yet small enough that a bogus prefix fails fast as a
/// `StepError` instead of exhausting memory.
const MAX_WORKER_FRAME_BYTES: usize = 64 * 1024 * 1024;

/// A `type = "script"`, `script_mode = "oneshot"` step: a fresh subprocess is
/// spawned per invocation, the envelope is written to its stdin (then stdin
/// is closed so the child sees EOF), and its stdout is parsed as a
/// [`Directive`] — the subprocess equivalent of [`crate::step::url::UrlTransform`].
pub struct ScriptOneshot {
    pub cmd: Vec<String>,
    pub timeout: Duration,
}

impl ScriptOneshot {
    pub fn new(cmd: Vec<String>, timeout_ms: u64) -> Self {
        Self {
            cmd,
            timeout: Duration::from_millis(timeout_ms),
        }
    }

    /// Spawn `cmd[0]` with args `cmd[1..]`, then CONCURRENTLY write the
    /// envelope's JSON to its stdin (closing stdin on completion so the
    /// child observes EOF) and read stdout/stderr to completion — all
    /// bounded by `self.timeout`. A timeout kills the child and returns
    /// [`StepError::Timeout`]; non-empty stderr is logged via
    /// `tracing::warn!` but never fails the step on its own.
    ///
    /// The write and the reads MUST run concurrently rather than
    /// sequentially (write-to-completion, then read): once the envelope
    /// exceeds the OS pipe buffer (commonly ~64KiB) and the child writes
    /// enough of its own output before it has drained stdin, a
    /// write-then-read ordering deadlocks — the gateway blocks trying to
    /// finish writing stdin (the child isn't reading it, its stdout pipe is
    /// now full) while the child blocks trying to finish writing stdout (the
    /// gateway isn't reading it yet). Driving `stdin.write_all` and both
    /// `read_to_end`s as one `tokio::join!` lets stdout/stderr drain while
    /// stdin is still being written, so neither side ever waits on the
    /// other's still-full pipe.
    ///
    /// Precondition (enforced at config load, see `config::load::validate`):
    /// `cmd` is non-empty.
    pub async fn run(&self, env: &Envelope) -> Result<Directive, StepError> {
        let bytes = serde_json::to_vec(env)?;

        let mut child = Command::new(&self.cmd[0])
            .args(&self.cmd[1..])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(StepError::Spawn)?;

        let mut stdin = child.stdin.take().expect("stdin piped above");
        let mut stdout = child.stdout.take().expect("stdout piped above");
        let mut stderr = child.stderr.take().expect("stderr piped above");

        let io = async {
            // A script that never reads stdin at all (it may only care about
            // its own output, e.g. a fixed-directive stub) will exit and
            // close its end of the stdin pipe out from under us, so
            // `write_all` can legitimately fail with a broken-pipe error
            // once the child is gone — that is not a step failure: the
            // directive we actually need lives on stdout, which is read
            // concurrently below regardless of how the write finished. Only
            // stdout/stderr read errors are propagated as real failures.
            let write_fut = async {
                if let Err(err) = stdin.write_all(&bytes).await {
                    tracing::debug!(
                        "script stdin write did not complete (child likely exited without \
                         reading it): {err}"
                    );
                }
                // Drop (close) stdin so the child observes EOF and can finish
                // reading/producing its output.
                drop(stdin);
            };

            let mut out_buf = Vec::new();
            let mut err_buf = Vec::new();
            let (_, out_res, err_res) = tokio::join!(
                write_fut,
                stdout.read_to_end(&mut out_buf),
                stderr.read_to_end(&mut err_buf),
            );
            out_res?;
            err_res?;
            Ok::<(Vec<u8>, Vec<u8>), std::io::Error>((out_buf, err_buf))
        };

        let (out_buf, err_buf) = match tokio::time::timeout(self.timeout, io).await {
            Ok(Ok(bufs)) => bufs,
            Ok(Err(err)) => {
                let _ = child.kill().await;
                return Err(StepError::Spawn(err));
            }
            Err(_elapsed) => {
                let _ = child.kill().await;
                let _ = child.wait().await;
                return Err(StepError::Timeout);
            }
        };

        // Reap the child now that its stdout/stderr have both hit EOF.
        let _ = child.wait().await;

        if !err_buf.is_empty() {
            tracing::warn!("script stderr: {}", String::from_utf8_lossy(&err_buf));
        }

        let directive: Directive = serde_json::from_slice(&out_buf)?;
        Ok(directive)
    }
}

/// A `type = "script"`, `script_mode = "worker"` step: a single long-lived
/// subprocess the gateway talks to repeatedly over its stdin/stdout, one
/// length-prefixed frame per call (M17). Unlike [`ScriptOneshot`] — which
/// spawns a fresh process per invocation and signals end-of-input by closing
/// stdin — the worker process persists across calls, so each envelope is
/// delimited by an explicit `u32` little-endian length prefix rather than by
/// EOF. This is what lets a script run on the `on_stream` hook (a directive
/// per chunk) without paying a process spawn per chunk.
///
/// Wire protocol, per call:
/// - request:  `[u32-le length][length bytes of envelope JSON]` written to the
///   child's stdin, then flushed.
/// - response: `[u32-le length][length bytes of directive JSON]` read back from
///   the child's stdout; the bytes are parsed as a [`Directive`].
///
/// The whole call is bounded by `timeout`. On timeout the child is killed and
/// [`StepError::Timeout`] is returned. If the child has died (broken pipe on
/// the write, or EOF / a short read on the response) the call returns a
/// [`StepError`] so the CALLER can discard this worker and respawn — the
/// worker never silently retries or resurrects itself.
///
/// stderr is inherited (not captured): a long-lived worker whose stderr pipe
/// is never drained would eventually block once that pipe fills, so — unlike
/// the oneshot, which reads stderr to EOF each run — the worker lets its
/// diagnostics flow to the gateway's own stderr.
pub struct ScriptWorker {
    child: Child,
    stdin: ChildStdin,
    stdout: ChildStdout,
    timeout: Duration,
}

impl ScriptWorker {
    /// Spawn `cmd[0]` with args `cmd[1..]` as a long-lived worker, piping its
    /// stdin/stdout (stderr inherited — see the type docs) and returning a
    /// handle ready for [`ScriptWorker::call`]. `kill_on_drop` guarantees the
    /// child is reaped when the caller drops this handle (e.g. after a
    /// died-mid-session error prompts a respawn).
    ///
    /// Precondition (enforced at config load, see `config::load::validate`):
    /// `cmd` is non-empty.
    pub fn spawn(cmd: &[String], timeout_ms: u64) -> Result<Self, StepError> {
        let mut child = Command::new(&cmd[0])
            .args(&cmd[1..])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .map_err(StepError::Spawn)?;

        let stdin = child.stdin.take().expect("stdin piped above");
        let stdout = child.stdout.take().expect("stdout piped above");

        Ok(Self {
            child,
            stdin,
            stdout,
            timeout: Duration::from_millis(timeout_ms),
        })
    }

    /// Write `envelope_json` as one length-prefixed frame to the worker's
    /// stdin, then read one length-prefixed frame back from its stdout and
    /// parse it as a [`Directive`]. The same worker process serves every call
    /// — two sequential `call`s reuse the one subprocess.
    ///
    /// The length prefix is a `u32` little-endian byte count; both the prefix
    /// and the payload are read with `read_exact`, so a response split across
    /// several OS reads is reassembled correctly and a truncated frame (the
    /// worker died mid-write) surfaces as an error rather than a partial parse.
    ///
    /// Errors:
    /// - [`StepError::Timeout`] if the whole exchange doesn't complete within
    ///   `self.timeout`; the child is killed before returning.
    /// - [`StepError::Spawn`] on a broken pipe (writing to a dead worker), an
    ///   EOF / short read (the worker died mid-session), or a response length
    ///   prefix over [`MAX_WORKER_FRAME_BYTES`] (a garbage/wrong-endian prefix,
    ///   rejected before allocating the payload buffer) — the caller should
    ///   respawn.
    /// - [`StepError::Decode`] if the response frame's bytes aren't a valid
    ///   [`Directive`].
    pub async fn call(&mut self, envelope_json: &[u8]) -> Result<Directive, StepError> {
        let timeout = self.timeout;
        let stdin = &mut self.stdin;
        let stdout = &mut self.stdout;

        // Frame the request, flush it, then read exactly the response frame.
        // Bounded as a whole by `timeout` below; a partial/short read on
        // either the length prefix or the payload yields an `io::Error`.
        let io = async move {
            let len = u32::try_from(envelope_json.len()).map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "envelope exceeds u32 frame length",
                )
            })?;
            stdin.write_all(&len.to_le_bytes()).await?;
            stdin.write_all(envelope_json).await?;
            stdin.flush().await?;

            let mut len_buf = [0u8; 4];
            stdout.read_exact(&mut len_buf).await?;
            let resp_len = u32::from_le_bytes(len_buf) as usize;
            if resp_len > MAX_WORKER_FRAME_BYTES {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "worker response frame length {resp_len} exceeds cap \
                         {MAX_WORKER_FRAME_BYTES}"
                    ),
                ));
            }
            let mut payload = vec![0u8; resp_len];
            stdout.read_exact(&mut payload).await?;
            Ok::<Vec<u8>, std::io::Error>(payload)
        };

        let payload = match tokio::time::timeout(timeout, io).await {
            Ok(Ok(payload)) => payload,
            Ok(Err(err)) => return Err(StepError::Spawn(err)),
            Err(_elapsed) => {
                let _ = self.child.kill().await;
                let _ = self.child.wait().await;
                return Err(StepError::Timeout);
            }
        };

        let directive: Directive = serde_json::from_slice(&payload)?;
        Ok(directive)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::envelope::build_request_envelope;
    use crate::http_msg::HttpMsg;
    use std::collections::BTreeMap;
    use std::time::Instant;

    fn msg() -> HttpMsg {
        HttpMsg {
            method: "POST".into(),
            path: "/v1/messages".into(),
            headers: BTreeMap::new(),
            body_b64: String::new(),
        }
    }

    fn env() -> Envelope {
        build_request_envelope(
            "claude",
            "script-step",
            &msg(),
            &serde_json::Map::new(),
            "corr-test",
            None,
        )
    }

    #[tokio::test]
    async fn reads_envelope_from_stdin_and_parses_continue_directive() {
        let step = ScriptOneshot::new(
            vec![
                "sh".to_string(),
                "-c".to_string(),
                "cat >/dev/null; printf '{\"action\":\"continue\",\"ops\":[]}'".to_string(),
            ],
            1000,
        );
        let directive = step.run(&env()).await.unwrap();
        match directive {
            Directive::Continue { ops } => assert!(ops.is_empty()),
            other => panic!("expected continue, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn parses_set_header_directive_from_stdout() {
        let step = ScriptOneshot::new(
            vec![
                "sh".to_string(),
                "-c".to_string(),
                "cat >/dev/null; printf '{\"action\":\"continue\",\"ops\":[{\"op\":\"set_header\",\"name\":\"x-script\",\"value\":\"seen\"}]}'".to_string(),
            ],
            1000,
        );
        let directive = step.run(&env()).await.unwrap();
        match directive {
            Directive::Continue { ops } => {
                assert_eq!(ops.len(), 1);
                match &ops[0] {
                    crate::directive::Op::SetHeader { name, value } => {
                        assert_eq!(name, "x-script");
                        assert_eq!(value, "seen");
                    }
                    other => panic!("expected set_header op, got {other:?}"),
                }
            }
            other => panic!("expected continue, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn timeout_kills_child_and_returns_promptly() {
        let step = ScriptOneshot::new(
            vec!["sh".to_string(), "-c".to_string(), "sleep 5".to_string()],
            100,
        );
        let start = Instant::now();
        let err = step.run(&env()).await.unwrap_err();
        assert!(matches!(err, StepError::Timeout));
        assert!(
            start.elapsed() < Duration::from_secs(2),
            "run() must return promptly after the timeout, took {:?}",
            start.elapsed()
        );
    }

    #[tokio::test]
    async fn malformed_stdout_is_a_decode_error() {
        let step = ScriptOneshot::new(
            vec![
                "sh".to_string(),
                "-c".to_string(),
                "cat >/dev/null; printf 'not json'".to_string(),
            ],
            1000,
        );
        let err = step.run(&env()).await.unwrap_err();
        assert!(matches!(err, StepError::Decode(_)));
    }

    #[tokio::test]
    async fn spawn_failure_for_nonexistent_binary_is_spawn_error() {
        let step = ScriptOneshot::new(vec!["/no/such/binary-xyz".to_string()], 1000);
        let err = step.run(&env()).await.unwrap_err();
        assert!(matches!(err, StepError::Spawn(_)));
    }

    /// Regression test for the write-then-read deadlock: a request body well
    /// over the ~64KiB OS pipe buffer, run against a script that ignores
    /// stdin entirely and prints its directive immediately. Under the old
    /// "write all of stdin to completion, THEN read stdout" ordering this
    /// either hangs (blocked writing stdin nobody drains) or, at best, fails
    /// with a spurious broken-pipe error once the child exits without ever
    /// reading it — instead of returning the directive the script already
    /// produced. A generous timeout bounds the test so a real regression
    /// fails fast rather than hanging the suite.
    #[tokio::test]
    async fn large_envelope_does_not_deadlock_against_a_script_that_ignores_stdin() {
        let big_body = "x".repeat(128 * 1024 + 1);
        let big_msg = HttpMsg {
            method: "POST".into(),
            path: "/v1/messages".into(),
            headers: BTreeMap::new(),
            body_b64: big_body,
        };
        let big_env = build_request_envelope(
            "claude",
            "script-step",
            &big_msg,
            &serde_json::Map::new(),
            "corr-test-large",
            None,
        );

        let step = ScriptOneshot::new(
            vec![
                "sh".to_string(),
                "-c".to_string(),
                "printf '{\"action\":\"continue\",\"ops\":[]}'".to_string(),
            ],
            5000,
        );

        let start = Instant::now();
        let directive = tokio::time::timeout(Duration::from_secs(10), step.run(&big_env))
            .await
            .expect("run() must not hang/deadlock on a large envelope")
            .expect("run() must succeed and return the script's directive");
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "run() took too long, suggesting the deadlock regressed: {:?}",
            start.elapsed()
        );
        match directive {
            Directive::Continue { ops } => assert!(ops.is_empty()),
            other => panic!("expected continue, got {other:?}"),
        }
    }

    /// Companion case to the deadlock regression test above: a script that
    /// (unlike the one above) DOES fully drain stdin before producing
    /// output, again with a large envelope — proving the concurrent-I/O fix
    /// doesn't just work by accident for scripts that ignore stdin, but also
    /// for ones that read it all.
    #[tokio::test]
    async fn large_envelope_does_not_deadlock_against_a_script_that_drains_stdin() {
        let big_body = "y".repeat(128 * 1024 + 1);
        let big_msg = HttpMsg {
            method: "POST".into(),
            path: "/v1/messages".into(),
            headers: BTreeMap::new(),
            body_b64: big_body,
        };
        let big_env = build_request_envelope(
            "claude",
            "script-step",
            &big_msg,
            &serde_json::Map::new(),
            "corr-test-large-2",
            None,
        );

        let step = ScriptOneshot::new(
            vec![
                "sh".to_string(),
                "-c".to_string(),
                "cat >/dev/null; printf '{\"action\":\"continue\",\"ops\":[]}'".to_string(),
            ],
            5000,
        );

        let start = Instant::now();
        let directive = tokio::time::timeout(Duration::from_secs(10), step.run(&big_env))
            .await
            .expect("run() must not hang/deadlock on a large envelope")
            .expect("run() must succeed and return the script's directive");
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "run() took too long, suggesting the deadlock regressed: {:?}",
            start.elapsed()
        );
        match directive {
            Directive::Continue { ops } => assert!(ops.is_empty()),
            other => panic!("expected continue, got {other:?}"),
        }
    }

    // ---- ScriptWorker (M17) ----

    /// The worker framing round-trips binary length-prefixed frames, which is
    /// awkward to do reliably in pure `sh`; we drive it from `python3`. On a
    /// host without `python3` these worker tests skip rather than fail (they
    /// exercise the framing protocol, not the interpreter).
    fn have_python3() -> bool {
        std::process::Command::new("python3")
            .arg("--version")
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
    }

    /// A worker that loops forever: read a `u32-le` length, read that many
    /// bytes, then write back a framed `continue` directive setting a header.
    fn looping_worker_cmd() -> Vec<String> {
        let prog = r#"
import sys, struct
while True:
    hdr = sys.stdin.buffer.read(4)
    if len(hdr) < 4:
        break
    n = struct.unpack('<I', hdr)[0]
    _ = sys.stdin.buffer.read(n)
    resp = b'{"action":"continue","ops":[{"op":"set_header","name":"x-worker","value":"seen"}]}'
    sys.stdout.buffer.write(struct.pack('<I', len(resp)))
    sys.stdout.buffer.write(resp)
    sys.stdout.buffer.flush()
"#;
        vec!["python3".to_string(), "-c".to_string(), prog.to_string()]
    }

    fn assert_x_worker_seen(directive: Directive) {
        match directive {
            Directive::Continue { ops } => {
                assert_eq!(ops.len(), 1);
                match &ops[0] {
                    crate::directive::Op::SetHeader { name, value } => {
                        assert_eq!(name, "x-worker");
                        assert_eq!(value, "seen");
                    }
                    other => panic!("expected set_header op, got {other:?}"),
                }
            }
            other => panic!("expected continue, got {other:?}"),
        }
    }

    /// Two sequential `call`s round-trip a framed directive on ONE spawned
    /// process — the child's pid is unchanged between calls, proving the
    /// worker persists (the entire point of worker mode).
    #[tokio::test]
    async fn worker_round_trips_twice_on_one_process() {
        if !have_python3() {
            eprintln!("skipping worker_round_trips_twice_on_one_process: python3 not available");
            return;
        }
        let mut worker = ScriptWorker::spawn(&looping_worker_cmd(), 2000).unwrap();
        let bytes = serde_json::to_vec(&env()).unwrap();

        let pid_before = worker.child.id();
        assert!(pid_before.is_some(), "worker child should be running");

        assert_x_worker_seen(worker.call(&bytes).await.unwrap());
        assert_x_worker_seen(worker.call(&bytes).await.unwrap());

        assert_eq!(
            worker.child.id(),
            pid_before,
            "both calls must be served by the SAME persistent process"
        );
    }

    /// A worker that produces exactly one framed response and then exits: the
    /// first `call` succeeds, the second surfaces a `StepError` (broken pipe
    /// writing to the dead child, or EOF reading its now-closed stdout) so the
    /// caller knows to respawn.
    #[tokio::test]
    async fn worker_that_exits_after_one_response_fails_second_call() {
        if !have_python3() {
            eprintln!(
                "skipping worker_that_exits_after_one_response_fails_second_call: python3 not \
                 available"
            );
            return;
        }
        let prog = r#"
import sys, struct
hdr = sys.stdin.buffer.read(4)
n = struct.unpack('<I', hdr)[0]
_ = sys.stdin.buffer.read(n)
resp = b'{"action":"continue","ops":[]}'
sys.stdout.buffer.write(struct.pack('<I', len(resp)))
sys.stdout.buffer.write(resp)
sys.stdout.buffer.flush()
"#;
        let cmd = vec!["python3".to_string(), "-c".to_string(), prog.to_string()];
        let mut worker = ScriptWorker::spawn(&cmd, 2000).unwrap();
        let bytes = serde_json::to_vec(&env()).unwrap();

        // First call: the single response the worker produces before exiting.
        match worker.call(&bytes).await.unwrap() {
            Directive::Continue { ops } => assert!(ops.is_empty()),
            other => panic!("expected continue, got {other:?}"),
        }

        // Second call: the worker is gone (died mid-session) -> StepError.
        let err = worker.call(&bytes).await.unwrap_err();
        assert!(
            matches!(err, StepError::Spawn(_)),
            "died-mid-session call should surface a process/IO StepError, got {err:?}"
        );
    }

    /// A worker that reads nothing and never answers: `call` must hit the
    /// timeout, kill the child, and return `StepError::Timeout` PROMPTLY (not
    /// hang the test). `sh` suffices here — no framing is involved.
    #[tokio::test]
    async fn worker_that_never_responds_times_out_and_is_killed() {
        let mut worker = ScriptWorker::spawn(
            &[
                "sh".to_string(),
                "-c".to_string(),
                "exec sleep 5".to_string(),
            ],
            150,
        )
        .unwrap();
        let bytes = serde_json::to_vec(&env()).unwrap();

        let start = Instant::now();
        let err = worker.call(&bytes).await.unwrap_err();
        assert!(
            matches!(err, StepError::Timeout),
            "expected Timeout, got {err:?}"
        );
        assert!(
            start.elapsed() < Duration::from_secs(2),
            "call() must return promptly after the timeout (child killed), took {:?}",
            start.elapsed()
        );

        // The child was killed inside call(); it must no longer be running.
        let status = worker.child.wait().await.unwrap();
        assert!(
            !status.success(),
            "killed worker should not report a clean exit"
        );
    }

    /// Spawning a nonexistent binary is a `StepError::Spawn`, mirroring the
    /// oneshot's spawn-failure contract.
    #[tokio::test]
    async fn worker_spawn_failure_for_nonexistent_binary_is_spawn_error() {
        let result = ScriptWorker::spawn(&["/no/such/binary-xyz".to_string()], 1000);
        assert!(matches!(result, Err(StepError::Spawn(_))));
    }

    /// A worker that lies about its response length — writing a `u32-le`
    /// prefix far larger than the payload it actually sends (here `0xFFFFFFF0`,
    /// ~4 GiB) — must be rejected by the frame cap BEFORE `call` tries to
    /// allocate `vec![0; resp_len]`, surfacing a `StepError` promptly rather
    /// than OOM-ing the gateway on a multi-GiB allocation. The child is killed
    /// by `kill_on_drop` when `worker` is dropped.
    #[tokio::test]
    async fn worker_oversized_response_frame_prefix_is_rejected_not_allocated() {
        if !have_python3() {
            eprintln!(
                "skipping worker_oversized_response_frame_prefix_is_rejected_not_allocated: \
                 python3 not available"
            );
            return;
        }
        // Reads one frame, then writes a bogus ~4 GiB length prefix followed by
        // a tiny payload. A correct `call` never allocates 4 GiB for this.
        let prog = r#"
import sys, struct
hdr = sys.stdin.buffer.read(4)
n = struct.unpack('<I', hdr)[0]
_ = sys.stdin.buffer.read(n)
sys.stdout.buffer.write(struct.pack('<I', 0xFFFFFFF0))
sys.stdout.buffer.write(b'x')
sys.stdout.buffer.flush()
import time
time.sleep(30)
"#;
        let cmd = vec!["python3".to_string(), "-c".to_string(), prog.to_string()];
        let mut worker = ScriptWorker::spawn(&cmd, 2000).unwrap();
        let bytes = serde_json::to_vec(&env()).unwrap();

        let start = Instant::now();
        let err = worker.call(&bytes).await.unwrap_err();
        assert!(
            matches!(err, StepError::Spawn(_)),
            "oversized frame prefix should surface an IO StepError, got {err:?}"
        );
        assert!(
            start.elapsed() < Duration::from_secs(2),
            "call() must reject the oversized prefix promptly (no giant alloc / no wait for the \
             timeout), took {:?}",
            start.elapsed()
        );
    }

    /// A large-envelope round trip against a well-behaved worker that reads the
    /// whole request frame before responding, with BOTH a >200 KiB request
    /// envelope AND a >200 KiB response payload. Proves the framed write/read
    /// (`write_all` prefix + `write_all` payload + `flush`, then `read_exact`
    /// prefix + `read_exact` payload) does not deadlock when both directions
    /// exceed the OS pipe buffer — the worker analogue of the oneshot's
    /// large-envelope deadlock regression tests above.
    #[tokio::test]
    async fn worker_large_envelope_and_response_round_trip_no_deadlock() {
        if !have_python3() {
            eprintln!(
                "skipping worker_large_envelope_and_response_round_trip_no_deadlock: python3 not \
                 available"
            );
            return;
        }
        // Reads the full request frame, then emits a `continue` directive whose
        // single `set_header` value is >200 KiB, so the RESPONSE frame is large
        // too. Payload is assembled in Python so the framed length matches
        // exactly.
        let prog = r#"
import sys, struct
hdr = sys.stdin.buffer.read(4)
if len(hdr) < 4:
    sys.exit(0)
n = struct.unpack('<I', hdr)[0]
_ = sys.stdin.buffer.read(n)
big = 'z' * (256 * 1024)
resp = ('{"action":"continue","ops":[{"op":"set_header","name":"x-big","value":"' + big + '"}]}').encode()
sys.stdout.buffer.write(struct.pack('<I', len(resp)))
sys.stdout.buffer.write(resp)
sys.stdout.buffer.flush()
"#;
        let cmd = vec!["python3".to_string(), "-c".to_string(), prog.to_string()];
        let mut worker = ScriptWorker::spawn(&cmd, 5000).unwrap();

        // A >200 KiB request envelope: a large request body base64 lands in the
        // serialized envelope JSON.
        let big_body = "q".repeat(256 * 1024);
        let big_msg = HttpMsg {
            method: "POST".into(),
            path: "/v1/messages".into(),
            headers: BTreeMap::new(),
            body_b64: big_body,
        };
        let big_env = build_request_envelope(
            "claude",
            "script-step",
            &big_msg,
            &serde_json::Map::new(),
            "corr-worker-large",
            None,
        );
        let bytes = serde_json::to_vec(&big_env).unwrap();
        assert!(
            bytes.len() > 200 * 1024,
            "request envelope should exceed 200 KiB"
        );

        let directive = tokio::time::timeout(Duration::from_secs(10), worker.call(&bytes))
            .await
            .expect("worker call must not hang/deadlock on a large round trip")
            .expect("worker call must succeed");
        match directive {
            Directive::Continue { ops } => {
                assert_eq!(ops.len(), 1);
                match &ops[0] {
                    crate::directive::Op::SetHeader { name, value } => {
                        assert_eq!(name, "x-big");
                        assert!(
                            value.len() > 200 * 1024,
                            "response value should exceed 200 KiB, was {}",
                            value.len()
                        );
                    }
                    other => panic!("expected set_header op, got {other:?}"),
                }
            }
            other => panic!("expected continue, got {other:?}"),
        }
    }
}
