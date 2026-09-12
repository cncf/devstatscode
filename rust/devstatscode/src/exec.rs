//! External command execution — port of `exec.go` (`ExecCommand`).

use std::collections::BTreeMap;
use std::fmt;
use std::io::Read;
use std::process::{Command, Stdio};
use std::time::Instant;

use crate::context::Ctx;
use crate::error::{fatal_on_error, go_io_error_string};
use crate::gofmt;
use crate::log::printf;
use crate::time::format_go_duration;

/// Command failure, rendered like Go's `exec.ExitError`/`exec.Error` messages.
#[derive(Debug, Clone, PartialEq)]
pub enum ExecError {
    /// Process exited with a non-zero status: `exit status N`.
    ExitStatus(i32),
    /// Process was terminated by a signal: `signal: killed`.
    Signal(i32),
    /// Executable not found in `$PATH`: `exec: "cmd": executable file not found in $PATH`.
    NotFound(String),
    /// Other start failure: `fork/exec <path>: <error>`.
    Start(String, String),
    /// I/O error while reading the output pipe.
    Io(String),
}

fn signal_name(sig: i32) -> String {
    let name = match sig {
        1 => "hangup",
        2 => "interrupt",
        3 => "quit",
        4 => "illegal instruction",
        5 => "trace/breakpoint trap",
        6 => "aborted",
        7 | 10 => "bus error",
        8 => "floating point exception",
        9 => "killed",
        11 => "segmentation fault",
        13 => "broken pipe",
        14 => "alarm clock",
        15 => "terminated",
        _ => return format!("signal {}", sig),
    };
    name.to_string()
}

impl fmt::Display for ExecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExecError::ExitStatus(n) => write!(f, "exit status {}", n),
            ExecError::Signal(s) => write!(f, "signal: {}", signal_name(*s)),
            ExecError::NotFound(cmd) => {
                write!(f, "exec: \"{}\": executable file not found in $PATH", cmd)
            }
            ExecError::Start(path, err) => write!(f, "fork/exec {}: {}", path, err),
            ExecError::Io(err) => write!(f, "{}", err),
        }
    }
}

impl std::error::Error for ExecError {}

fn map_string(env: &BTreeMap<String, String>) -> String {
    gofmt::map(env)
}

fn log_command(ctx: &Ctx, cmd_and_args: &[String], env: &BTreeMap<String, String>) {
    if !ctx.exec_quiet {
        let cmd = gofmt::slice(cmd_and_args);
        let env_s = map_string(env);
        printf(&format!(
            "Command, arguments, environment:\n{}\n{}\n",
            cmd, env_s
        ));
        println!("Command and arguments:\n{}\n{}", cmd, env_s);
    }
}

/// Resolve a start error into Go's `exec` error wording.
fn start_error(command: &str, err: &std::io::Error) -> ExecError {
    if err.kind() == std::io::ErrorKind::NotFound {
        if command.contains('/') {
            return ExecError::Start(command.to_string(), go_io_error_string(err));
        }
        return ExecError::NotFound(command.to_string());
    }
    if command.contains('/') || std::path::Path::new(command).exists() {
        return ExecError::Start(command.to_string(), go_io_error_string(err));
    }
    // Go resolves the binary through $PATH before forking; a permission
    // problem on the resolved path surfaces as `fork/exec <resolved>: ...`.
    let resolved = std::env::var_os("PATH")
        .and_then(|p| {
            std::env::split_paths(&p)
                .map(|dir| dir.join(command))
                .find(|c| c.is_file())
                .map(|c| c.to_string_lossy().to_string())
        })
        .unwrap_or_else(|| command.to_string());
    ExecError::Start(resolved, go_io_error_string(err))
}

fn exit_error(status: std::process::ExitStatus) -> Option<ExecError> {
    if status.success() {
        return None;
    }
    if let Some(code) = status.code() {
        return Some(ExecError::ExitStatus(code));
    }
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        if let Some(sig) = status.signal() {
            return Some(ExecError::Signal(sig));
        }
    }
    Some(ExecError::ExitStatus(-1))
}

fn truncate_middle(s: &str, max: usize, keep: usize) -> String {
    if s.len() > max && s.is_char_boundary(keep) && s.is_char_boundary(s.len() - keep) {
        format!("{}...{}", &s[..keep], &s[s.len() - keep..])
    } else {
        s.to_string()
    }
}

fn fail(ctx: &Ctx, err: ExecError, out: Vec<u8>) -> (Vec<u8>, Option<ExecError>) {
    if ctx.exec_fatal {
        fatal_on_error(err);
    }
    (out, Some(err))
}

/// Execute `cmd_and_args[0]` with the remaining arguments, optionally adding
/// `env` to the environment (Go `ExecCommand`).
///
/// Returns the captured stdout (only when `ctx.exec_output` is set) or the
/// failure. When `ctx.exec_fatal` is set any failure terminates the process
/// like Go's `FatalOnError`.
pub fn exec_command(
    ctx: &Ctx,
    cmd_and_args: &[String],
    env: &BTreeMap<String, String>,
) -> Result<String, ExecError> {
    match exec_command_go(ctx, cmd_and_args, env) {
        (out, None) => Ok(out),
        (_, Some(err)) => Err(err),
    }
}

/// [`exec_command`] returning Go's `(string, error)` pair: when the command
/// ran but failed (`cmd.Wait()` error) the captured stdout is returned
/// together with the error, like Go's `return stdOut.String(), err`.
/// Invalid UTF-8 in the output is replaced by U+FFFD; use
/// [`exec_command_bytes`] for the raw bytes.
pub fn exec_command_go(
    ctx: &Ctx,
    cmd_and_args: &[String],
    env: &BTreeMap<String, String>,
) -> (String, Option<ExecError>) {
    let (out, err) = exec_command_raw(ctx, cmd_and_args, env);
    (String::from_utf8_lossy(&out).to_string(), err)
}

/// [`exec_command`] returning the captured stdout as raw bytes — Go strings
/// are byte strings, so callers that cut or store command output byte-wise
/// (e.g. the git tag messages of `annotations`) use this one.
pub fn exec_command_bytes(
    ctx: &Ctx,
    cmd_and_args: &[String],
    env: &BTreeMap<String, String>,
) -> Result<Vec<u8>, ExecError> {
    match exec_command_raw(ctx, cmd_and_args, env) {
        (out, None) => Ok(out),
        (_, Some(err)) => Err(err),
    }
}

fn exec_command_raw(
    ctx: &Ctx,
    cmd_and_args: &[String],
    env: &BTreeMap<String, String>,
) -> (Vec<u8>, Option<ExecError>) {
    let dt_start = Instant::now();
    let pipe_size = 0x100;
    let command = &cmd_and_args[0];
    let arguments = &cmd_and_args[1..];
    if ctx.cmd_debug > 0 {
        let args: Vec<String> = cmd_and_args
            .iter()
            .map(|arg| {
                let arg = truncate_middle(arg, 0x200, 0x100);
                if arg.contains(' ') {
                    format!("'{}'", arg)
                } else {
                    arg
                }
            })
            .collect();
        printf(&format!("{}\n", args.join(" ")));
    }
    let mut cmd = Command::new(command);
    cmd.args(arguments);
    if !env.is_empty() {
        cmd.envs(env);
        if ctx.cmd_debug > 0 {
            printf(&format!("Environment Override: {}\n", map_string(env)));
            if ctx.cmd_debug > 2 {
                let mut new_env: Vec<String> = std::env::vars()
                    .map(|(k, v)| format!("{}={}", k, v))
                    .collect();
                for (k, v) in env {
                    new_env.push(format!("{}={}", k, v));
                }
                printf(&format!("Full Environment: {}\n", gofmt::slice(&new_env)));
            }
        }
    }
    cmd.stdin(Stdio::inherit());
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());

    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            log_command(ctx, cmd_and_args, env);
            return fail(ctx, start_error(command, &e), Vec::new());
        }
    };

    // stderr is drained on a helper thread so a chatty command can't block.
    let mut stderr_pipe = child.stderr.take().expect("stderr piped");
    let stderr_thread = std::thread::spawn(move || {
        let mut buf = Vec::new();
        let _ = stderr_pipe.read_to_end(&mut buf);
        buf
    });

    let mut stdout_pipe = child.stdout.take().expect("stdout piped");
    let mut std_out: Vec<u8> = Vec::new();
    if ctx.cmd_debug > 1 {
        // Stream stdout while the command runs.
        let mut buffer = vec![0u8; pipe_size];
        loop {
            match stdout_pipe.read(&mut buffer) {
                Ok(0) => break,
                Ok(n) => {
                    let chunk = &buffer[..n];
                    printf(&String::from_utf8_lossy(chunk));
                    std_out.extend_from_slice(chunk);
                }
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => {
                    log_command(ctx, cmd_and_args, env);
                    let _ = child.kill();
                    let _ = child.wait();
                    return fail(ctx, ExecError::Io(go_io_error_string(&e)), Vec::new());
                }
            }
        }
    } else {
        let _ = stdout_pipe.read_to_end(&mut std_out);
    }
    let std_err = stderr_thread.join().unwrap_or_default();
    let status = child.wait();
    let err_str = String::from_utf8_lossy(&std_err).to_string();

    let err = match status {
        Ok(st) => exit_error(st),
        Err(e) => Some(ExecError::Io(go_io_error_string(&e))),
    };
    if let Some(err) = err {
        if ctx.cmd_debug <= 1 && !std_out.is_empty() && !ctx.exec_quiet {
            printf(&format!("{}\n", String::from_utf8_lossy(&std_out)));
        }
        if !err_str.is_empty() && !ctx.exec_quiet {
            printf(&format!("STDERR:\n{}\n", err_str));
        }
        log_command(ctx, cmd_and_args, env);
        // Go returns `stdOut.String()`, which is empty when stdout was
        // streamed (`CmdDebug > 1`).
        let out = if ctx.cmd_debug <= 1 {
            std_out
        } else {
            Vec::new()
        };
        return fail(ctx, err, out);
    }
    if ctx.cmd_debug > 1 && !err_str.is_empty() {
        printf(&format!("Errors:\n{}\n", err_str));
    }
    if ctx.cmd_debug > 0 {
        let info = truncate_middle(&cmd_and_args.join(" "), 0x280, 0x140);
        printf(&format!(
            "{} ... {}\n",
            info,
            format_go_duration(dt_start.elapsed())
        ));
    }
    if ctx.exec_output {
        (std_out, None)
    } else {
        (Vec::new(), None)
    }
}

/// Go `exec.Command(cmd, args...).CombinedOutput()`: run the command and
/// return its stdout and stderr interleaved as one byte buffer (both streams
/// are connected to the same pipe, so the interleaving is the process's own).
/// On failure the output collected so far is returned together with the
/// error, worded like Go's `exec` errors.
pub fn combined_output(cmd_and_args: &[String]) -> Result<Vec<u8>, (Vec<u8>, ExecError)> {
    let command = &cmd_and_args[0];
    let (mut reader, writer) = match std::io::pipe() {
        Ok(p) => p,
        Err(e) => return Err((Vec::new(), ExecError::Io(go_io_error_string(&e)))),
    };
    let writer2 = match writer.try_clone() {
        Ok(w) => w,
        Err(e) => return Err((Vec::new(), ExecError::Io(go_io_error_string(&e)))),
    };
    let mut cmd = Command::new(command);
    cmd.args(&cmd_and_args[1..]);
    cmd.stdin(Stdio::inherit());
    cmd.stdout(Stdio::from(writer));
    cmd.stderr(Stdio::from(writer2));
    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => return Err((Vec::new(), start_error(command, &e))),
    };
    // The parent must drop its copies of the write end, or the read below
    // never sees EOF.
    drop(cmd);
    let mut out = Vec::new();
    let read = reader.read_to_end(&mut out);
    let status = child.wait();
    if let Err(e) = read {
        return Err((out, ExecError::Io(go_io_error_string(&e))));
    }
    match status {
        Ok(st) => match exit_error(st) {
            None => Ok(out),
            Some(err) => Err((out, err)),
        },
        Err(e) => Err((out, ExecError::Io(go_io_error_string(&e)))),
    }
}

/// Convenience wrapper taking `&str` arguments.
pub fn exec_command_strs(
    ctx: &Ctx,
    cmd_and_args: &[&str],
    env: &BTreeMap<String, String>,
) -> Result<String, ExecError> {
    let owned: Vec<String> = cmd_and_args.iter().map(|s| s.to_string()).collect();
    exec_command(ctx, &owned, env)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx() -> Ctx {
        Ctx {
            exec_fatal: false,
            exec_output: true,
            exec_quiet: true,
            ..Ctx::default()
        }
    }

    #[test]
    fn combined_output_merges_streams_and_reports_errors() {
        // The spawned shell inherits the process cwd, which
        // `env::tests::update_env_reads_file_in_cwd` moves to a temporary
        // directory under the same lock; with stderr merged into the result a
        // vanished cwd would show up as a `getcwd()` warning.
        let _g = crate::context::test_support::env_lock();
        let args = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        let out =
            combined_output(&args(&["sh", "-c", "echo out; echo err 1>&2; echo out2"])).unwrap();
        assert_eq!(out, b"out\nerr\nout2\n");
        let (out, err) = combined_output(&args(&["sh", "-c", "echo partial; exit 3"])).unwrap_err();
        assert_eq!(out, b"partial\n");
        assert_eq!(err.to_string(), "exit status 3");
        let (out, err) = combined_output(&args(&["no-such-binary-xyz"])).unwrap_err();
        assert!(out.is_empty());
        assert_eq!(
            err.to_string(),
            "exec: \"no-such-binary-xyz\": executable file not found in $PATH"
        );
        let (_, err) = combined_output(&args(&["/no/such/dir/bin"])).unwrap_err();
        assert_eq!(
            err.to_string(),
            "fork/exec /no/such/dir/bin: no such file or directory"
        );
    }

    #[test]
    fn runs_and_captures_output() {
        let out =
            exec_command_strs(&ctx(), &["sh", "-c", "printf 'a\\nb'"], &BTreeMap::new()).unwrap();
        assert_eq!(out, "a\nb");
    }

    #[test]
    fn output_suppressed_without_exec_output() {
        let mut c = ctx();
        c.exec_output = false;
        let out = exec_command_strs(&c, &["echo", "hi"], &BTreeMap::new()).unwrap();
        assert_eq!(out, "");
    }

    #[test]
    fn passes_environment() {
        let mut env = BTreeMap::new();
        env.insert("DEVSTATS_EXEC_TEST".to_string(), "v=1".to_string());
        let out =
            exec_command_strs(&ctx(), &["sh", "-c", "echo $DEVSTATS_EXEC_TEST"], &env).unwrap();
        assert_eq!(out, "v=1\n");
    }

    #[test]
    fn exit_status_error() {
        let err = exec_command_strs(
            &ctx(),
            &["sh", "-c", "echo out; echo err >&2; exit 3"],
            &BTreeMap::new(),
        )
        .unwrap_err();
        assert_eq!(err, ExecError::ExitStatus(3));
        assert_eq!(err.to_string(), "exit status 3");
    }

    #[test]
    fn signal_error() {
        let err =
            exec_command_strs(&ctx(), &["sh", "-c", "kill -9 $$"], &BTreeMap::new()).unwrap_err();
        assert_eq!(err, ExecError::Signal(9));
        assert_eq!(err.to_string(), "signal: killed");
    }

    #[test]
    fn missing_binary_error() {
        let err = exec_command_strs(&ctx(), &["devstats-no-such-binary-xyz"], &BTreeMap::new())
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "exec: \"devstats-no-such-binary-xyz\": executable file not found in $PATH"
        );
        let err =
            exec_command_strs(&ctx(), &["/no/such/dir/binary"], &BTreeMap::new()).unwrap_err();
        assert_eq!(
            err.to_string(),
            "fork/exec /no/such/dir/binary: no such file or directory"
        );
    }

    #[test]
    fn truncation_helper() {
        let long = "x".repeat(0x300);
        let t = truncate_middle(&long, 0x200, 0x100);
        assert_eq!(t.len(), 0x200 + 3);
        assert_eq!(truncate_middle("short", 0x200, 0x100), "short");
    }
}
