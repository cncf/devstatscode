//! File reading with the `/<project>/` → `/shared/` fallback — port of `io.go`.

use std::fmt;
use std::path::Path;

use crate::context::Ctx;
use crate::error::go_io_error_string;
use crate::log::printf;

/// A file error rendered the way Go's `os.PathError` prints
/// (`open <path>: no such file or directory`).
#[derive(Debug)]
pub struct FileError {
    pub op: &'static str,
    pub path: String,
    pub source: std::io::Error,
}

impl fmt::Display for FileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} {}: {}",
            self.op,
            self.path,
            go_io_error_string(&self.source)
        )
    }
}

impl std::error::Error for FileError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

/// Read `path` like Go `ioutil.ReadFile`, reporting errors Go-style.
///
/// Go's `os.ReadFile` opens the file first (so `open <path>: …` for a missing
/// or unreadable path) and only fails at the first `Read` for a directory,
/// hence `read <dir>: is a directory`.
pub fn read_file_raw<P: AsRef<Path>>(path: P) -> Result<Vec<u8>, FileError> {
    let p = path.as_ref();
    std::fs::read(p).map_err(|e| FileError {
        op: if e.kind() == std::io::ErrorKind::IsADirectory {
            "read"
        } else {
            "open"
        },
        path: p.to_string_lossy().into_owned(),
        source: e,
    })
}

/// Read any file, falling back to the same path with `/<ctx.project>/`
/// replaced by `/shared/`, so files can be shared between projects.
pub fn read_file(ctx: &Ctx, path: &str) -> Result<Vec<u8>, FileError> {
    let first = read_file_raw(path);
    if first.is_ok() || ctx.project.is_empty() {
        if ctx.debug > 0 {
            printf(&format!("lib.ReadFile('{}'): ok\n", path));
        }
        return first;
    }
    let shared = path.replace(&format!("/{}/", ctx.project), "/shared/");
    let second = read_file_raw(&shared);
    match &second {
        Ok(_) => {
            if ctx.debug > 0 {
                printf(&format!("lib.ReadFile('{}'): ok\n", shared));
            }
        }
        Err(e) => {
            printf(&format!("lib.ReadFile('{}'): error: {}\n", shared, e));
        }
    }
    second
}

/// Go `os.Hostname()`: the kernel's host name (`gethostname(2)`, the same
/// source as `/proc/sys/kernel/hostname` on Linux and `kern.hostname` on
/// FreeBSD).
pub fn hostname() -> Result<String, String> {
    let mut buf = vec![0u8; 256];
    // SAFETY: `buf` is a valid, writable buffer of the given length for the
    // duration of the call; `gethostname` NUL-terminates within it (or fails).
    let rc = unsafe { libc::gethostname(buf.as_mut_ptr() as *mut libc::c_char, buf.len()) };
    if rc != 0 {
        return Err(go_io_error_string(&std::io::Error::last_os_error()));
    }
    let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    Ok(String::from_utf8_lossy(&buf[..end]).into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn falls_back_to_shared() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::create_dir_all(root.join("metrics/shared")).unwrap();
        std::fs::create_dir_all(root.join("metrics/proj")).unwrap();
        std::fs::write(root.join("metrics/shared/x.sql"), b"shared").unwrap();
        std::fs::write(root.join("metrics/proj/y.sql"), b"own").unwrap();
        let ctx = Ctx {
            project: "proj".to_string(),
            ..Ctx::default()
        };
        let base = root.to_string_lossy();
        assert_eq!(
            read_file(&ctx, &format!("{}/metrics/proj/y.sql", base)).unwrap(),
            b"own"
        );
        assert_eq!(
            read_file(&ctx, &format!("{}/metrics/proj/x.sql", base)).unwrap(),
            b"shared"
        );
        let err = read_file(&ctx, &format!("{}/metrics/proj/z.sql", base)).unwrap_err();
        assert_eq!(
            err.to_string(),
            format!(
                "open {}/metrics/shared/z.sql: no such file or directory",
                base
            )
        );
        let no_proj = Ctx::default();
        let err = read_file(&no_proj, &format!("{}/metrics/proj/x.sql", base)).unwrap_err();
        assert!(err.to_string().starts_with("open "));
        assert!(err.to_string().ends_with("no such file or directory"));
    }
}
