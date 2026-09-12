//! Environment helpers — port of `env.go`: `env.env` hot reload and
//! prefix/suffix based variable replacement (`DB_HOST` ← `DB_HOST_SRC` ...).

use std::collections::BTreeMap;
use std::sync::{Mutex, OnceLock};
use std::thread;
use std::time::Duration;

use crate::consts::UNSET;
use crate::log::{is_log_initialized, printf};

static ENV_MAP: Mutex<BTreeMap<String, String>> = Mutex::new(BTreeMap::new());

/// Background loop re-reading `env.env` every 30 seconds (Go `EnvSyncer`).
pub fn env_syncer() {
    loop {
        thread::sleep(Duration::from_secs(30));
        update_env(true);
    }
}

/// Start the [`env_syncer`] thread once per process.
pub fn start_env_syncer() {
    static STARTED: OnceLock<()> = OnceLock::new();
    STARTED.get_or_init(|| {
        thread::Builder::new()
            .name("env_syncer".into())
            .spawn(env_syncer)
            .expect("spawn env syncer");
    });
}

/// Apply `k=v` lines from `./env.env` (if present) to the process environment,
/// reporting changed values (via the logger when `use_log`, plain stdout
/// otherwise).
pub fn update_env(use_log: bool) {
    let Ok(content) = std::fs::read_to_string("env.env") else {
        return;
    };
    let mut map = ENV_MAP.lock().unwrap_or_else(|p| p.into_inner());
    for line in content.lines() {
        let Some((k, v)) = line.split_once('=') else {
            continue;
        };
        let k = k.trim();
        if k.is_empty() {
            continue;
        }
        let v = v.trim().to_string();
        set_var(k, &v);
        let changed = map.get(k) != Some(&v);
        if changed {
            let msg = format!("new environment overwrite: '{}' --> '{}'\n", k, v);
            if use_log {
                if is_log_initialized() {
                    printf(&msg);
                }
            } else {
                print!("{}", msg);
            }
        }
        map.insert(k.to_string(), v);
    }
}

/// Environment as sorted `(name, value)` pairs (Go `os.Environ()` + sort).
fn sorted_environ() -> Vec<(String, String)> {
    let mut env: Vec<(String, String)> = std::env::vars_os()
        .map(|(k, v)| {
            (
                k.to_string_lossy().into_owned(),
                v.to_string_lossy().into_owned(),
            )
        })
        .collect();
    // Go sorts the "K=V" strings; sorting by the same string keeps the order identical
    env.sort_by(|a, b| format!("{}={}", a.0, a.1).cmp(&format!("{}={}", b.0, b.1)));
    env
}

/// For every variable named `<prefix>...` for which `<name><suffix>` is set and
/// non-empty, replace its value with the suffixed one; also define missing
/// `<name>` from `<name><suffix>`. Returns the previous values (`{{unset}}` for
/// variables that did not exist) for [`env_restore`]. Empty `suffix` → no-op.
pub fn env_replace(prefix: &str, suffix: &str) -> BTreeMap<String, String> {
    let mut old_env = BTreeMap::new();
    if suffix.is_empty() {
        return old_env;
    }
    let environ = sorted_environ();
    // Go matches the prefix against the whole "NAME=VALUE" entry.
    let prefix_matches = |name: &str, value: &str| {
        prefix.is_empty() || format!("{}={}", name, value).starts_with(prefix)
    };
    for (name, value) in &environ {
        if prefix_matches(name, value) {
            let suffixed = std::env::var(format!("{}{}", name, suffix)).unwrap_or_default();
            if !suffixed.is_empty() {
                old_env.insert(name.clone(), value.clone());
                set_var(name, &suffixed);
            }
        }
    }
    for (name, value) in &environ {
        let Some(base) = name.strip_suffix(suffix) else {
            continue;
        };
        if base.is_empty() {
            continue;
        }
        if prefix_matches(name, value) && !old_env.contains_key(base) {
            old_env.insert(base.to_string(), UNSET.to_string());
            set_var(base, value);
        }
    }
    old_env
}

/// Restore variables saved by [`env_replace`].
pub fn env_restore(env: &BTreeMap<String, String>) {
    for (name, value) in env {
        if value == UNSET {
            remove_var(name);
        } else {
            set_var(name, value);
        }
    }
}

/// `std::env::set_var` wrapper (unsafe since Rust 2024: callers must not race
/// with other threads reading the environment — same contract as Go's
/// `os.Setenv`, which DevStats relies on everywhere).
pub fn set_var(name: &str, value: &str) {
    // SAFETY: DevStats binaries mutate the environment only from the main
    // thread during start-up / single-threaded phases, exactly as the Go
    // original does.
    unsafe { std::env::set_var(name, value) }
}

/// `std::env::remove_var` wrapper, see [`set_var`].
pub fn remove_var(name: &str) {
    // SAFETY: see `set_var`.
    unsafe { std::env::remove_var(name) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::test_support::env_lock;

    fn m(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn go_env_table() {
        let _g = env_lock();
        struct Case {
            name: &'static str,
            environment: BTreeMap<String, String>,
            prefix: &'static str,
            suffix: &'static str,
            new_envs: &'static [&'static str],
            expected_save: BTreeMap<String, String>,
            expected_env: BTreeMap<String, String>,
        }
        let cases = vec![
            Case {
                name: "No op",
                environment: m(&[]),
                prefix: "",
                suffix: "",
                new_envs: &[],
                expected_save: m(&[]),
                expected_env: m(&[]),
            },
            Case {
                name: "No replaces",
                environment: m(&[("a", "A"), ("c", "C"), ("b", "B")]),
                prefix: "",
                suffix: "",
                new_envs: &[],
                expected_save: m(&[]),
                expected_env: m(&[("a", "A"), ("c", "C"), ("b", "B")]),
            },
            Case {
                name: "No suffix",
                environment: m(&[("pref_a", "A"), ("pref_c", "C"), ("pref_b", "B")]),
                prefix: "pref_",
                suffix: "",
                new_envs: &[],
                expected_save: m(&[]),
                expected_env: m(&[("pref_a", "A"), ("pref_c", "C"), ("pref_b", "B")]),
            },
            Case {
                name: "No prefix and no suffix hit",
                environment: m(&[("pref_a", "A"), ("pref_c", "C"), ("pref_b", "B")]),
                prefix: "",
                suffix: "_suff",
                new_envs: &[],
                expected_save: m(&[]),
                expected_env: m(&[("pref_a", "A"), ("pref_c", "C"), ("pref_b", "B")]),
            },
            Case {
                name: "No prefix with suffix hit",
                environment: m(&[
                    ("pref_a", "A"),
                    ("pref_c", "C"),
                    ("pref_b", "B"),
                    ("pref_a_suff", "D"),
                ]),
                prefix: "",
                suffix: "_suff",
                new_envs: &[],
                expected_save: m(&[("pref_a", "A")]),
                expected_env: m(&[
                    ("pref_a", "D"),
                    ("pref_c", "C"),
                    ("pref_b", "B"),
                    ("pref_a_suff", "D"),
                ]),
            },
            Case {
                name: "Prefix and suffix",
                environment: m(&[
                    ("pref_a", "A"),
                    ("pref_c", "C"),
                    ("pref_b", "B"),
                    ("pref_a_suff", "D"),
                    ("a", "A"),
                    ("a_suff", "D"),
                ]),
                prefix: "pref_",
                suffix: "_suff",
                new_envs: &[],
                expected_save: m(&[("pref_a", "A")]),
                expected_env: m(&[
                    ("pref_a", "D"),
                    ("pref_c", "C"),
                    ("pref_b", "B"),
                    ("pref_a_suff", "D"),
                    ("a", "A"),
                    ("a_suff", "D"),
                ]),
            },
            Case {
                name: "Replace all starting with 'a' with suffix 2",
                environment: m(&[
                    ("a1", "1"),
                    ("a2", "2"),
                    ("b1", "3"),
                    ("b2", "4"),
                    ("a12", "5"),
                    ("a22", "6"),
                    ("b12", "7"),
                    ("b22", "8"),
                ]),
                prefix: "a",
                suffix: "2",
                new_envs: &[],
                expected_save: m(&[("a", "{{unset}}"), ("a1", "1"), ("a2", "2")]),
                expected_env: m(&[
                    ("a1", "5"),
                    ("a2", "6"),
                    ("b1", "3"),
                    ("b2", "4"),
                    ("a12", "5"),
                    ("a22", "6"),
                    ("b12", "7"),
                    ("b22", "8"),
                ]),
            },
            Case {
                name: "Need to save empty variables too",
                environment: m(&[
                    ("a", ""),
                    ("b", "B"),
                    ("c", ""),
                    ("a2", "1"),
                    ("b2", "2"),
                    ("c2", "3"),
                ]),
                prefix: "",
                suffix: "2",
                new_envs: &[],
                expected_save: m(&[("a", ""), ("b", "B"), ("c", "")]),
                expected_env: m(&[
                    ("a", "1"),
                    ("b", "2"),
                    ("c", "3"),
                    ("a2", "1"),
                    ("b2", "2"),
                    ("c2", "3"),
                ]),
            },
            Case {
                name: "Replace nonexisting var",
                environment: m(&[("pref_a_suff", "new_value")]),
                prefix: "pref_",
                suffix: "_suff",
                new_envs: &["pref_a"],
                expected_save: m(&[("pref_a", "{{unset}}")]),
                expected_env: m(&[("pref_a", "new_value"), ("pref_a_suff", "new_value")]),
            },
            Case {
                name: "Crazy",
                environment: m(&[("aa", "2"), ("a", "1"), ("aaaa", "4"), ("aaa", "3")]),
                prefix: "a",
                suffix: "a",
                new_envs: &[],
                expected_save: m(&[("a", "1"), ("aa", "2"), ("aaa", "3")]),
                expected_env: m(&[("a", "2"), ("aa", "3"), ("aaa", "4"), ("aaaa", "4")]),
            },
            Case {
                name: "Values containing '=' survive save/restore (Go bug fixed)",
                environment: m(&[("pref_x", "k=v=w"), ("pref_x_suff", "a=b")]),
                prefix: "pref_",
                suffix: "_suff",
                new_envs: &[],
                expected_save: m(&[("pref_x", "k=v=w")]),
                expected_env: m(&[("pref_x", "a=b"), ("pref_x_suff", "a=b")]),
            },
        ];
        for (index, test) in cases.iter().enumerate() {
            // The process environment may already contain variables ending
            // with the case's suffix (e.g. the `GIT_CONFIG_KEY_2` set by some
            // tool harnesses); they would pollute the result exactly like they
            // do for the Go test, so hide them for the duration of the case.
            let polluting: Vec<(String, String)> = if test.suffix.is_empty() {
                Vec::new()
            } else {
                std::env::vars()
                    .filter(|(k, _)| {
                        k.ends_with(test.suffix)
                            && (test.prefix.is_empty() || k.starts_with(test.prefix))
                            && !test.environment.contains_key(k)
                    })
                    .collect()
            };
            for (k, _) in &polluting {
                remove_var(k);
            }
            for (k, v) in &test.environment {
                set_var(k, v);
            }
            let saved = env_replace(test.prefix, test.suffix);
            let mut replaced = BTreeMap::new();
            for k in test.environment.keys() {
                replaced.insert(k.clone(), std::env::var(k).unwrap_or_default());
            }
            for k in test.new_envs {
                replaced.insert(k.to_string(), std::env::var(k).unwrap_or_default());
            }
            env_restore(&saved);
            let mut restored = BTreeMap::new();
            for k in test.environment.keys() {
                restored.insert(k.clone(), std::env::var(k).unwrap_or_default());
            }
            for k in test.environment.keys() {
                remove_var(k);
            }
            for k in test.new_envs {
                remove_var(k);
            }
            for (k, v) in &polluting {
                set_var(k, v);
            }
            assert_eq!(
                replaced,
                test.expected_env,
                "case {} '{}': replaced env",
                index + 1,
                test.name
            );
            assert_eq!(
                saved,
                test.expected_save,
                "case {} '{}': saved env",
                index + 1,
                test.name
            );
            assert_eq!(
                restored,
                test.environment,
                "case {} '{}': restored env",
                index + 1,
                test.name
            );
        }
    }

    #[test]
    fn update_env_reads_file_in_cwd() {
        let _g = env_lock();
        let dir = tempfile::tempdir().unwrap();
        let old = std::env::current_dir().unwrap();
        std::env::set_current_dir(dir.path()).unwrap();
        std::fs::write(
            "env.env",
            "DEVSTATS_RS_TEST_A = x=y \n\nno_equals\n=novalue\nDEVSTATS_RS_TEST_B=1\n",
        )
        .unwrap();
        update_env(false);
        std::env::set_current_dir(&old).unwrap();
        assert_eq!(std::env::var("DEVSTATS_RS_TEST_A").unwrap(), "x=y");
        assert_eq!(std::env::var("DEVSTATS_RS_TEST_B").unwrap(), "1");
        remove_var("DEVSTATS_RS_TEST_A");
        remove_var("DEVSTATS_RS_TEST_B");
    }
}
