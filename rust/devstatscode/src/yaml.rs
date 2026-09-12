//! YAML helpers — port of `yaml.go`.

use serde::Serialize;

use crate::error::fatal_on_error;
use crate::json::write_file_0644;

/// Serialize `obj` as YAML into file `path` (mode 0644). Fatal on error.
pub fn object_to_yaml<T: Serialize>(obj: &T, path: &str) {
    let yaml = match serde_yaml_ng::to_string(obj) {
        Ok(s) => s,
        Err(e) => fatal_on_error(e),
    };
    write_file_0644(path, yaml.as_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn writes_yaml_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.yaml");
        let mut m: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
        m.insert("projects", vec!["a", "b"]);
        object_to_yaml(&m, path.to_str().unwrap());
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            "projects:\n- a\n- b\n"
        );
    }
}
