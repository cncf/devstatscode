//! Unicode helpers — port of `unicode.go`.

use unicode_normalization::UnicodeNormalization;

/// NFKD-normalize and drop every code point outside printable ASCII
/// (`< 32` or `>= 127`), so `gżegżółką` → `gzegzoka`.
pub fn strip_unicode(s: &str) -> String {
    s.nfkd()
        .filter(|c| (*c as u32) >= 32 && (*c as u32) < 127)
        .collect()
}

/// Normalize a name for DB comparison: [`strip_unicode`], trim, lowercase and
/// remove `- / . space , ; : ` ( ) [ ] < > _ " '`.
pub fn normalize_name(s: &str) -> String {
    strip_unicode(s)
        .trim()
        .to_lowercase()
        .chars()
        .filter(|c| {
            !matches!(
                c,
                '-' | '/'
                    | '.'
                    | ' '
                    | ','
                    | ';'
                    | ':'
                    | '`'
                    | '('
                    | ')'
                    | '['
                    | ']'
                    | '<'
                    | '>'
                    | '_'
                    | '"'
                    | '\''
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strip_unicode_table() {
        assert_eq!(strip_unicode("hello"), "hello");
        assert_eq!(strip_unicode("control:\t\n\r"), "control:");
        assert_eq!(strip_unicode("gżegżółką"), "gzegzoka");
        assert_eq!(strip_unicode("net_ease_网易有态"), "net_ease_");
    }

    #[test]
    fn normalize_name_table() {
        assert_eq!(normalize_name("hello"), "hello");
        assert_eq!(normalize_name("control:\t\n\r"), "control");
        assert_eq!(normalize_name("gżegżółką"), "gzegzoka");
        assert_eq!(normalize_name("net_ease_网易有态"), "netease");
        assert_eq!(
            normalize_name(" see;hello-world/k8s.io, said: HE`MAN "),
            "seehelloworldk8siosaidheman"
        );
        assert_eq!(
            normalize_name("Contributions (issues, PRs, git pushes)"),
            "contributionsissuesprsgitpushes"
        );
        assert_eq!(normalize_name("Exclude (ro[bot]nik)"), "excluderobotnik");
        assert_eq!(
            normalize_name("comment\"sallcoted'ivoire"),
            "commentsallcotedivoire"
        );
        assert_eq!(normalize_name("Piraeus-Datastore"), "piraeusdatastore");
    }
}
