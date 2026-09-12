//! Go `encoding/base64` `StdEncoding.DecodeString` semantics: padding
//! required, `\r`/`\n` ignored anywhere, and the exact
//! `illegal base64 data at input byte N` error positions — the git commit
//! metadata of `get_repos` is base64-encoded by `git_commits.sh` and any
//! decode error is reported in the tool's output.

use std::fmt;

/// Go `base64.CorruptInputError`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CorruptInputError(pub usize);

impl fmt::Display for CorruptInputError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "illegal base64 data at input byte {}", self.0)
    }
}

impl std::error::Error for CorruptInputError {}

const PAD: u8 = b'=';

fn decode_char(c: u8) -> Option<u8> {
    match c {
        b'A'..=b'Z' => Some(c - b'A'),
        b'a'..=b'z' => Some(c - b'a' + 26),
        b'0'..=b'9' => Some(c - b'0' + 52),
        b'+' => Some(62),
        b'/' => Some(63),
        _ => None,
    }
}

/// Go `decodeQuantum`: decode one 4-character quantum starting at `si`,
/// appending up to 3 bytes to `dst`. Returns the new input position.
fn decode_quantum(
    dst: &mut Vec<u8>,
    src: &[u8],
    mut si: usize,
) -> Result<usize, (usize, CorruptInputError)> {
    let mut dbuf = [0u8; 4];
    let mut dlen = 4;
    let mut j = 0usize;
    let mut trailing_err: Option<CorruptInputError> = None;
    while j < 4 {
        if src.len() == si {
            match j {
                0 => return Ok(si),
                // Go: `case j == 1, enc.padChar != NoPadding` — StdEncoding pads.
                _ => return Err((si, CorruptInputError(si - j))),
            }
        }
        let inp = src[si];
        si += 1;
        if let Some(out) = decode_char(inp) {
            dbuf[j] = out;
            j += 1;
            continue;
        }
        if inp == b'\n' || inp == b'\r' {
            continue;
        }
        if inp != PAD {
            return Err((si, CorruptInputError(si - 1)));
        }
        // We've reached the end and there's padding
        match j {
            0 | 1 => return Err((si, CorruptInputError(si - 1))),
            2 => {
                // "==" is expected, the first "=" is already consumed.
                while si < src.len() && (src[si] == b'\n' || src[si] == b'\r') {
                    si += 1;
                }
                if si == src.len() {
                    return Err((si, CorruptInputError(src.len())));
                }
                if src[si] != PAD {
                    return Err((si, CorruptInputError(si - 1)));
                }
                si += 1;
            }
            _ => {}
        }
        while si < src.len() && (src[si] == b'\n' || src[si] == b'\r') {
            si += 1;
        }
        if si < src.len() {
            // trailing garbage
            trailing_err = Some(CorruptInputError(si));
        }
        dlen = j;
        break;
    }
    // Convert 4x 6bit source bytes into 3 bytes
    let val =
        (dbuf[0] as u32) << 18 | (dbuf[1] as u32) << 12 | (dbuf[2] as u32) << 6 | dbuf[3] as u32;
    let b = [(val >> 16) as u8, (val >> 8) as u8, val as u8];
    match dlen {
        4 => dst.extend_from_slice(&b[..3]),
        3 => dst.extend_from_slice(&b[..2]),
        2 => dst.extend_from_slice(&b[..1]),
        _ => {}
    }
    match trailing_err {
        Some(e) => Err((si, e)),
        None => Ok(si),
    }
}

/// Go `base64.StdEncoding.DecodeString`: the decoded bytes, or the bytes
/// decoded before the error together with the error.
pub fn std_decode(s: &str) -> Result<Vec<u8>, (Vec<u8>, CorruptInputError)> {
    let src = s.as_bytes();
    let mut dst = Vec::with_capacity(src.len() / 4 * 3 + 3);
    let mut si = 0;
    while si < src.len() {
        match decode_quantum(&mut dst, src, si) {
            Ok(nsi) => si = nsi,
            Err((_, e)) => return Err((dst, e)),
        }
    }
    Ok(dst)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok(s: &str) -> Vec<u8> {
        std_decode(s).unwrap()
    }

    fn err(s: &str) -> usize {
        std_decode(s).unwrap_err().1 .0
    }

    #[test]
    fn decodes_like_go() {
        assert_eq!(ok(""), b"");
        assert_eq!(ok("YQ=="), b"a");
        assert_eq!(ok("YWI="), b"ab");
        assert_eq!(ok("YWJj"), b"abc");
        assert_eq!(ok("YWJjZA=="), b"abcd");
        assert_eq!(ok("YW\nJj\r\n"), b"abc");
        assert_eq!(ok("YQ=\n="), b"a");
        assert_eq!(ok("+/8="), [0xfb, 0xff]);
        assert_eq!(ok("xIXEmcWCdw=="), "ąęłw".as_bytes());
        // Non-canonical trailing bits are accepted (StdEncoding is not Strict).
        assert_eq!(ok("YR=="), b"a");
    }

    #[test]
    fn error_positions_like_go() {
        // Values produced by Go's base64.StdEncoding.DecodeString.
        assert_eq!(err("Y"), 0);
        assert_eq!(err("YQ"), 0);
        assert_eq!(err("YWJ"), 0);
        assert_eq!(err("YWJjZ"), 4);
        assert_eq!(err("YQ="), 3);
        assert_eq!(err("YQ=x"), 2);
        assert_eq!(err("YQ==x"), 4);
        assert_eq!(err("Y=Jj"), 1);
        assert_eq!(err("=WJj"), 0);
        assert_eq!(err("YW-j"), 2);
        assert_eq!(err("YWJj YQ=="), 4);
        assert_eq!(err("YWJjYQ==YWJj"), 8);
        assert_eq!(std_decode("YWJjYQ==YWJj").unwrap_err().0, b"abca");
        assert_eq!(err("YWJj=YQ=="), 4);
        assert_eq!(
            CorruptInputError(7).to_string(),
            "illegal base64 data at input byte 7"
        );
    }
}
