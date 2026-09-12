//! Go `compress/gzip` behaviour for the GH Archive downloads: `getGHAJSON`
//! first opens the response with `gzip.NewReader` (which only reads and
//! validates the member header — "No data yet, gzip reader" when that
//! fails) and then `ioutil.ReadAll`s the decompressed stream ("Error (no
//! data yet, ioutil readall)"). The header is checked here exactly like Go's
//! `Reader.readHeader`, so the retry decisions and the printed error texts
//! match; the body is inflated with `flate2` (multi-member, like Go).

use std::io::Read;

const FLAG_HDR_CRC: u8 = 1 << 1;
const FLAG_EXTRA: u8 = 1 << 2;
const FLAG_NAME: u8 = 1 << 3;
const FLAG_COMMENT: u8 = 1 << 4;

/// IEEE CRC-32 (`hash/crc32.ChecksumIEEE`), used for the optional header CRC.
fn crc32_ieee(mut crc: u32, data: &[u8]) -> u32 {
    crc = !crc;
    for &b in data {
        crc ^= b as u32;
        for _ in 0..8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0xEDB8_8320
            } else {
                crc >> 1
            };
        }
    }
    !crc
}

/// Go `gzip.NewReader(body)`: the error (Go text) when the gzip member header
/// cannot be read — `EOF` for an empty body, `unexpected EOF` for a short one,
/// `gzip: invalid header` for a non-gzip body (e.g. an HTML 404 page).
pub fn header_error(body: &[u8]) -> Option<String> {
    if body.is_empty() {
        return Some("EOF".to_string());
    }
    if body.len() < 10 {
        return Some("unexpected EOF".to_string());
    }
    if body[0] != 0x1f || body[1] != 0x8b || body[2] != 8 {
        return Some("gzip: invalid header".to_string());
    }
    let flg = body[3];
    let mut digest = crc32_ieee(0, &body[..10]);
    let mut pos = 10usize;
    if flg & FLAG_EXTRA != 0 {
        if body.len() < pos + 2 {
            return Some("unexpected EOF".to_string());
        }
        let xlen = u16::from_le_bytes([body[pos], body[pos + 1]]) as usize;
        digest = crc32_ieee(digest, &body[pos..pos + 2]);
        pos += 2;
        if body.len() < pos + xlen {
            return Some("unexpected EOF".to_string());
        }
        digest = crc32_ieee(digest, &body[pos..pos + xlen]);
        pos += xlen;
    }
    for flag in [FLAG_NAME, FLAG_COMMENT] {
        if flg & flag == 0 {
            continue;
        }
        // Go `readString`: bytes up to the NUL, at most 512 of them; running
        // out of input there surfaces as plain `EOF`.
        let start = pos;
        loop {
            if pos - start >= 512 {
                return Some("gzip: invalid header".to_string());
            }
            if pos >= body.len() {
                return Some("EOF".to_string());
            }
            let b = body[pos];
            pos += 1;
            if b == 0 {
                break;
            }
        }
        digest = crc32_ieee(digest, &body[start..pos]);
    }
    if flg & FLAG_HDR_CRC != 0 {
        if body.len() < pos + 2 {
            return Some("unexpected EOF".to_string());
        }
        let want = u16::from_le_bytes([body[pos], body[pos + 1]]);
        if want != (digest & 0xffff) as u16 {
            return Some("gzip: invalid header".to_string());
        }
    }
    None
}

/// Go `ioutil.ReadAll(gzipReader)` after a successful `NewReader`: the whole
/// decompressed stream (all members), or a Go-worded error.
pub fn read_all(body: &[u8]) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    let mut dec = flate2::read::MultiGzDecoder::new(body);
    match dec.read_to_end(&mut out) {
        Ok(_) => Ok(out),
        Err(e) => Err(go_gzip_error(&e)),
    }
}

/// Go text of a `flate2` decompression error: a truncated stream is
/// `unexpected EOF`, a checksum mismatch `gzip: invalid checksum`, a bad
/// following member header `gzip: invalid header`; other corruption keeps the
/// `flate2` wording (Go's `flate: corrupt input before offset N` carries an
/// offset that is not available here).
fn go_gzip_error(e: &std::io::Error) -> String {
    if e.kind() == std::io::ErrorKind::UnexpectedEof {
        return "unexpected EOF".to_string();
    }
    let msg = e.to_string();
    let lower = msg.to_lowercase();
    if lower.contains("checksum") || lower.contains("crc") {
        return "gzip: invalid checksum".to_string();
    }
    if lower.contains("header") {
        return "gzip: invalid header".to_string();
    }
    format!("flate: {}", msg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use std::io::Write;

    fn gz(data: &[u8]) -> Vec<u8> {
        let mut enc = GzEncoder::new(Vec::new(), Compression::default());
        enc.write_all(data).unwrap();
        enc.finish().unwrap()
    }

    #[test]
    fn crc32_matches_ieee() {
        // `crc32.ChecksumIEEE([]byte("123456789"))` = 0xCBF43926
        assert_eq!(crc32_ieee(0, b"123456789"), 0xCBF4_3926);
        // Incremental updates give the same result.
        let d = crc32_ieee(0, b"12345");
        assert_eq!(crc32_ieee(d, b"6789"), 0xCBF4_3926);
    }

    #[test]
    fn header_errors_like_go() {
        assert_eq!(header_error(b""), Some("EOF".to_string()));
        assert_eq!(
            header_error(b"\x1f\x8b\x08"),
            Some("unexpected EOF".to_string())
        );
        assert_eq!(
            header_error(b"<html><body>404 Not Found</body></html>"),
            Some("gzip: invalid header".to_string())
        );
        // Wrong compression method.
        let mut bad = gz(b"x");
        bad[2] = 7;
        assert_eq!(header_error(&bad), Some("gzip: invalid header".to_string()));
        // Plain member header is fine.
        assert_eq!(header_error(&gz(b"{}\n")), None);
        // FNAME without terminating NUL: Go's readString returns the reader's EOF.
        let mut named = vec![0x1f, 0x8b, 8, FLAG_NAME, 0, 0, 0, 0, 0, 3];
        named.extend_from_slice(b"file.json");
        assert_eq!(header_error(&named), Some("EOF".to_string()));
        named.push(0);
        assert_eq!(header_error(&named), None);
        // FEXTRA cut short.
        let extra = vec![0x1f, 0x8b, 8, FLAG_EXTRA, 0, 0, 0, 0, 0, 3, 5];
        assert_eq!(header_error(&extra), Some("unexpected EOF".to_string()));
        // Header CRC: wrong → invalid header, right → ok.
        let mut hcrc = vec![0x1f, 0x8b, 8, FLAG_HDR_CRC, 0, 0, 0, 0, 0, 3];
        let d = crc32_ieee(0, &hcrc);
        hcrc.extend_from_slice(&[0xff, 0xff]);
        assert_eq!(
            header_error(&hcrc),
            Some("gzip: invalid header".to_string())
        );
        hcrc.truncate(10);
        hcrc.extend_from_slice(&(d as u16).to_le_bytes());
        assert_eq!(header_error(&hcrc), None);
    }

    #[test]
    fn read_all_multi_member_and_errors() {
        let mut two = gz(b"{\"a\":1}\n");
        two.extend_from_slice(&gz(b"{\"b\":2}\n"));
        assert_eq!(read_all(&two).unwrap(), b"{\"a\":1}\n{\"b\":2}\n");
        // Truncated stream.
        let full = gz(b"some longer payload to make sure the deflate stream has a body");
        let cut = &full[..full.len() - 6];
        assert_eq!(read_all(cut), Err("unexpected EOF".to_string()));
        // Corrupted checksum.
        let mut bad = full.clone();
        let n = bad.len();
        bad[n - 5] ^= 0xff;
        assert_eq!(read_all(&bad), Err("gzip: invalid checksum".to_string()));
        // Empty gzip member (the real 2012-03-10-15.json.gz) inflates to nothing.
        assert_eq!(read_all(&gz(b"")).unwrap(), b"");
    }
}
