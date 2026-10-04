//! Go `compress/gzip` behaviour for the GH Archive downloads: `getGHAJSON`
//! first opens the response with `gzip.NewReader` (which only reads and
//! validates the member header — "No data yet, gzip reader" when that
//! fails) and then `ioutil.ReadAll`s the decompressed stream ("Error (no
//! data yet, ioutil readall)"). The header is checked here exactly like Go's
//! `Reader.readHeader`, so the retry decisions and the printed error texts
//! match; the body is inflated with `flate2` (multi-member, like Go).

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
    parse_header(body).err()
}

/// Go `Reader.readHeader`: the length of the member header at the start of
/// `body`, or the Go error text.
fn parse_header(body: &[u8]) -> Result<usize, String> {
    if body.is_empty() {
        return Err("EOF".to_string());
    }
    if body.len() < 10 {
        return Err("unexpected EOF".to_string());
    }
    if body[0] != 0x1f || body[1] != 0x8b || body[2] != 8 {
        return Err("gzip: invalid header".to_string());
    }
    let flg = body[3];
    let mut digest = crc32_ieee(0, &body[..10]);
    let mut pos = 10usize;
    if flg & FLAG_EXTRA != 0 {
        if body.len() < pos + 2 {
            return Err("unexpected EOF".to_string());
        }
        let xlen = u16::from_le_bytes([body[pos], body[pos + 1]]) as usize;
        digest = crc32_ieee(digest, &body[pos..pos + 2]);
        pos += 2;
        if body.len() < pos + xlen {
            return Err("unexpected EOF".to_string());
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
                return Err("gzip: invalid header".to_string());
            }
            if pos >= body.len() {
                return Err("EOF".to_string());
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
            return Err("unexpected EOF".to_string());
        }
        let want = u16::from_le_bytes([body[pos], body[pos + 1]]);
        if want != (digest & 0xffff) as u16 {
            return Err("gzip: invalid header".to_string());
        }
        pos += 2;
    }
    Ok(pos)
}

/// Go `ioutil.ReadAll(gzipReader)` after a successful `NewReader`: the whole
/// decompressed stream (all members), or a Go-worded error together with
/// every byte decompressed before it — like Go, where `ReadAll` returns the
/// data read so far along with the error and `flate` flushes whatever it
/// decoded before a corrupt symbol — so `getGHAJSON` can parse that partial
/// data once the retries are exhausted. The gzip framing (header, raw
/// deflate member, CRC-32/size trailer, further members) is walked here
/// because `flate2`'s readers drop the output of the failing read.
pub fn read_all(body: &[u8]) -> Result<Vec<u8>, (Vec<u8>, String)> {
    let mut out: Vec<u8> = Vec::new();
    let mut rest = body;
    loop {
        // The first header was already validated (`header_error`); a short or
        // invalid header of a further member is an error, like Go's `readHeader`.
        let hdr = match parse_header(rest) {
            Ok(n) => n,
            Err(e) => return Err((out, e)),
        };
        rest = &rest[hdr..];
        let member_start = out.len();
        let mut dec = flate2::Decompress::new(false);
        let mut stalled = 0;
        loop {
            if out.capacity() - out.len() < 64 * 1024 {
                out.reserve(out.len().max(256 * 1024));
            }
            let (in0, out0) = (dec.total_in(), dec.total_out());
            let res = dec.decompress_vec(rest, &mut out, flate2::FlushDecompress::None);
            let consumed = (dec.total_in() - in0) as usize;
            let produced = dec.total_out() - out0;
            rest = &rest[consumed..];
            match res {
                Ok(flate2::Status::StreamEnd) => break,
                Ok(_) if consumed == 0 && produced == 0 => {
                    if rest.is_empty() {
                        return Err((out, "unexpected EOF".to_string()));
                    }
                    stalled += 1;
                    if stalled > 1 {
                        return Err((out, "flate: corrupt deflate stream".to_string()));
                    }
                }
                Ok(_) => stalled = 0,
                Err(_) => return Err((out, "flate: corrupt deflate stream".to_string())),
            }
        }
        if rest.len() < 8 {
            return Err((out, "unexpected EOF".to_string()));
        }
        let crc = u32::from_le_bytes([rest[0], rest[1], rest[2], rest[3]]);
        let size = u32::from_le_bytes([rest[4], rest[5], rest[6], rest[7]]);
        rest = &rest[8..];
        let member = &out[member_start..];
        if crc != crc32fast::hash(member) || size != member.len() as u32 {
            return Err((out, "gzip: invalid checksum".to_string()));
        }
        if rest.is_empty() {
            return Ok(out);
        }
    }
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
        // Truncated stream: the error comes with the bytes decompressed so far
        // (cut inside the 8-byte trailer: the whole payload).
        let payload = b"some longer payload to make sure the deflate stream has a body";
        let full = gz(payload);
        let cut = &full[..full.len() - 6];
        assert_eq!(
            read_all(cut),
            Err((payload.to_vec(), "unexpected EOF".to_string()))
        );
        // A stream cut inside the header-less middle of a long body still
        // yields its decodable prefix.
        let long: Vec<u8> = (0..20000u32)
            .map(|i| format!("{{\"id\":\"{i}\"}}\n"))
            .collect::<String>()
            .into_bytes();
        let full_long = gz(&long);
        let (partial, err) = read_all(&full_long[..full_long.len() / 2]).unwrap_err();
        assert_eq!(err, "unexpected EOF");
        assert!(partial.len() > 1000 && long.starts_with(&partial));
        // Corrupted checksum: everything was decompressed, only the trailer is wrong.
        let mut bad = full.clone();
        let n = bad.len();
        bad[n - 5] ^= 0xff;
        assert_eq!(
            read_all(&bad),
            Err((payload.to_vec(), "gzip: invalid checksum".to_string()))
        );
        // Wrong ISIZE in the trailer is a checksum error too.
        let mut bad_size = full.clone();
        bad_size[n - 1] ^= 0x01;
        assert_eq!(
            read_all(&bad_size),
            Err((payload.to_vec(), "gzip: invalid checksum".to_string()))
        );
        // Corruption inside the deflate stream: what decoded before it is kept.
        let mut corrupt = full_long.clone();
        for b in corrupt.iter_mut().skip(2000).take(64) {
            *b ^= 0x55;
        }
        // (this corruption decodes to garbage up to the trailer, so the
        // checksum catches it; an invalid symbol would stop the inflate).
        let (partial, err) = read_all(&corrupt).unwrap_err();
        assert!(err == "flate: corrupt deflate stream" || err == "gzip: invalid checksum");
        assert!(partial.len() > 1000 && partial != long);
        assert!(long.starts_with(&partial[..1000]));
        let mut corrupt2 = full_long.clone();
        corrupt2[2000..2064].fill(0xff);
        let (partial, err) = read_all(&corrupt2).unwrap_err();
        assert!(err == "flate: corrupt deflate stream" || err == "gzip: invalid checksum");
        assert!(partial.len() > 1000 && long.starts_with(&partial[..1000]));
        // A second member that is truncated/garbage: the first one is kept.
        let mut two_cut = two.clone();
        two_cut.truncate(two.len() - 12);
        let (partial, err) = read_all(&two_cut).unwrap_err();
        assert_eq!(err, "unexpected EOF");
        assert!(partial.starts_with(b"{\"a\":1}\n") && partial.len() > 8);
        let second = gz(b"{\"b\":2}\n").len();
        two_cut.truncate(two.len() - second + 5);
        assert_eq!(
            read_all(&two_cut),
            Err((b"{\"a\":1}\n".to_vec(), "unexpected EOF".to_string()))
        );
        let mut trailing = gz(b"{\"a\":1}\n");
        trailing.extend_from_slice(b"<html>not gzip</html>");
        assert_eq!(
            read_all(&trailing),
            Err((b"{\"a\":1}\n".to_vec(), "gzip: invalid header".to_string()))
        );
        let mut short = gz(b"{\"a\":1}\n");
        short.extend_from_slice(&[0x1f, 0x8b, 8]);
        assert_eq!(
            read_all(&short),
            Err((b"{\"a\":1}\n".to_vec(), "unexpected EOF".to_string()))
        );
        // Empty gzip member (the real 2012-03-10-15.json.gz) inflates to nothing.
        assert_eq!(read_all(&gz(b"")).unwrap(), b"");
        // Header with every optional field (FEXTRA, FNAME, FCOMMENT, FHCRC) is walked.
        let mut fancy = vec![
            0x1f,
            0x8b,
            8,
            FLAG_EXTRA | FLAG_NAME | FLAG_COMMENT | FLAG_HDR_CRC,
            0,
            0,
            0,
            0,
            0,
            0xff,
        ];
        fancy.extend_from_slice(&[2, 0, b'x', b'y']);
        fancy.extend_from_slice(b"name\0");
        fancy.extend_from_slice(b"comment\0");
        let hcrc = (crc32_ieee(0, &fancy) & 0xffff) as u16;
        fancy.extend_from_slice(&hcrc.to_le_bytes());
        let plain = gz(payload);
        fancy.extend_from_slice(&plain[10..]);
        assert_eq!(header_error(&fancy), None);
        assert_eq!(read_all(&fancy).unwrap(), payload);
    }
}
