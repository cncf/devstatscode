//! Go `encoding/csv` **writer** semantics (RFC 4180 as Go does it): fields are
//! quoted only when they contain the delimiter, a double quote, `\r` or `\n`,
//! start with white space or are exactly `\.`; quotes are doubled; records end
//! with `\n` (or `\r\n` with `use_crlf`, which also drops bare `\r`s inside
//! quoted fields exactly like Go). Used by `runq` (`GHA2DB_CSVOUT`) and every
//! other tool that writes CSV reports.

use std::io::{self, Write};

/// Go `csv.Writer`.
pub struct Writer<W: Write> {
    /// Field delimiter (Go `Comma`, default `,`).
    pub comma: char,
    /// Terminate records with `\r\n` (Go `UseCRLF`).
    pub use_crlf: bool,
    w: io::BufWriter<W>,
}

/// Go `csv.errInvalidDelim` (`csv: invalid field or comment delimiter`).
pub const ERR_INVALID_DELIM: &str = "csv: invalid field or comment delimiter";

fn valid_delim(c: char) -> bool {
    c != '\0' && c != '"' && c != '\r' && c != '\n' && c != char::REPLACEMENT_CHARACTER
}

impl<W: Write> Writer<W> {
    /// `csv.NewWriter(w)`.
    pub fn new(w: W) -> Self {
        Writer {
            comma: ',',
            use_crlf: false,
            w: io::BufWriter::new(w),
        }
    }

    /// Go `fieldNeedsQuotes`. Fields are byte strings like Go strings: an
    /// invalid UTF-8 first byte decodes as `RuneError`, which is not a space.
    fn field_needs_quotes(&self, field: &[u8]) -> bool {
        if field.is_empty() {
            return false;
        }
        if field == b"\\." {
            return true;
        }
        let mut comma_buf = [0u8; 4];
        let comma = self.comma.encode_utf8(&mut comma_buf).as_bytes();
        if field.windows(comma.len()).any(|w| w == comma)
            || field.iter().any(|&b| b == b'"' || b == b'\r' || b == b'\n')
        {
            return true;
        }
        field
            .utf8_chunks()
            .next()
            .and_then(|c| c.valid().chars().next())
            .is_some_and(char::is_whitespace)
    }

    /// `Writer.Write`: write one record (the fields are not flushed until
    /// [`flush`](Self::flush)). Fields may be `&str`, `String` or raw bytes.
    pub fn write(&mut self, record: &[impl AsRef<[u8]>]) -> io::Result<()> {
        if !valid_delim(self.comma) {
            return Err(io::Error::other(ERR_INVALID_DELIM));
        }
        let mut comma_buf = [0u8; 4];
        let comma = self.comma.encode_utf8(&mut comma_buf).as_bytes().to_vec();
        for (n, field) in record.iter().enumerate() {
            let field = field.as_ref();
            if n > 0 {
                self.w.write_all(&comma)?;
            }
            if !self.field_needs_quotes(field) {
                self.w.write_all(field)?;
                continue;
            }
            self.w.write_all(b"\"")?;
            for &b in field {
                match b {
                    b'"' => self.w.write_all(b"\"\"")?,
                    b'\r' => {
                        if !self.use_crlf {
                            self.w.write_all(b"\r")?;
                        }
                    }
                    b'\n' => {
                        if self.use_crlf {
                            self.w.write_all(b"\r\n")?;
                        } else {
                            self.w.write_all(b"\n")?;
                        }
                    }
                    other => self.w.write_all(&[other])?,
                }
            }
            self.w.write_all(b"\"")?;
        }
        if self.use_crlf {
            self.w.write_all(b"\r\n")
        } else {
            self.w.write_all(b"\n")
        }
    }

    /// `Writer.WriteAll`: every record, then flush.
    pub fn write_all_records(&mut self, records: &[Vec<String>]) -> io::Result<()> {
        for r in records {
            self.write(r)?;
        }
        self.flush()
    }

    /// `Writer.Flush`.
    pub fn flush(&mut self) -> io::Result<()> {
        self.w.flush()
    }
}

/// Render records to a string with Go's default writer settings.
pub fn to_string(records: &[Vec<String>]) -> String {
    let mut buf = Vec::new();
    {
        let mut w = Writer::new(&mut buf);
        w.write_all_records(records).unwrap();
    }
    String::from_utf8(buf).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(input: &[&[&str]], comma: Option<char>, use_crlf: bool) -> Result<String, String> {
        let mut buf = Vec::new();
        {
            let mut w = Writer::new(&mut buf);
            if let Some(c) = comma {
                w.comma = c;
            }
            w.use_crlf = use_crlf;
            for rec in input {
                w.write(rec).map_err(|e| e.to_string())?;
            }
            w.flush().unwrap();
        }
        Ok(String::from_utf8(buf).unwrap())
    }

    /// Go `encoding/csv` `writerTests` table (Go 1.22+).
    /// (input records, expected output, delimiter override, use CRLF)
    type WriterCase<'a> = (&'a [&'a [&'a str]], &'a str, Option<char>, bool);

    #[test]
    fn go_writer_tests() {
        let cases: Vec<WriterCase> = vec![
            (&[&["abc"]], "abc\n", None, false),
            (&[&["abc"]], "abc\r\n", None, true),
            (&[&["\"abc\""]], "\"\"\"abc\"\"\"\n", None, false),
            (&[&["a\"b"]], "\"a\"\"b\"\n", None, false),
            (&[&["\"a\"b\""]], "\"\"\"a\"\"b\"\"\"\n", None, false),
            (&[&[" abc"]], "\" abc\"\n", None, false),
            (&[&["abc,def"]], "\"abc,def\"\n", None, false),
            (&[&["abc", "def"]], "abc,def\n", None, false),
            (&[&["abc"], &["def"]], "abc\ndef\n", None, false),
            (&[&["abc\ndef"]], "\"abc\ndef\"\n", None, false),
            (&[&["abc\ndef"]], "\"abc\r\ndef\"\r\n", None, true),
            (&[&["abc\rdef"]], "\"abcdef\"\r\n", None, true),
            (&[&["abc\rdef"]], "\"abc\rdef\"\n", None, false),
            (&[&[""]], "\n", None, false),
            (&[&["", ""]], ",\n", None, false),
            (&[&["", "", ""]], ",,\n", None, false),
            (&[&["", "", "a"]], ",,a\n", None, false),
            (&[&["", "a", ""]], ",a,\n", None, false),
            (&[&["", "a", "a"]], ",a,a\n", None, false),
            (&[&["a", "", ""]], "a,,\n", None, false),
            (&[&["a", "", "a"]], "a,,a\n", None, false),
            (&[&["a", "a", ""]], "a,a,\n", None, false),
            (&[&["a", "a", "a"]], "a,a,a\n", None, false),
            (&[&["\\."]], "\"\\.\"\n", None, false),
            (
                &[&["x09A\u{b4}\u{1c}", "aktau"]],
                "x09A\u{b4}\u{1c},aktau\n",
                None,
                false,
            ),
            (
                &[&[",x09A\u{b4}\u{1c}", "aktau"]],
                "\",x09A\u{b4}\u{1c}\",aktau\n",
                None,
                false,
            ),
            (&[&["a", "a", ""]], "a|a|\n", Some('|'), false),
            (&[&[",", ",", ""]], ",|,|\n", Some('|'), false),
        ];
        for (input, want, comma, crlf) in cases {
            assert_eq!(run(input, comma, crlf).unwrap(), want, "input {input:?}");
        }
        assert_eq!(
            run(&[&["foo"]], Some('"'), false).unwrap_err(),
            ERR_INVALID_DELIM
        );
    }

    #[test]
    fn raw_bytes_fields() {
        let mut buf = Vec::new();
        {
            let mut w = Writer::new(&mut buf);
            w.write(&[
                b"\xff\xfe a".to_vec(),
                b" \xff".to_vec(),
                b"a\"\xff".to_vec(),
            ])
            .unwrap();
            w.flush().unwrap();
        }
        assert_eq!(buf, b"\xff\xfe a,\" \xff\",\"a\"\"\xff\"\n".to_vec());
    }

    #[test]
    fn leading_unicode_space_and_tabs_are_quoted() {
        assert_eq!(to_string(&[vec!["\tx".to_string()]]), "\"\tx\"\n");
        assert_eq!(to_string(&[vec!["\u{a0}x".to_string()]]), "\"\u{a0}x\"\n");
        assert_eq!(to_string(&[vec!["x ".to_string()]]), "x \n");
        assert_eq!(
            to_string(&[vec!["ż".to_string(), "a,b".to_string()]]),
            "ż,\"a,b\"\n"
        );
    }
}
