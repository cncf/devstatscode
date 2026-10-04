package devstatscode

import (
	"bytes"
	"encoding/json"
	"unicode/utf8"
)

// JSONChunk - a piece of a damaged GH Archive line: a syntactically
// well-formed top-level JSON value (Broken false, it may still fail to
// decode as an event) or an undecodable remainder (Broken true): not valid
// UTF-8, or a syntax error up to the end of its NUL-delimited segment.
// Rust: devstatscode::broken_json::JsonChunk.
type JSONChunk struct {
	Bytes  []byte
	Broken bool
}

// IsPlainLine - true when line may take the fast path: it has no NUL byte and
// is valid UTF-8, so a single strict decode decides about it. jsoniter would
// silently accept `<json>\0<anything>` (a literal NUL ends its token
// scanning), dropping whatever follows the NUL - such lines must always go
// through RecoverJSONChunks.
func IsPlainLine(line []byte) bool {
	return bytes.IndexByte(line, 0) < 0 && utf8.Valid(line)
}

// TrimJSONWS - trims JSON whitespace only (" \t\r\n")
func TrimJSONWS(b []byte) []byte {
	return bytes.Trim(b, " \t\r\n")
}

// SplitNULSegments - splits line on runs of NUL bytes into trimmed, non-empty segments
func SplitNULSegments(line []byte) (segs [][]byte) {
	for _, seg := range bytes.Split(line, []byte{0}) {
		seg = TrimJSONWS(seg)
		if len(seg) > 0 {
			segs = append(segs, seg)
		}
	}
	return
}

func isJSONWS(c byte) bool {
	return c == ' ' || c == '\t' || c == '\r' || c == '\n'
}

func pushBroken(chunks []JSONChunk, rest []byte) []JSONChunk {
	rest = TrimJSONWS(rest)
	if len(rest) > 0 {
		chunks = append(chunks, JSONChunk{Bytes: rest, Broken: true})
	}
	return chunks
}

// scanJSONValues - scans one NUL-free, valid UTF-8 segment for consecutive
// top-level JSON values (json.Decoder rules: an object or array ends at its
// closing bracket, a string, number or literal must be followed by whitespace
// or the end of the segment). A syntax error ends the scan: the trimmed rest
// of the segment, from the end of the last good value, is one broken chunk.
func scanJSONValues(seg []byte) (chunks []JSONChunk) {
	dec := json.NewDecoder(bytes.NewReader(seg))
	for {
		start := min(int(dec.InputOffset()), len(seg))
		var raw json.RawMessage
		if dec.Decode(&raw) != nil {
			// io.EOF leaves only whitespace, which pushBroken drops
			return pushBroken(chunks, seg[start:])
		}
		end := min(int(dec.InputOffset()), len(seg))
		value := TrimJSONWS(seg[start:end])
		selfDelimited := len(value) > 0 && (value[0] == '{' || value[0] == '[')
		glued := end < len(seg) && !isJSONWS(seg[end])
		if len(value) == 0 || (!selfDelimited && glued) {
			return pushBroken(chunks, seg[start:])
		}
		chunks = append(chunks, JSONChunk{Bytes: value})
	}
	return
}

// RecoverJSONChunks - splits a damaged line into chunks: NUL runs separate
// segments, each segment is trimmed of JSON whitespace, a segment that is not
// valid UTF-8 is one broken chunk, otherwise its consecutive top-level JSON
// values are returned in order and a syntax error turns the rest of the
// segment into one broken chunk. Nothing is ever merged across NULs or
// repaired: only well-formed, unmodified values are offered for decoding.
func RecoverJSONChunks(line []byte) (chunks []JSONChunk) {
	for _, seg := range SplitNULSegments(line) {
		if !utf8.Valid(seg) {
			chunks = append(chunks, JSONChunk{Bytes: seg, Broken: true})
			continue
		}
		chunks = append(chunks, scanJSONValues(seg)...)
	}
	return
}
