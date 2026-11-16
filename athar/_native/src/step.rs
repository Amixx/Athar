//! Minimal STEP (ISO-10303-21) data-section tokenizer for IFC.
//!
//! This is the front of the native parse path: it turns the bytes of an IFC
//! file's `DATA` section into `(step_id, keyword, Vec<Token>)` records without
//! materializing anything in Python. It is deliberately small — it understands
//! exactly the STEP grammar IFC uses (simple records of the form
//! `#id = KEYWORD(arg, ...);`), not the full EXPRESS/Part-21 surface.
//!
//! What it handles:
//!   * `/* ... */` comments and arbitrary whitespace between tokens.
//!   * Records spanning multiple lines; the terminating `;` ends a record.
//!   * Arguments: integers, reals, strings (`'...'` with `''` escape and the
//!     `\X\` / `\X2\` / `\X4\` / `\S\` control directives decoded), enums
//!     (`.T.`), entity refs (`#123`), `$` (null), `*` (derived), nested lists
//!     `( ... )`, and typed values `KEYWORD( ... )`.
//!   * Raw 8-bit bytes inside strings: valid UTF-8 sequences decode as UTF-8,
//!     every other byte as its Latin-1 code point, so `'H\xF6he'` and
//!     `'H\X\F6he'` are the same string.
//!   * Keywords and enumeration values in any letter case, normalized to upper
//!     case.
//!   * The HEADER section only as far as finding the `DATA` statement after its
//!     `ENDSEC;`, outside strings and comments.
//!
//! Every byte from `DATA;` to its `ENDSEC;` belongs to a record, a bare `#id;`
//! or trivia; anything else is an error naming the byte offset and the record
//! being parsed (or the last completed one). Duplicate `#id` definitions are
//! errors.
//!
//! What it does NOT do (by design — handled elsewhere or unsupported):
//!   * `\S\` under ISO 8859 code pages other than `\PA\` (kept verbatim).
//!   * Complex (multiple-inheritance) instances `#1=(A()B());`, which IFC2X3
//!     and IFC4 exporters do not write; they are rejected.

use std::collections::{HashMap, HashSet};

/// A single parsed STEP argument.
#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    Int(i64),
    Real(f64),
    /// String literal contents (between the quotes), `''` already collapsed to `'`.
    Str(String),
    /// Enumeration / logical, e.g. `.T.` is stored as `T`.
    Enum(String),
    /// Entity reference `#123`.
    Ref(i64),
    /// `$` omitted/null.
    Null,
    /// `*` derived.
    Derived,
    /// `( ... )` aggregate.
    List(Vec<Token>),
    /// `KEYWORD( ... )` typed/simple value, e.g. `IFCLENGTHMEASURE(1.0)`.
    Typed(String, Vec<Token>),
}

/// One STEP entity instance record.
#[derive(Debug, Clone)]
pub struct Record {
    pub step_id: i64,
    pub keyword: String,
    pub attrs: Vec<Token>,
}

pub struct Parser<'a> {
    bytes: &'a [u8],
    pos: usize,
    id_spans: Option<Vec<(usize, usize, i64)>>,
    current_id: Option<i64>,
    last_id: Option<i64>,
}

impl<'a> Parser<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Parser { bytes, pos: 0, id_spans: None, current_id: None, last_id: None }
    }

    /// Preserve literal spellings; only definition/reference integers may change.
    pub fn rekey(mut self, mapping: &HashMap<i64, i64>) -> Result<Vec<u8>, String> {
        self.seek_data_section()?;
        self.skip_trivia()?;
        let prefix_end = self.pos;
        let mut records = Vec::new();
        let mut seen = HashSet::new();
        while self.peek() == Some(b'#') {
            let start = self.pos;
            self.id_spans = Some(Vec::new());
            let record = self
                .parse_record()?
                .ok_or_else(|| self.error_at(start, "expected entity definition"))?;
            if !seen.insert(record.step_id) {
                return Err(format!("duplicate definition of #{} at byte {start} in record #{}", record.step_id, record.step_id));
            }
            self.skip_trivia()?;
            let end = self.pos;
            let mut rewritten = Vec::new();
            let mut cursor = start;
            for (a, b, id) in self.id_spans.take().unwrap() {
                let target = mapping.get(&id).ok_or_else(|| self.error_at(a, format!("unmapped reference #{id}")))?;
                rewritten.extend_from_slice(&self.bytes[cursor..a]);
                rewritten.extend_from_slice(target.to_string().as_bytes());
                cursor = b;
            }
            rewritten.extend_from_slice(&self.bytes[cursor..end]);
            records.push((mapping[&record.step_id], rewritten));
        }
        let suffix_start = self.pos;
        if self.read_keyword() != "ENDSEC" || seen.len() != mapping.len() {
            return Err(self.error_at(suffix_start, "rekey requires one complete DATA section and a total entity mapping"));
        }
        self.skip_trivia()?;
        self.expect(b';')?;
        self.pos = suffix_start;
        records.sort_by_key(|(id, _)| *id);
        let mut output = self.bytes[..prefix_end].to_vec();
        for (_, record) in records {
            output.extend(record);
            // Adjacent records without trivia must remain separable by text merge tools.
            if output.last() != Some(&b'\n') {
                output.push(b'\n');
            }
        }
        output.extend_from_slice(&self.bytes[self.pos..]);
        Ok(output)
    }

    /// Parse every `#id = KEYWORD(...)` record of the DATA section, up to and
    /// including its `ENDSEC;`.
    pub fn parse(mut self) -> Result<Vec<Record>, String> {
        self.seek_data_section()?;
        let mut records = Vec::new();
        let mut seen = HashSet::new();
        loop {
            self.skip_trivia()?;
            let start = self.pos;
            match self.peek() {
                None => return Err(self.error_at(start, "DATA section ends without ENDSEC")),
                Some(b'#') => {
                    if let Some(rec) = self.parse_record()? {
                        if !seen.insert(rec.step_id) {
                            return Err(format!("duplicate definition of #{} at byte {start} in record #{}", rec.step_id, rec.step_id));
                        }
                        records.push(rec);
                    }
                }
                Some(b) if is_keyword_start(b) => {
                    let keyword = self.read_keyword();
                    if keyword != "ENDSEC" {
                        return Err(self.error_at(start, format!("unexpected keyword {keyword} in DATA section")));
                    }
                    self.skip_trivia()?;
                    self.expect(b';')?;
                    return Ok(records);
                }
                found => {
                    return Err(self.error_at(start, format!("unexpected {} in DATA section", describe(found))));
                }
            }
        }
    }

    /// Position just after the `DATA;` statement (`DATA(...);` in Part 21
    /// edition 3) that follows the header's `ENDSEC;`.
    fn seek_data_section(&mut self) -> Result<(), String> {
        let mut in_header = false;
        loop {
            self.skip_trivia()?;
            if self.peek().is_none() {
                return Err(self.error_at(self.pos, "no DATA section"));
            }
            match self.read_keyword().as_str() {
                "DATA" if !in_header => {
                    self.skip_trivia()?;
                    if self.peek() == Some(b'(') {
                        self.read_list()?;
                        self.skip_trivia()?;
                    }
                    return self.expect(b';');
                }
                "HEADER" => in_header = true,
                "ENDSEC" => in_header = false,
                _ => {}
            }
            self.skip_statement()?;
        }
    }

    /// Skip a header statement through its `;`, stepping over strings and comments.
    fn skip_statement(&mut self) -> Result<(), String> {
        loop {
            self.skip_trivia()?;
            match self.peek() {
                None => return Err(self.error_at(self.pos, "no DATA section")),
                Some(b'\'') => {
                    self.read_string()?;
                }
                Some(b';') => {
                    self.pos += 1;
                    return Ok(());
                }
                Some(_) => self.pos += 1,
            }
        }
    }

    fn parse_record(&mut self) -> Result<Option<Record>, String> {
        // At '#'.
        self.pos += 1;
        let step_id = self.read_int_literal()?;
        self.current_id = Some(step_id);
        self.skip_trivia()?;
        if self.peek() == Some(b';') {
            // `#12;` reference-only line; not a record.
            self.pos += 1;
            self.current_id = None;
            return Ok(None);
        }
        self.expect(b'=')?;
        self.skip_trivia()?;
        if self.peek() == Some(b'(') {
            return Err(self.error_at(self.pos, "complex entity instance not supported"));
        }
        let keyword_start = self.pos;
        let keyword = self.read_keyword();
        if keyword.is_empty() {
            let found = describe(self.peek());
            return Err(self.error_at(keyword_start, format!("expected entity keyword, found {found}")));
        }
        self.skip_trivia()?;
        if self.peek() != Some(b'(') {
            let found = describe(self.peek());
            return Err(self.error_at(self.pos, format!("expected '(' after {keyword}, found {found}")));
        }
        let attrs = self.read_list()?;
        self.skip_trivia()?;
        self.expect(b';')?;
        self.last_id = self.current_id.take();
        Ok(Some(Record { step_id, keyword, attrs }))
    }

    /// Read a parenthesized, comma-separated token list. Assumes current is '('.
    fn read_list(&mut self) -> Result<Vec<Token>, String> {
        let start = self.pos;
        self.pos += 1; // '('
        let mut items = Vec::new();
        self.skip_trivia()?;
        if self.peek() == Some(b')') {
            self.pos += 1;
            return Ok(items);
        }
        loop {
            items.push(self.read_token()?);
            self.skip_trivia()?;
            match self.peek() {
                Some(b',') => self.pos += 1,
                Some(b')') => {
                    self.pos += 1;
                    return Ok(items);
                }
                None => return Err(self.error_at(start, "unterminated list")),
                found => {
                    return Err(self.error_at(self.pos, format!("expected ',' or ')' in list, found {}", describe(found))));
                }
            }
        }
    }

    fn read_token(&mut self) -> Result<Token, String> {
        self.skip_trivia()?;
        let b = match self.peek() {
            Some(b) => b,
            None => return Err(self.error_at(self.pos, "unexpected end of input")),
        };
        match b {
            b'#' => {
                self.pos += 1;
                Ok(Token::Ref(self.read_int_literal()?))
            }
            b'\'' => Ok(Token::Str(self.read_string()?)),
            b'$' => {
                self.pos += 1;
                Ok(Token::Null)
            }
            b'*' => {
                self.pos += 1;
                Ok(Token::Derived)
            }
            b'(' => Ok(Token::List(self.read_list()?)),
            b'.' => Ok(Token::Enum(self.read_enum()?)),
            b'-' | b'+' | b'0'..=b'9' => self.read_number(),
            _ if is_keyword_start(b) => {
                let kw = self.read_keyword();
                self.skip_trivia()?;
                if self.peek() == Some(b'(') {
                    let inner = self.read_list()?;
                    Ok(Token::Typed(kw, inner))
                } else {
                    // Bare keyword (rare) — treat as enum-like atom.
                    Ok(Token::Enum(kw))
                }
            }
            other => Err(self.error_at(self.pos, format!("unexpected {} in value", describe(Some(other))))),
        }
    }

    fn read_number(&mut self) -> Result<Token, String> {
        let start = self.pos;
        let mut is_real = false;
        if matches!(self.peek(), Some(b'-') | Some(b'+')) {
            self.pos += 1;
        }
        while let Some(b) = self.peek() {
            match b {
                b'0'..=b'9' => self.pos += 1,
                b'.' => {
                    is_real = true;
                    self.pos += 1;
                }
                b'e' | b'E' => {
                    is_real = true;
                    self.pos += 1;
                    if matches!(self.peek(), Some(b'-') | Some(b'+')) {
                        self.pos += 1;
                    }
                }
                _ => break,
            }
        }
        let text = std::str::from_utf8(&self.bytes[start..self.pos]).expect("ASCII bytes");
        if is_real {
            text.parse::<f64>()
                .map(Token::Real)
                .map_err(|_| self.error_at(start, format!("bad real literal {text:?}")))
        } else {
            text.parse::<i64>()
                .map(Token::Int)
                .map_err(|_| self.error_at(start, format!("bad int literal {text:?}")))
        }
    }

    fn read_int_literal(&mut self) -> Result<i64, String> {
        let start = self.pos;
        if matches!(self.peek(), Some(b'-') | Some(b'+')) {
            self.pos += 1;
        }
        let digits_start = self.pos;
        while matches!(self.peek(), Some(b'0'..=b'9')) {
            self.pos += 1;
        }
        if self.pos == digits_start {
            let found = describe(self.peek());
            return Err(self.error_at(self.pos, format!("expected digits after '#', found {found}")));
        }
        let text = std::str::from_utf8(&self.bytes[start..self.pos]).expect("ASCII bytes");
        // Lenient on overflow: a corrupted/mangled id (e.g. a 23-digit ref that
        // some tools emit when a reference is broken) parses to a sentinel that
        // no real record carries, so it later resolves as a dangling reference
        // and is dropped — mirroring ifcopenshell's load-time repair.
        let id = text.parse::<i64>().unwrap_or(i64::MIN);
        if let Some(spans) = &mut self.id_spans {
            spans.push((start, self.pos, id));
        }
        Ok(id)
    }

    /// Read a `'...'` string; `''` is an escaped single quote. The closing
    /// quote is found by byte: 0x27 never occurs inside a multi-byte UTF-8
    /// sequence, and raw 8-bit bytes need not form one.
    fn read_string(&mut self) -> Result<String, String> {
        let start = self.pos;
        self.pos += 1; // opening quote
        let mut raw = Vec::new();
        loop {
            let rest = &self.bytes[self.pos..];
            let Some(quote) = rest.iter().position(|&b| b == b'\'') else {
                return Err(self.error_at(start, "unterminated string"));
            };
            raw.extend_from_slice(&rest[..quote]);
            self.pos += quote + 1;
            if self.peek() == Some(b'\'') {
                raw.push(b'\'');
                self.pos += 1;
            } else {
                break;
            }
        }
        Ok(decode_control_directives(&decode_string_bytes(raw)))
    }

    fn read_enum(&mut self) -> Result<String, String> {
        let start = self.pos;
        self.pos += 1; // leading '.'
        let text = self.read_keyword();
        if text.is_empty() || self.peek() != Some(b'.') {
            return Err(self.error_at(start, "malformed enumeration value"));
        }
        self.pos += 1; // trailing '.'
        Ok(text)
    }

    /// Read a keyword, normalized to upper case.
    fn read_keyword(&mut self) -> String {
        let start = self.pos;
        while matches!(self.peek(), Some(b) if is_keyword_char(b)) {
            self.pos += 1;
        }
        self.bytes[start..self.pos].iter().map(|b| char::from(b.to_ascii_uppercase())).collect()
    }

    fn skip_trivia(&mut self) -> Result<(), String> {
        loop {
            match self.peek() {
                Some(b) if b.is_ascii_whitespace() => self.pos += 1,
                Some(b'/') if self.bytes.get(self.pos + 1) == Some(&b'*') => {
                    let start = self.pos;
                    self.pos += 2;
                    while self.pos < self.bytes.len()
                        && !(self.bytes[self.pos] == b'*'
                            && self.bytes.get(self.pos + 1) == Some(&b'/'))
                    {
                        self.pos += 1;
                    }
                    if self.pos == self.bytes.len() {
                        return Err(self.error_at(start, "unterminated comment"));
                    }
                    self.pos += 2;
                }
                _ => return Ok(()),
            }
        }
    }

    fn expect(&mut self, wanted: u8) -> Result<(), String> {
        match self.peek() {
            Some(b) if b == wanted => {
                self.pos += 1;
                Ok(())
            }
            found => {
                let message = format!("expected '{}', found {}", char::from(wanted), describe(found));
                Err(self.error_at(self.pos, message))
            }
        }
    }

    fn error_at(&self, offset: usize, message: impl std::fmt::Display) -> String {
        match (self.current_id, self.last_id) {
            (Some(id), _) => format!("{message} at byte {offset} in record #{id}"),
            (None, Some(id)) => format!("{message} at byte {offset} after record #{id}"),
            (None, None) => format!("{message} at byte {offset} before the first record"),
        }
    }

    #[inline]
    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.pos).copied()
    }
}

fn describe(byte: Option<u8>) -> String {
    match byte {
        None => "end of input".to_string(),
        Some(b) if b.is_ascii_graphic() => format!("'{}'", char::from(b)),
        Some(b) => format!("byte 0x{b:02X}"),
    }
}

#[inline]
fn is_keyword_start(b: u8) -> bool {
    b.is_ascii_alphabetic() || b == b'_'
}

#[inline]
fn is_keyword_char(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// Valid UTF-8 sequences decode as UTF-8; every other byte decodes as its
/// Latin-1 code point.
fn decode_string_bytes(raw: Vec<u8>) -> String {
    match String::from_utf8(raw) {
        Ok(text) => text,
        Err(err) => {
            let raw = err.into_bytes();
            let mut out = String::with_capacity(raw.len() * 2);
            for chunk in raw.utf8_chunks() {
                out.push_str(chunk.valid());
                out.extend(chunk.invalid().iter().map(|&b| char::from(b)));
            }
            out
        }
    }
}

#[inline]
fn utf8_len(first: u8) -> usize {
    if first < 0x80 {
        1
    } else if first >> 5 == 0b110 {
        2
    } else if first >> 4 == 0b1110 {
        3
    } else if first >> 3 == 0b11110 {
        4
    } else {
        1
    }
}

/// Decode ISO 10303-21 string control directives (`\\`, `\X\hh`, `\X2\…\X0\`,
/// `\X4\…\X0\`, `\S\c`, `\P?\`) so one character has one representation
/// whichever encoding the exporter chose. Malformed directives, and `\S\`
/// under a code page other than ISO 8859-1, stay verbatim (including the
/// `\P?\` directive that selected the page).
fn decode_control_directives(raw: &str) -> String {
    if !raw.contains('\\') {
        return raw.to_string();
    }
    let b = raw.as_bytes();
    let mut out = String::with_capacity(raw.len());
    let mut latin1_page = true;
    let mut i = 0;
    while i < b.len() {
        if b[i] != b'\\' {
            let len = utf8_len(b[i]).min(b.len() - i);
            out.push_str(&raw[i..i + len]);
            i += len;
            continue;
        }
        let rest = &b[i..];
        if rest.starts_with(b"\\\\") {
            out.push('\\');
            i += 2;
            continue;
        }
        if rest.starts_with(b"\\X2\\") || rest.starts_with(b"\\X4\\") {
            let width = if rest[2] == b'2' { 4 } else { 8 };
            if let Some((decoded, used)) = decode_hex_run(&rest[4..], width) {
                out.push_str(&decoded);
                i += 4 + used;
                continue;
            }
        } else if rest.starts_with(b"\\X\\") && rest.len() >= 5 {
            if let Some(c) = hex_value(&rest[3..5]).and_then(char::from_u32) {
                out.push(c);
                i += 5;
                continue;
            }
        } else if rest.starts_with(b"\\S\\") && rest.len() >= 4 && latin1_page && rest[3] < 0x80 {
            out.push(char::from(rest[3] + 128));
            i += 4;
            continue;
        } else if rest.len() >= 4 && rest[1] == b'P' && (b'A'..=b'I').contains(&rest[2]) && rest[3] == b'\\' {
            latin1_page = rest[2] == b'A';
            if !latin1_page {
                out.push_str(&raw[i..i + 4]);
            }
            i += 4;
            continue;
        }
        out.push('\\');
        i += 1;
    }
    out
}

/// Decode `width`-digit hex code units up to the closing `\X0\`; returns the
/// text and the bytes consumed including the terminator.
fn decode_hex_run(b: &[u8], width: usize) -> Option<(String, usize)> {
    let mut units: Vec<u32> = Vec::new();
    let mut i = 0;
    while !b[i..].starts_with(b"\\X0\\") {
        units.push(hex_value(b.get(i..i + width)?)?);
        i += width;
    }
    let text = if width == 4 {
        let utf16: Vec<u16> = units.iter().map(|&u| u as u16).collect();
        String::from_utf16(&utf16).ok()?
    } else {
        units.into_iter().map(char::from_u32).collect::<Option<String>>()?
    };
    Some((text, i + 4))
}

fn hex_value(digits: &[u8]) -> Option<u32> {
    let s = std::str::from_utf8(digits).ok()?;
    if !s.bytes().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    u32::from_str_radix(s, 16).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_simple_records() {
        let src = b"\
ISO-10303-21;
HEADER;
ENDSEC;
DATA;
#1= IFCCARTESIANPOINT((0.,0.,0.));
#2=IFCDIRECTION((1.,0.,0.));
#3= IFCWALL('0abc',$,'My Wall',$,$,#10,#11,.ELEMENTEDCASE.);
#4= IFCPROPERTYSINGLEVALUE('P',$,IFCLABEL('x'),$);
ENDSEC;
END-ISO-10303-21;
";
        let recs = Parser::new(src).parse().expect("parse ok");
        assert_eq!(recs.len(), 4);
        assert_eq!(recs[0].step_id, 1);
        assert_eq!(recs[0].keyword, "IFCCARTESIANPOINT");
        match &recs[0].attrs[0] {
            Token::List(items) => {
                assert_eq!(items.len(), 3);
                assert_eq!(items[0], Token::Real(0.0));
            }
            other => panic!("expected list, got {other:?}"),
        }
        // wall: string, null, string, null, null, ref, ref, enum
        let w = &recs[2];
        assert_eq!(w.keyword, "IFCWALL");
        assert_eq!(w.attrs[0], Token::Str("0abc".to_string()));
        assert_eq!(w.attrs[1], Token::Null);
        assert_eq!(w.attrs[2], Token::Str("My Wall".to_string()));
        assert_eq!(w.attrs[5], Token::Ref(10));
        assert_eq!(w.attrs[7], Token::Enum("ELEMENTEDCASE".to_string()));
        // typed value inside property
        match &recs[3].attrs[2] {
            Token::Typed(kw, inner) => {
                assert_eq!(kw, "IFCLABEL");
                assert_eq!(inner[0], Token::Str("x".to_string()));
            }
            other => panic!("expected typed, got {other:?}"),
        }
    }

    #[test]
    fn handles_escaped_quotes_and_comments() {
        let src = b"DATA;\n#1= IFCLABEL('it''s'); /* c */ #2=IFCBOOLEAN(.T.);\nENDSEC;";
        let recs = Parser::new(src).parse().expect("ok");
        assert_eq!(recs[0].attrs[0], Token::Str("it's".to_string()));
        assert_eq!(recs[1].attrs[0], Token::Enum("T".to_string()));
    }

    #[test]
    fn decodes_string_control_directives() {
        for raw in [
            "st\u{e5}l",
            "st\\X\\E5l",
            "st\\S\\el",
            "st\\PA\\\\S\\el",
            "st\\X2\\00E5\\X0\\l",
            "st\\X4\\000000E5\\X0\\l",
        ] {
            assert_eq!(decode_control_directives(raw), "st\u{e5}l", "{raw}");
        }
        assert_eq!(decode_control_directives("\\X2\\D83DDE00\\X0\\"), "\u{1f600}");
        assert_eq!(decode_control_directives("a\\\\b"), "a\\b");
        assert_eq!(decode_control_directives("\\X2\\00E\\X0\\"), "\\X2\\00E\\X0\\");
        assert_eq!(decode_control_directives("\\PB\\\\S\\e"), "\\PB\\\\S\\e");
    }

    #[test]
    fn parsed_strings_have_one_form_per_character() {
        let src = b"DATA;\n#1=IFCLABEL('st\\X2\\00E5\\X0\\l');\n#2=IFCLABEL('st\\X\\E5l');\n#3=IFCLABEL('st\xc3\xa5l');\n#4=IFCLABEL('st\xe5l');\nENDSEC;";
        let recs = Parser::new(src).parse().expect("ok");
        assert_eq!(recs.len(), 4);
        for rec in &recs {
            assert_eq!(rec.attrs[0], Token::Str("st\u{e5}l".to_string()));
        }
    }

    fn strings(src: &[u8]) -> Vec<Token> {
        Parser::new(src).parse().expect("ok").into_iter().map(|rec| rec.attrs[0].clone()).collect()
    }

    fn parse_error(src: &[u8]) -> String {
        Parser::new(src).parse().expect_err("parse must fail")
    }

    #[test]
    fn raw_latin1_bytes_decode_like_their_escapes() {
        let src = b"DATA;\n#1=IFCLABEL('m\xb2');\n#2=IFCLABEL('m\\X\\B2');\n#3=IFCLABEL('m\xb3');\n\
#4=IFCLABEL('H\xf6he');\n#5=IFCLABEL('H\\X\\F6he');\nENDSEC;";
        let got = strings(src);
        assert_eq!(got[0], Token::Str("m\u{b2}".to_string()));
        assert_eq!(got[0], got[1]);
        assert_eq!(got[2], Token::Str("m\u{b3}".to_string()));
        assert_eq!(got[3], Token::Str("H\u{f6}he".to_string()));
        assert_eq!(got[3], got[4]);
    }

    #[test]
    fn cp1252_byte_0x80_decodes_as_its_latin1_code_point() {
        let got = strings(b"DATA;\n#1=IFCLABEL('a\x80b');\n#2=IFCLABEL('a\\X\\80b');\nENDSEC;");
        assert_eq!(got[0], Token::Str("a\u{80}b".to_string()));
        assert_eq!(got[0], got[1]);
    }

    #[test]
    fn lone_lead_byte_before_closing_quote_keeps_the_quote() {
        let got = strings(b"DATA;\n#1=IFCLABEL('ab\xc3');\n#2=IFCLABEL('\xe2\x82');\n#3=IFCLABEL('z');\nENDSEC;");
        assert_eq!(
            got,
            vec![
                Token::Str("ab\u{c3}".to_string()),
                Token::Str("\u{e2}\u{82}".to_string()),
                Token::Str("z".to_string()),
            ]
        );
    }

    #[test]
    fn valid_utf8_is_untouched() {
        let got = strings("DATA;\n#1=IFCLABEL('Höhe \u{1f600} 漢');\nENDSEC;".as_bytes());
        assert_eq!(got[0], Token::Str("Höhe \u{1f600} 漢".to_string()));
    }

    #[test]
    fn data_inside_header_strings_and_comments_is_not_the_data_section() {
        let src = b"\
ISO-10303-21;
HEADER;
/* DATA; */
FILE_DESCRIPTION(('DATA; #9=IFCWALL(1);'),'2;1');
FILE_NAME('C:\\\\DATA;\\\\x.ifc','',(''),(''),'','','');
FILE_SCHEMA(('IFC4'));
ENDSEC;
DATA;
#1=IFCPROJECT('g');
ENDSEC;
END-ISO-10303-21;
";
        let recs = Parser::new(src).parse().expect("ok");
        assert_eq!(recs.len(), 1);
        assert_eq!((recs[0].step_id, recs[0].keyword.as_str()), (1, "IFCPROJECT"));
    }

    #[test]
    fn file_without_data_section_is_an_error() {
        let err = parse_error(b"ISO-10303-21;\nHEADER;\nFILE_DESCRIPTION(('x'),'2;1');\nENDSEC;\n#1=IFCWALL('g');\n");
        assert!(err.starts_with("no DATA section at byte "), "{err}");
    }

    #[test]
    fn keywords_and_enums_are_case_insensitive() {
        let src = b"DATA;\n#1=IfcWall('g',.notDefined.,ifcLabel('x'),.t.);\nendsec;";
        let rec = &Parser::new(src).parse().expect("ok")[0];
        assert_eq!(rec.keyword, "IFCWALL");
        assert_eq!(rec.attrs[1], Token::Enum("NOTDEFINED".to_string()));
        assert_eq!(rec.attrs[2], Token::Typed("IFCLABEL".to_string(), vec![Token::Str("x".to_string())]));
        assert_eq!(rec.attrs[3], Token::Enum("T".to_string()));
    }

    #[test]
    fn duplicate_definition_is_an_error_naming_the_id() {
        let err = parse_error(b"DATA;\n#7=IFCX(1);\n#8=IFCX(2);\n#7=IFCX(3);\nENDSEC;");
        assert_eq!(err, "duplicate definition of #7 at byte 30 in record #7");
    }

    #[test]
    fn complex_instance_is_rejected_with_id_and_offset() {
        let err = parse_error(b"DATA;\n#1=IFCX(1);\n#2=(IFCA(1)IFCB(2));\nENDSEC;");
        assert_eq!(err, "complex entity instance not supported at byte 21 in record #2");
    }

    #[test]
    fn errors_carry_offset_record_and_hex_byte() {
        let src = b"DATA;\n#1=IFCX(1);\n#2=IFCY(\x01);\nENDSEC;";
        assert_eq!(parse_error(src), "unexpected byte 0x01 in value at byte 26 in record #2");
        let src = b"DATA;\n#1=IFCX(1);\n#2=IFCY('a' m);\nENDSEC;";
        assert_eq!(parse_error(src), "expected ',' or ')' in list, found 'm' at byte 30 in record #2");
        let src = b"DATA;\n#1=IFCX(1);\n#2=IFCY('abc";
        assert_eq!(parse_error(src), "unterminated string at byte 26 in record #2");
        let src = b"DATA;\n#1=IFCX(1);\n";
        assert_eq!(parse_error(src), "DATA section ends without ENDSEC at byte 18 after record #1");
        let src = b"DATA;\nIFCX(1);\nENDSEC;";
        assert_eq!(parse_error(src), "unexpected keyword IFCX in DATA section at byte 6 before the first record");
        let src = b"DATA;\n#1=IFCX(1)\n#2=IFCX(2);\nENDSEC;";
        assert_eq!(parse_error(src), "expected ';', found '#' at byte 17 in record #1");
    }

    #[test]
    fn rekey_preserves_mixed_case_and_raw_literal_bytes() {
        let src = b"DATA;\r\n#9=IfcLabel('H\xf6he',ifcReal(1.00E+0),.t.);\r\nendsec /* x */;\r\n";
        let expected = b"DATA;\r\n#2=IfcLabel('H\xf6he',ifcReal(1.00E+0),.t.);\r\nendsec /* x */;\r\n";
        assert_eq!(Parser::new(src).rekey(&HashMap::from([(9, 2)])).unwrap(), expected);
        let err = Parser::new(b"DATA;#9=IFCX();ENDSEC_MISSPELLED;")
            .rekey(&HashMap::from([(9, 2)])).unwrap_err();
        assert!(err.contains("at byte 15 after record #9"), "{err}");
    }

    #[test]
    fn unterminated_comment_carries_location() {
        assert_eq!(parse_error(b"DATA;#7=IFCX(/*"), "unterminated comment at byte 13 in record #7");
        assert_eq!(parse_error(b"DATA;#7=IFCX();/*"), "unterminated comment at byte 15 after record #7");
    }

    #[test]
    fn parses_number_forms() {
        // Per ISO-10303-21 a real always has a digit before the '.', so `4.` is
        // valid but `.5` is not — the grammar this tokenizer targets.
        let src = b"DATA;\n#1=T((1,-2,+3,1.5,-0.25,2.5E-3,4.));\nENDSEC;";
        let recs = Parser::new(src).parse().expect("ok");
        let items = match &recs[0].attrs[0] {
            Token::List(items) => items,
            other => panic!("expected list, got {other:?}"),
        };
        assert_eq!(items[0], Token::Int(1));
        assert_eq!(items[1], Token::Int(-2));
        assert_eq!(items[2], Token::Int(3));
        assert_eq!(items[3], Token::Real(1.5));
        assert_eq!(items[4], Token::Real(-0.25));
        assert_eq!(items[5], Token::Real(0.0025));
        assert_eq!(items[6], Token::Real(4.0));
    }

    #[test]
    fn parses_nested_lists_null_and_derived() {
        let src = b"DATA;\n#1=IFCX((#2,#3),$,*,());\nENDSEC;";
        let rec = &Parser::new(src).parse().expect("ok")[0];
        match &rec.attrs[0] {
            Token::List(inner) => {
                assert_eq!(inner[0], Token::Ref(2));
                assert_eq!(inner[1], Token::Ref(3));
            }
            other => panic!("expected nested list, got {other:?}"),
        }
        assert_eq!(rec.attrs[1], Token::Null);
        assert_eq!(rec.attrs[2], Token::Derived);
        assert_eq!(rec.attrs[3], Token::List(vec![]));
    }

    #[test]
    fn record_can_span_multiple_lines() {
        let src = b"DATA;\n#1=IFCWALL('g',\n  $,\n  'Name'\n);\nENDSEC;";
        let rec = &Parser::new(src).parse().expect("ok")[0];
        assert_eq!(rec.keyword, "IFCWALL");
        assert_eq!(rec.attrs.len(), 3);
        assert_eq!(rec.attrs[2], Token::Str("Name".to_string()));
    }

    #[test]
    fn skips_reference_only_lines() {
        // A bare `#id;` with no `= KEYWORD(...)` is not a record.
        let src = b"DATA;\n#5;\n#1=IFCWALL('g');\nENDSEC;";
        let recs = Parser::new(src).parse().expect("ok");
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].step_id, 1);
    }

    #[test]
    fn oversized_ref_becomes_sentinel() {
        // A 23-digit id overflows i64; it parses to the i64::MIN sentinel so it
        // later resolves as a dangling reference rather than crashing.
        let src = b"DATA;\n#1=IFCWALL(#99999999999999999999999);\nENDSEC;";
        let rec = &Parser::new(src).parse().expect("ok")[0];
        assert_eq!(rec.attrs[0], Token::Ref(i64::MIN));
    }

    #[test]
    fn stops_at_endsec_and_ignores_header() {
        let src = b"\
ISO-10303-21;
HEADER;
FILE_NAME('skip',$);
ENDSEC;
DATA;
#1=IFCPROJECT('g');
ENDSEC;
END-ISO-10303-21;
";
        let recs = Parser::new(src).parse().expect("ok");
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].keyword, "IFCPROJECT");
    }
}
