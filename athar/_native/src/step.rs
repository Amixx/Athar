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
//!   * Arguments: integers, reals, strings (`'...'` with `''` escape), enums
//!     (`.T.`), entity refs (`#123`), `$` (null), `*` (derived), nested lists
//!     `( ... )`, and typed values `KEYWORD( ... )`.
//!
//! What it does NOT do (by design — handled elsewhere or unsupported):
//!   * STEP string unescaping of `\X\` / `\X2\` / `\S\` encodings — strings are
//!     carried through verbatim and NFC-normalized later; this is revisited
//!     when a corpus file needs it.
//!   * The HEADER section beyond locating `DATA;`.

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
}

impl<'a> Parser<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Parser { bytes, pos: 0 }
    }

    /// Parse the whole file, returning every `#id = KEYWORD(...)` record in the
    /// DATA section. Definitions outside DATA (header) are skipped.
    pub fn parse(mut self) -> Result<Vec<Record>, String> {
        self.seek_data_section();
        let mut records = Vec::new();
        loop {
            self.skip_trivia();
            if self.pos >= self.bytes.len() {
                break;
            }
            let b = self.bytes[self.pos];
            if b == b'#' {
                if let Some(rec) = self.parse_record()? {
                    records.push(rec);
                }
            } else if self.looks_at(b"ENDSEC") {
                break;
            } else {
                // Unknown construct at top level: skip to the next ';'.
                self.skip_to_semicolon();
            }
        }
        Ok(records)
    }

    fn seek_data_section(&mut self) {
        // Find "DATA;" (case-sensitive per spec); fall back to start if absent.
        if let Some(idx) = find_subsequence(self.bytes, b"DATA;") {
            self.pos = idx + b"DATA;".len();
        } else if let Some(idx) = find_subsequence(self.bytes, b"DATA ") {
            self.pos = idx;
        }
    }

    fn parse_record(&mut self) -> Result<Option<Record>, String> {
        // At '#'.
        self.pos += 1;
        let step_id = self.read_int_literal()?;
        self.skip_trivia();
        if self.peek() != Some(b'=') {
            // e.g. `#12;` reference-only line; skip.
            self.skip_to_semicolon();
            return Ok(None);
        }
        self.pos += 1; // '='
        self.skip_trivia();
        let keyword = self.read_keyword();
        if keyword.is_empty() {
            return Err(format!("missing keyword after #{step_id}="));
        }
        self.skip_trivia();
        let attrs = if self.peek() == Some(b'(') {
            self.read_list()?
        } else {
            Vec::new()
        };
        self.skip_trivia();
        if self.peek() == Some(b';') {
            self.pos += 1;
        }
        Ok(Some(Record {
            step_id,
            keyword,
            attrs,
        }))
    }

    /// Read a parenthesized, comma-separated token list. Assumes current is '('.
    fn read_list(&mut self) -> Result<Vec<Token>, String> {
        self.pos += 1; // '('
        let mut items = Vec::new();
        loop {
            self.skip_trivia();
            match self.peek() {
                None => return Err("unterminated list".to_string()),
                Some(b')') => {
                    self.pos += 1;
                    break;
                }
                Some(b',') => {
                    self.pos += 1;
                }
                _ => {
                    let tok = self.read_token()?;
                    items.push(tok);
                }
            }
        }
        Ok(items)
    }

    fn read_token(&mut self) -> Result<Token, String> {
        self.skip_trivia();
        let b = self.peek().ok_or("unexpected end of input")?;
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
            b'.' => Ok(Token::Enum(self.read_enum())),
            b'-' | b'+' | b'0'..=b'9' => self.read_number(),
            _ if is_keyword_start(b) => {
                let kw = self.read_keyword();
                self.skip_trivia();
                if self.peek() == Some(b'(') {
                    let inner = self.read_list()?;
                    Ok(Token::Typed(kw, inner))
                } else {
                    // Bare keyword (rare) — treat as enum-like atom.
                    Ok(Token::Enum(kw))
                }
            }
            other => Err(format!("unexpected byte '{}' in token", other as char)),
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
        let text = std::str::from_utf8(&self.bytes[start..self.pos])
            .map_err(|_| "invalid utf8 in number".to_string())?;
        if is_real {
            text.parse::<f64>()
                .map(Token::Real)
                .map_err(|_| format!("bad real literal {text:?}"))
        } else {
            text.parse::<i64>()
                .map(Token::Int)
                .map_err(|_| format!("bad int literal {text:?}"))
        }
    }

    fn read_int_literal(&mut self) -> Result<i64, String> {
        let start = self.pos;
        if matches!(self.peek(), Some(b'-') | Some(b'+')) {
            self.pos += 1;
        }
        while matches!(self.peek(), Some(b'0'..=b'9')) {
            self.pos += 1;
        }
        let text = std::str::from_utf8(&self.bytes[start..self.pos])
            .map_err(|_| "invalid utf8 in integer".to_string())?;
        // Lenient on overflow: a corrupted/mangled id (e.g. a 23-digit ref that
        // some tools emit when a reference is broken) parses to a sentinel that
        // no real record carries, so it later resolves as a dangling reference
        // and is dropped — mirroring ifcopenshell's load-time repair.
        Ok(text.parse::<i64>().unwrap_or(i64::MIN))
    }

    /// Read a `'...'` string; `''` is an escaped single quote.
    fn read_string(&mut self) -> Result<String, String> {
        self.pos += 1; // opening quote
        let mut out = String::new();
        loop {
            match self.peek() {
                None => return Err("unterminated string".to_string()),
                Some(b'\'') => {
                    if self.bytes.get(self.pos + 1) == Some(&b'\'') {
                        out.push('\'');
                        self.pos += 2;
                    } else {
                        self.pos += 1;
                        break;
                    }
                }
                Some(_) => {
                    // Copy a full UTF-8 byte sequence verbatim.
                    let ch_len = utf8_len(self.bytes[self.pos]);
                    let end = (self.pos + ch_len).min(self.bytes.len());
                    if let Ok(s) = std::str::from_utf8(&self.bytes[self.pos..end]) {
                        out.push_str(s);
                    }
                    self.pos = end;
                }
            }
        }
        Ok(out)
    }

    fn read_enum(&mut self) -> String {
        self.pos += 1; // leading '.'
        let start = self.pos;
        while let Some(b) = self.peek() {
            if b == b'.' {
                break;
            }
            self.pos += 1;
        }
        let text = String::from_utf8_lossy(&self.bytes[start..self.pos]).into_owned();
        if self.peek() == Some(b'.') {
            self.pos += 1; // trailing '.'
        }
        text
    }

    fn read_keyword(&mut self) -> String {
        let start = self.pos;
        while let Some(b) = self.peek() {
            if is_keyword_char(b) {
                self.pos += 1;
            } else {
                break;
            }
        }
        String::from_utf8_lossy(&self.bytes[start..self.pos]).into_owned()
    }

    fn skip_trivia(&mut self) {
        loop {
            match self.peek() {
                Some(b) if b.is_ascii_whitespace() => self.pos += 1,
                Some(b'/') if self.bytes.get(self.pos + 1) == Some(&b'*') => {
                    self.pos += 2;
                    while self.pos < self.bytes.len()
                        && !(self.bytes[self.pos] == b'*'
                            && self.bytes.get(self.pos + 1) == Some(&b'/'))
                    {
                        self.pos += 1;
                    }
                    self.pos = (self.pos + 2).min(self.bytes.len());
                }
                _ => break,
            }
        }
    }

    fn skip_to_semicolon(&mut self) {
        while let Some(b) = self.peek() {
            self.pos += 1;
            if b == b';' {
                break;
            }
        }
    }

    fn looks_at(&self, needle: &[u8]) -> bool {
        self.bytes[self.pos..].starts_with(needle)
    }

    #[inline]
    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.pos).copied()
    }
}

#[inline]
fn is_keyword_start(b: u8) -> bool {
    b.is_ascii_uppercase() || b == b'_'
}

#[inline]
fn is_keyword_char(b: u8) -> bool {
    b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_'
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

fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
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
