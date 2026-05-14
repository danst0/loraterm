//! Output → DM chunking pipeline.
//!
//! Responsibilities:
//! 1. Strip ANSI escape sequences from PTY output.
//! 2. Buffer raw bytes and flush on newline or after a silence window.
//! 3. Split into UTF-8-safe chunks of ≤ `max_chars` characters.
//! 4. Prepend `[i/N] ` when `number_chunks` and N > 1.
//! 5. Enforce a hard cap on chunks per output burst (drops the rest with a marker).

use std::time::{Duration, Instant};

/// Hard cap on lines emitted per single chunker flush. Anything beyond is
/// dropped and signalled with a trailing `[...]` marker.
const MAX_LINES_PER_FLUSH: usize = 10;

/// Configuration knobs for the chunker (subset of [`crate::config::ChunkingConfig`]).
#[derive(Debug, Clone, Copy)]
pub struct ChunkerOpts {
    pub max_chars: usize,
    pub number_chunks: bool,
    pub flush_silence: Duration,
    pub burst_chunk_cap: usize,
}

impl Default for ChunkerOpts {
    fn default() -> Self {
        Self {
            max_chars: 120,
            number_chunks: true,
            flush_silence: Duration::from_millis(600),
            burst_chunk_cap: 50,
        }
    }
}

/// Stateful chunker — push bytes, get chunks back when flushable.
#[derive(Debug)]
pub struct Chunker {
    opts: ChunkerOpts,
    buf: Vec<u8>,
    last_push: Option<Instant>,
}

impl Chunker {
    pub fn new(opts: ChunkerOpts) -> Self {
        Self {
            opts,
            buf: Vec::new(),
            last_push: None,
        }
    }

    /// Append raw bytes; returns chunks if a newline was seen (immediate flush).
    pub fn push(&mut self, bytes: &[u8]) -> Vec<String> {
        self.buf.extend_from_slice(bytes);
        self.last_push = Some(Instant::now());
        if self.buf.contains(&b'\n') {
            self.flush_committed()
        } else {
            Vec::new()
        }
    }

    /// Flush after silence threshold elapsed.
    pub fn maybe_flush(&mut self, now: Instant) -> Vec<String> {
        if self.buf.is_empty() {
            return Vec::new();
        }
        match self.last_push {
            Some(t) if now.duration_since(t) >= self.opts.flush_silence => self.drain_all(),
            _ => Vec::new(),
        }
    }

    /// Force-flush everything (used at PTY EOF / shutdown).
    pub fn flush_force(&mut self) -> Vec<String> {
        self.drain_all()
    }

    /// Time when the chunker would next want to be woken (i.e. silence-deadline).
    pub fn next_deadline(&self) -> Option<Instant> {
        self.last_push.map(|t| t + self.opts.flush_silence)
    }

    /// Flush everything up to (and including) the last newline; keep the tail buffered.
    fn flush_committed(&mut self) -> Vec<String> {
        let last_nl = match self.buf.iter().rposition(|b| *b == b'\n') {
            Some(p) => p,
            None => return Vec::new(),
        };
        let mut head: Vec<u8> = self.buf.drain(..=last_nl).collect();
        self.normalize_text(&mut head);
        let text = String::from_utf8_lossy(&head).replace('\u{FFFD}', "?");
        chunks_from_text(&text, self.opts)
    }

    fn drain_all(&mut self) -> Vec<String> {
        let mut bytes: Vec<u8> = std::mem::take(&mut self.buf);
        self.last_push = None;
        self.normalize_text(&mut bytes);
        let text = String::from_utf8_lossy(&bytes).replace('\u{FFFD}', "?");
        chunks_from_text(&text, self.opts)
    }

    fn normalize_text(&self, bytes: &mut Vec<u8>) {
        let stripped = strip_ansi_escapes::strip(&bytes);
        *bytes = stripped;
        // Replace bare CR (not part of CRLF) with LF
        let mut out = Vec::with_capacity(bytes.len());
        let mut iter = bytes.iter().peekable();
        while let Some(&b) = iter.next() {
            if b == b'\r' {
                if iter.peek().copied() == Some(&b'\n') {
                    iter.next();
                }
                out.push(b'\n');
            } else {
                out.push(b);
            }
        }
        *bytes = out;
    }
}

/// Convert a normalized text blob (ANSI-stripped, CR-cleaned) into chunks.
///
/// Trims trailing whitespace off each chunk; collapses runs of >2 blank lines to 2.
pub fn chunks_from_text(text: &str, opts: ChunkerOpts) -> Vec<String> {
    if text.trim().is_empty() {
        return Vec::new();
    }

    // Collapse triple+ blank lines.
    let cleaned = collapse_blank_lines(text);

    // Line cap: keep at most the first N lines and signal truncation with `[...]`.
    let line_capped = cap_lines(&cleaned, MAX_LINES_PER_FLUSH);

    // Reserve room for `[i/N] ` prefix worst-case (≤ 10 chars).
    let prefix_budget = if opts.number_chunks { 10 } else { 0 };
    let inner_max = opts.max_chars.saturating_sub(prefix_budget).max(20);

    let mut raw_chunks = split_into_char_chunks(&line_capped, inner_max);

    // Drop empty / pure-whitespace chunks
    raw_chunks.retain(|c| !c.trim().is_empty());

    // Enforce burst cap.
    let dropped_count = if raw_chunks.len() > opts.burst_chunk_cap {
        let dropped = raw_chunks.len() - opts.burst_chunk_cap;
        raw_chunks.truncate(opts.burst_chunk_cap);
        Some(dropped)
    } else {
        None
    };

    let n = raw_chunks.len() + dropped_count.map_or(0, |_| 1);
    let mut out: Vec<String> = if opts.number_chunks && n > 1 {
        raw_chunks
            .into_iter()
            .enumerate()
            .map(|(i, body)| format!("[{}/{}] {}", i + 1, n, body))
            .collect()
    } else {
        raw_chunks
    };

    if let Some(dropped) = dropped_count {
        let marker = format!(
            "[output truncated — {} more chunks dropped]",
            dropped
        );
        out.push(if opts.number_chunks && n > 1 {
            format!("[{}/{}] {}", n, n, marker)
        } else {
            marker
        });
    }

    out
}

/// Keep only the first `max_lines` newline-terminated lines. If more were present,
/// append a literal "[...]" marker line. A non-newline-terminated trailing line
/// counts as a line for the purpose of this cap.
fn cap_lines(text: &str, max_lines: usize) -> String {
    let total = text.bytes().filter(|&b| b == b'\n').count()
        + if !text.is_empty() && !text.ends_with('\n') { 1 } else { 0 };
    if total <= max_lines {
        return text.to_owned();
    }

    let mut seen = 0usize;
    let mut cut_at = text.len();
    for (i, b) in text.bytes().enumerate() {
        if b == b'\n' {
            seen += 1;
            if seen == max_lines {
                cut_at = i + 1;
                break;
            }
        }
    }

    let mut out = String::with_capacity(cut_at + 6);
    out.push_str(&text[..cut_at]);
    out.push_str("[...]\n");
    out
}

fn collapse_blank_lines(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut blank_run = 0usize;
    for line in text.split_inclusive('\n') {
        if line.trim().is_empty() {
            blank_run += 1;
            if blank_run <= 2 {
                out.push_str(line);
            }
        } else {
            blank_run = 0;
            out.push_str(line);
        }
    }
    out
}

/// Split text into UTF-8-safe slices of ≤ `max_chars` characters each.
/// Prefers to break at the last newline within the window; falls back to the
/// last whitespace; falls back to a hard char boundary.
fn split_into_char_chunks(text: &str, max_chars: usize) -> Vec<String> {
    if max_chars == 0 {
        return vec![text.to_string()];
    }
    let mut out = Vec::new();
    let mut rest = text;

    while !rest.is_empty() {
        // Try to take up to max_chars chars. If rest is shorter, we're done.
        let char_count = rest.chars().count();
        if char_count <= max_chars {
            out.push(rest.trim_end_matches('\n').to_string());
            break;
        }

        // Find byte index of the (max_chars)-th char to bound our search window.
        let window_end_byte = rest
            .char_indices()
            .nth(max_chars)
            .map(|(b, _)| b)
            .unwrap_or(rest.len());
        let window = &rest[..window_end_byte];

        let break_at_byte = window
            .rfind('\n')
            .map(|i| i + 1) // include the newline in the head chunk
            .or_else(|| window.rfind(char::is_whitespace).map(|i| i + 1))
            .unwrap_or(window_end_byte);

        let (head, tail) = rest.split_at(break_at_byte);
        out.push(head.trim_end_matches('\n').to_string());
        rest = tail.trim_start_matches(|c: char| c == ' ' || c == '\t');
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn opts(max_chars: usize, number: bool) -> ChunkerOpts {
        ChunkerOpts {
            max_chars,
            number_chunks: number,
            flush_silence: Duration::from_millis(600),
            burst_chunk_cap: 50,
        }
    }

    #[test]
    fn empty_input_yields_no_chunks() {
        assert!(chunks_from_text("", opts(120, true)).is_empty());
        assert!(chunks_from_text("   \n\n  ", opts(120, true)).is_empty());
    }

    #[test]
    fn short_ascii_one_chunk() {
        let chunks = chunks_from_text("hello world\n", opts(120, true));
        assert_eq!(chunks, vec!["hello world".to_string()]);
    }

    #[test]
    fn long_line_splits_to_multiple_chunks_with_prefix() {
        let body = "a".repeat(500);
        let chunks = chunks_from_text(&body, opts(120, true));
        assert!(chunks.len() >= 4, "got {} chunks", chunks.len());
        for c in &chunks {
            assert!(c.chars().count() <= 120, "chunk too long: {}", c.len());
            assert!(c.starts_with('['));
        }
    }

    #[test]
    fn no_numbering_when_single_chunk() {
        let chunks = chunks_from_text("hello\n", opts(120, true));
        assert_eq!(chunks, vec!["hello".to_string()]);
    }

    #[test]
    fn ansi_escapes_stripped() {
        let mut c = Chunker::new(opts(120, false));
        let mut out = c.push(b"hello\x1b[31mred\x1b[0m\n");
        out.extend(c.flush_force());
        let combined = out.join("|");
        assert!(combined.contains("hellored"), "got: {combined}");
        assert!(!combined.contains("\x1b"));
    }

    #[test]
    fn cr_is_normalized_to_lf() {
        let mut c = Chunker::new(opts(120, false));
        let out = c.push(b"prompt$ \rcommand\n");
        assert!(!out.is_empty());
        let s = out.join("|");
        assert!(!s.contains('\r'));
    }

    #[test]
    fn utf8_boundary_never_split() {
        let mut body = String::new();
        for _ in 0..50 {
            body.push_str("😀 ");
        }
        let chunks = chunks_from_text(&body, opts(20, false));
        // Every chunk must be valid UTF-8 (the type guarantees it, but
        // we additionally check we didn't truncate inside the emoji).
        for c in &chunks {
            assert!(!c.contains('\u{FFFD}'));
        }
    }

    #[test]
    fn burst_cap_truncates_with_marker() {
        let body = "x".repeat(10_000);
        let mut o = opts(20, false);
        o.burst_chunk_cap = 5;
        let chunks = chunks_from_text(&body, o);
        assert_eq!(chunks.len(), 6, "5 chunks + 1 marker");
        assert!(chunks.last().unwrap().contains("truncated"));
    }

    #[test]
    fn chunker_flushes_on_newline() {
        let mut c = Chunker::new(opts(120, false));
        let empty = c.push(b"partial line without newline");
        assert!(empty.is_empty(), "no newline → no flush");
        let flushed = c.push(b" continued\n");
        assert!(!flushed.is_empty());
    }

    #[test]
    fn chunker_silence_flushes_after_window() {
        let mut c = Chunker::new(opts(120, false));
        let _ = c.push(b"no newline here");
        let too_early = c.maybe_flush(Instant::now());
        assert!(too_early.is_empty());
        let way_later = c.maybe_flush(Instant::now() + Duration::from_secs(2));
        assert!(!way_later.is_empty());
    }

    #[test]
    fn line_cap_truncates_long_output_with_marker() {
        // 15 single-character lines; cap is 10.
        let body: String = (1..=15).map(|n| format!("{n}\n")).collect();
        let chunks = chunks_from_text(&body, opts(200, false));
        let joined = chunks.join("\n");
        // First 10 numbers present.
        for n in 1..=10 {
            assert!(joined.contains(&format!("{n}")), "missing line {n}: {joined}");
        }
        // Anything beyond was dropped.
        for n in 11..=15 {
            assert!(
                !joined.lines().any(|l| l.trim() == n.to_string()),
                "line {n} should be dropped: {joined}"
            );
        }
        assert!(joined.contains("[...]"), "missing truncation marker: {joined}");
    }

    #[test]
    fn line_cap_passthrough_when_below_limit() {
        let body = "one\ntwo\nthree\n";
        let chunks = chunks_from_text(body, opts(200, false));
        let joined = chunks.join("\n");
        assert!(!joined.contains("[...]"), "no marker expected: {joined}");
    }

    #[test]
    fn numbering_format() {
        let body = "a".repeat(300);
        let chunks = chunks_from_text(&body, opts(100, true));
        assert!(chunks.len() >= 3);
        let n = chunks.len();
        for (i, c) in chunks.iter().enumerate() {
            let expected_prefix = format!("[{}/{}] ", i + 1, n);
            assert!(
                c.starts_with(&expected_prefix),
                "chunk {} doesn't start with {expected_prefix}: {c}",
                i + 1,
            );
        }
    }
}
