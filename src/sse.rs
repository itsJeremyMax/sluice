//! Incremental Server-Sent Events (SSE) framer (design doc M10 Task 2).
//!
//! [`SseFramer`] accumulates raw bytes from a streaming HTTP response body
//! and yields complete [`SseEvent`]s as soon as their terminating blank line
//! (`\n\n`) has arrived, holding any partial trailing bytes across calls to
//! [`SseFramer::push`]. This is deliberately minimal: only the `data:` field
//! of an SSE event is surfaced (concatenated across every `data:` line in the
//! event, one optional leading space stripped per line, per the SSE spec's
//! field-value convention) — `event:`, `id:`, `retry:` and comment (`:`)
//! lines are ignored, since none of the three provider wire formats this
//! gateway targets (Anthropic, OpenAI, Google) rely on them for the
//! information this gateway cares about.
//!
//! Line splitting is on bare `\n`; a `\r` immediately preceding it is
//! trimmed, so both `\n`- and `\r\n`-terminated lines parse correctly. The
//! blank-line *event terminator* itself is matched literally as `\n\n`
//! (per the design brief) — a `\r\n\r\n` terminator is not specially
//! recognized. All providers this gateway targets send bare-`\n` SSE, so
//! this is a documented simplification rather than a spec gap in practice.
//!
//! The internal buffer is bounded by `max_event_bytes`: if a single
//! un-terminated event grows past that cap, [`SseFramer::push`] returns
//! [`SseError::EventTooLarge`] and resets the framer's buffer (the event is
//! unrecoverable, so there is nothing useful to hold onto). If that same
//! `push` call *also* completed one or more properly `"\n\n"`-terminated
//! events earlier in the buffer, those are real, already-delimited data —
//! not part of the oversized tail — and are still returned via `Ok` rather
//! than discarded alongside it (design doc M11 Task 3 fix).
//!
//! Note: `config.gateway.max_event_bytes` does not exist as of this task —
//! the M9-era `Gateway` config struct (`src/config/mod.rs`) has
//! `max_context_bytes` and `max_body_bytes` but no `max_event_bytes` field.
//! Per the task brief, [`DEFAULT_MAX_EVENT_BYTES`] (1 MiB) is used as a
//! plain constant; wiring a config field through is left to whichever later
//! task actually constructs a framer from `Config`.

use thiserror::Error;

/// Default cap on an un-terminated event's buffered size, in bytes (1 MiB).
/// See the module doc comment: no `config.gateway.max_event_bytes` field
/// exists yet to source this from.
pub const DEFAULT_MAX_EVENT_BYTES: usize = 1_048_576;

/// A single parsed SSE event: the concatenation of every `data:` line's
/// payload in the event, joined by `\n`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SseEvent {
    pub data: String,
}

/// Failure accumulating or framing SSE bytes.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum SseError {
    #[error("SSE event exceeded max size of {max} bytes")]
    EventTooLarge { max: usize },
}

/// Incremental SSE framer. Feed raw bytes via [`push`](Self::push) as they
/// arrive off the wire; complete events are returned as soon as their
/// terminating blank line is seen. Call [`finish`](Self::finish) once the
/// underlying stream ends to flush a trailing event that never received its
/// terminator (some servers omit the final blank line).
pub struct SseFramer {
    buf: Vec<u8>,
    max_event_bytes: usize,
    /// Resume cursor (design doc M10 fix): the number of leading bytes of
    /// `buf` already confirmed, as of the end of the previous scan, to
    /// contain no `"\n\n"` delimiter. The next scan starts one byte before
    /// this (`scanned.saturating_sub(1)`) rather than from `0`, so a
    /// delimiter that straddles the boundary between two `push` calls
    /// (`\n` at the very end of one push, `\n` at the very start of the
    /// next) is still found via the two-byte window that spans it.
    scanned: usize,
}

impl Default for SseFramer {
    fn default() -> Self {
        Self::new()
    }
}

impl SseFramer {
    /// A framer bounded by [`DEFAULT_MAX_EVENT_BYTES`].
    pub fn new() -> Self {
        Self::with_max_event_bytes(DEFAULT_MAX_EVENT_BYTES)
    }

    /// A framer bounded by an explicit cap.
    pub fn with_max_event_bytes(max_event_bytes: usize) -> Self {
        Self {
            buf: Vec::new(),
            max_event_bytes,
            scanned: 0,
        }
    }

    /// Accumulate `bytes` and return every complete event they produced
    /// (zero, one, or many — a single `push` can contain multiple events,
    /// or none if the buffer is still holding a partial tail). Returns `Err`
    /// only when NO complete event was produced by this call and the
    /// remaining buffered tail exceeds `max_event_bytes`; if one or more
    /// events did complete, they are returned via `Ok` even if the tail left
    /// over after them is separately oversized (and silently discarded) —
    /// see the module doc comment.
    pub fn push(&mut self, bytes: &[u8]) -> Result<Vec<SseEvent>, SseError> {
        self.buf.extend_from_slice(bytes);

        let mut events = Vec::new();
        loop {
            // Resume from one byte before the last confirmed-clean offset,
            // not byte 0: everything before that was already scanned by a
            // prior `push` and found delimiter-free, except for the single
            // byte that could pair with newly-appended data to complete a
            // straddling "\n\n".
            let search_start = self.scanned.saturating_sub(1);
            match find_double_newline(&self.buf, search_start) {
                Some(terminator_start) => {
                    let raw: Vec<u8> = self.buf.drain(..terminator_start + 2).collect();
                    // Drop the trailing "\n\n" terminator itself from the parsed data.
                    events.push(parse_event(&raw[..raw.len() - 2]));
                    // The buffer just shifted left; the remaining tail
                    // hasn't been scanned as a standalone buffer yet.
                    self.scanned = 0;
                }
                None => {
                    self.scanned = self.buf.len();
                    break;
                }
            }
        }

        if self.buf.len() > self.max_event_bytes {
            // The remaining (still-unterminated) tail is unrecoverable —
            // discard it and reset the cursor. But any events already fully
            // framed earlier in THIS SAME `push` call (drained into
            // `events` above, each already past its own `"\n\n"`
            // terminator) are real, already-delimited data, not part of the
            // oversized tail — they must not be thrown away with it. A
            // caller (e.g. the on_stream `mutate` pipeline, `proxy.rs`)
            // treats every returned event as client-visible content —
            // losing them here would silently corrupt/truncate the stream,
            // not merely drop best-effort telemetry the way it does for
            // on_stream `observe`.
            self.buf.clear();
            self.scanned = 0;
            if events.is_empty() {
                return Err(SseError::EventTooLarge {
                    max: self.max_event_bytes,
                });
            }
            // Fall through: still return the events collected so far. The
            // caller has no separate signal that a trailing oversized
            // fragment was ALSO discarded in this same call — an accepted
            // limitation of this `Result<Vec<_>, _>` shape (the events that
            // did complete are surfaced without also carrying the fact that
            // something else, later in the same buffer, did not).
        }

        Ok(events)
    }

    /// Flush a trailing event that never received its terminating blank
    /// line (e.g. the stream ended right after the last `data:` line).
    /// Returns `None` if nothing is buffered.
    pub fn finish(&mut self) -> Option<SseEvent> {
        if self.buf.is_empty() {
            return None;
        }
        let raw = std::mem::take(&mut self.buf);
        self.scanned = 0;
        Some(parse_event(&raw))
    }

    /// Flush the raw trailing bytes still buffered (never framed by a
    /// terminating blank line) verbatim, with no `data:`-line parsing
    /// applied. Used by the `on_stream` `mutate` pipeline (design doc M11
    /// Task 3) to forward genuinely non-SSE-shaped or partial-tail bytes to
    /// the client unchanged: unlike [`finish`](Self::finish), which discards
    /// everything but concatenated `data:` payloads, this preserves the
    /// bytes byte-for-byte — mutate only ever runs the step chain over fully
    /// framed events, never over this trailing remainder.
    pub fn finish_raw(&mut self) -> Option<Vec<u8>> {
        if self.buf.is_empty() {
            return None;
        }
        let raw = std::mem::take(&mut self.buf);
        self.scanned = 0;
        Some(raw)
    }
}

/// Find the start index of the first `"\n\n"` in `buf` at or after `start`,
/// if any (the returned index is relative to the start of `buf`, not to
/// `start`).
fn find_double_newline(buf: &[u8], start: usize) -> Option<usize> {
    let start = start.min(buf.len());
    buf[start..]
        .windows(2)
        .position(|w| w == b"\n\n")
        .map(|p| p + start)
}

/// Parse one event's raw bytes (everything up to, but not including, its
/// terminating blank line) into an [`SseEvent`] by concatenating `data:`
/// line payloads.
fn parse_event(raw: &[u8]) -> SseEvent {
    let text = String::from_utf8_lossy(raw);
    let mut data_lines = Vec::new();
    for line in text.split('\n') {
        let line = line.strip_suffix('\r').unwrap_or(line);
        if let Some(rest) = line.strip_prefix("data:") {
            let rest = rest.strip_prefix(' ').unwrap_or(rest);
            data_lines.push(rest.to_string());
        }
    }
    SseEvent {
        data: data_lines.join("\n"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_event_in_one_push() {
        let mut framer = SseFramer::new();
        let events = framer.push(b"data: hello\n\n").unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].data, "hello");
    }

    #[test]
    fn splits_multiple_events_in_one_chunk() {
        let mut framer = SseFramer::new();
        let events = framer
            .push(b"data: first\n\ndata: second\n\ndata: third\n\n")
            .unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].data, "first");
        assert_eq!(events[1].data, "second");
        assert_eq!(events[2].data, "third");
    }

    #[test]
    fn holds_partial_tail_across_calls() {
        let mut framer = SseFramer::new();
        let events = framer.push(b"data: hel").unwrap();
        assert!(events.is_empty());
        let events = framer.push(b"lo\n\n").unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].data, "hello");
    }

    #[test]
    fn holds_partial_tail_split_mid_terminator() {
        // The "\n\n" terminator itself can straddle two pushes.
        let mut framer = SseFramer::new();
        let events = framer.push(b"data: hello\n").unwrap();
        assert!(events.is_empty());
        let events = framer.push(b"\n").unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].data, "hello");
    }

    #[test]
    fn resume_cursor_finds_delimiter_split_across_many_pushes() {
        // Build up a multi-push, still-unterminated buffer (each individual
        // push has no "\n\n" in it, so a naive from-scratch rescan would
        // still work, but this exercises the resume cursor across several
        // pushes rather than just two) before the terminator itself
        // straddles the final two pushes.
        let mut framer = SseFramer::new();
        assert!(framer.push(b"data: ").unwrap().is_empty());
        assert!(framer.push(b"chunk-a").unwrap().is_empty());
        assert!(framer.push(b"chunk-b").unwrap().is_empty());
        assert!(framer.push(b"\n").unwrap().is_empty());
        let events = framer.push(b"\n").unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].data, "chunk-achunk-b");

        // The framer must still work correctly (buffer not corrupted) for
        // a subsequent event after the resume cursor was exercised.
        let events = framer.push(b"data: next\n\n").unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].data, "next");
    }

    #[test]
    fn multi_line_data_fields_are_joined_with_newline() {
        let mut framer = SseFramer::new();
        let events = framer.push(b"data: line one\ndata: line two\n\n").unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].data, "line one\nline two");
    }

    #[test]
    fn non_data_lines_are_ignored() {
        let mut framer = SseFramer::new();
        let events = framer
            .push(b"event: message\nid: 42\ndata: payload\n\n")
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].data, "payload");
    }

    #[test]
    fn data_prefix_without_space_is_still_stripped() {
        let mut framer = SseFramer::new();
        let events = framer.push(b"data:no-space\n\n").unwrap();
        assert_eq!(events[0].data, "no-space");
    }

    #[test]
    fn oversized_unterminated_event_errors() {
        let mut framer = SseFramer::with_max_event_bytes(16);
        let err = framer
            .push(b"data: this line is way too long for the cap")
            .unwrap_err();
        assert_eq!(err, SseError::EventTooLarge { max: 16 });
    }

    #[test]
    fn error_resets_buffer_so_framer_can_be_reused() {
        let mut framer = SseFramer::with_max_event_bytes(16);
        assert!(framer
            .push(b"data: way too long to fit in the cap")
            .is_err());
        // Subsequent well-sized pushes should work normally.
        let events = framer.push(b"data: ok\n\n").unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].data, "ok");
    }

    /// Regression test: a complete, properly `"\n\n"`-terminated event must
    /// still be returned even when the SAME `push` call's leftover
    /// unterminated tail is itself oversized. Before this fix, `push`
    /// checked the oversize condition against the buffer *after* already
    /// draining completed events into the `events` Vec, then discarded
    /// that whole Vec by returning `Err` — silently losing already-framed,
    /// legitimate events any time an oversized tail happened to follow them
    /// in one push. That's a correctness bug for a caller like the
    /// on_stream `mutate` pipeline, which treats every returned event as
    /// real client-visible content (unlike `on_stream` observe, for which a
    /// dropped event was merely best-effort telemetry).
    #[test]
    fn complete_event_survives_when_trailing_tail_in_same_push_is_oversized() {
        let mut framer = SseFramer::with_max_event_bytes(16);
        let mut input = b"data: ok\n\n".to_vec();
        input.extend_from_slice(b"data: this trailing part is unterminated and way too long");

        let events = framer.push(&input).unwrap();
        assert_eq!(events.len(), 1, "the complete leading event must survive");
        assert_eq!(events[0].data, "ok");

        // The oversized tail was discarded and the cursor/buffer reset, so
        // the framer is immediately usable again.
        let events = framer.push(b"data: next\n\n").unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].data, "next");
    }

    #[test]
    fn finish_flushes_trailing_unterminated_event() {
        let mut framer = SseFramer::new();
        let events = framer.push(b"data: trailing").unwrap();
        assert!(events.is_empty());
        let flushed = framer.finish().expect("trailing data must flush");
        assert_eq!(flushed.data, "trailing");
    }

    #[test]
    fn finish_on_empty_buffer_returns_none() {
        let mut framer = SseFramer::new();
        assert!(framer.finish().is_none());
    }

    #[test]
    fn finish_after_all_events_consumed_returns_none() {
        let mut framer = SseFramer::new();
        let events = framer.push(b"data: hello\n\n").unwrap();
        assert_eq!(events.len(), 1);
        assert!(framer.finish().is_none());
    }

    #[test]
    fn finish_raw_returns_verbatim_trailing_bytes() {
        let mut framer = SseFramer::new();
        assert!(framer
            .push(b"event: ping\ndata: trailing")
            .unwrap()
            .is_empty());
        let raw = framer.finish_raw().expect("trailing bytes must flush");
        // Byte-for-byte, including the `event:` line `finish`/`parse_event`
        // would have discarded.
        assert_eq!(raw, b"event: ping\ndata: trailing");
    }

    #[test]
    fn finish_raw_on_empty_buffer_returns_none() {
        let mut framer = SseFramer::new();
        assert!(framer.finish_raw().is_none());
    }

    #[test]
    fn finish_raw_after_all_events_consumed_returns_none() {
        let mut framer = SseFramer::new();
        let events = framer.push(b"data: hello\n\n").unwrap();
        assert_eq!(events.len(), 1);
        assert!(framer.finish_raw().is_none());
    }
}
