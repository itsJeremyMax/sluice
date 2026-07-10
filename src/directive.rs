use std::collections::BTreeMap;

use serde::Deserialize;
use serde_json::Value;
use thiserror::Error;

use crate::http_msg::HttpMsg;
use crate::reconstruct;

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum Directive {
    Continue {
        #[serde(default)]
        ops: Vec<Op>,
    },
    ShortCircuit {
        response: RespSpec,
    },
    Abort {
        response: RespSpec,
    },
    /// `on_stream` `mutate`-only (design doc M11 Task 3): forward `chunk`'s
    /// (possibly rewritten) bytes to the client in place of the original
    /// event, applying `ops` (in practice only `Op::SetContext` has any
    /// observable effect at `on_stream` — see `proxy::apply_stream_ops`)
    /// to the cross-event stream context first. Illegal at `on_request`/
    /// `on_response` — those hooks never construct or accept this variant.
    Emit {
        chunk: EmitChunk,
        #[serde(default)]
        ops: Vec<Op>,
    },
    /// `on_stream` `mutate`-only: swallow this event (the client receives
    /// nothing for it) without ending the stream, after applying `ops` to
    /// the cross-event stream context. Illegal at `on_request`/`on_response`.
    Drop {
        #[serde(default)]
        ops: Vec<Op>,
    },
}

/// The `chunk` payload of an `emit` directive: the base64-encoded bytes that
/// become this event's `data:` payload on the client stream. Deliberately
/// just the one field this hook actually needs (rather than mirroring every
/// field of the chunk envelope `proxy` builds for the step) — `serde` simply
/// ignores any other keys (`seq`, `final`, `delta`, ...) a step echoes back
/// alongside it, so a step that mirrors the whole envelope shape back still
/// deserializes fine.
#[derive(Debug, Clone, Deserialize)]
pub struct EmitChunk {
    pub data_b64: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Op {
    SetHeader { name: String, value: String },
    DeleteHeader { name: String },
    SetBody { body_b64: String },
    SetPath { path: String },
    SetContext { value: Value },
}

#[derive(Debug, Clone, Deserialize)]
pub struct RespSpec {
    pub status: u16,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    #[serde(default)]
    pub body_b64: String,
}

#[derive(Debug, Error, PartialEq)]
pub enum OpError {
    #[error("step attempted to modify protected header '{0}'")]
    ProtectedHeader(String),
    #[error("context for namespace '{namespace}' is {size} bytes, exceeding cap of {cap} bytes")]
    ContextTooLarge {
        namespace: String,
        size: usize,
        cap: usize,
    },
}

/// Apply a step's `continue` ops to the working request message and context.
///
/// Atomic: ops are applied to local clones of `req`/`context`, and the
/// caller's originals are only overwritten on full success. If any op
/// errors partway through, `req` and `context` are left completely
/// untouched — this matters for `fail_open` routing, where a failed step's
/// partial mutations must never leak into later steps or upstream.
pub fn apply_ops(
    req: &mut HttpMsg,
    context: &mut serde_json::Map<String, Value>,
    self_name: &str,
    ops: &[Op],
    max_context_bytes: usize,
) -> Result<(), OpError> {
    let mut new_req = req.clone();
    let mut new_ctx = context.clone();

    for op in ops {
        match op {
            Op::SetHeader { name, value } => {
                guard_protected(name)?;
                new_req.set_header(name, value);
            }
            Op::DeleteHeader { name } => {
                guard_protected(name)?;
                new_req.delete_header(name);
            }
            Op::SetBody { body_b64 } => {
                new_req.body_b64 = body_b64.clone();
            }
            Op::SetPath { path } => {
                new_req.path = path.clone();
            }
            Op::SetContext { value } => {
                let size = serde_json::to_vec(value)
                    .map(|bytes| bytes.len())
                    .unwrap_or(usize::MAX);
                if size > max_context_bytes {
                    return Err(OpError::ContextTooLarge {
                        namespace: self_name.to_string(),
                        size,
                        cap: max_context_bytes,
                    });
                }
                new_ctx.insert(self_name.to_string(), value.clone());
            }
        }
    }

    *req = new_req;
    *context = new_ctx;
    Ok(())
}

fn guard_protected(name: &str) -> Result<(), OpError> {
    if reconstruct::is_hop_by_hop(name) {
        return Err(OpError::ProtectedHeader(name.to_ascii_lowercase()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn msg() -> HttpMsg {
        HttpMsg {
            method: "POST".into(),
            path: "/v1/messages".into(),
            headers: BTreeMap::new(),
            body_b64: String::new(),
        }
    }

    #[test]
    fn parses_continue_with_ops() {
        let d: Directive = serde_json::from_str(
            r#"{"action":"continue","ops":[{"op":"set_header","name":"x-tag","value":"seen"}]}"#,
        )
        .unwrap();
        match d {
            Directive::Continue { ops } => assert_eq!(ops.len(), 1),
            other => panic!("expected continue, got {other:?}"),
        }
    }

    #[test]
    fn parses_continue_without_ops() {
        let d: Directive = serde_json::from_str(r#"{"action":"continue"}"#).unwrap();
        assert!(matches!(d, Directive::Continue { ops } if ops.is_empty()));
    }

    #[test]
    fn parses_short_circuit() {
        let d: Directive = serde_json::from_str(
            r#"{"action":"short_circuit","response":{"status":200,"body_b64":"aGk="}}"#,
        )
        .unwrap();
        match d {
            Directive::ShortCircuit { response } => assert_eq!(response.status, 200),
            other => panic!("expected short_circuit, got {other:?}"),
        }
    }

    #[test]
    fn parses_emit_with_chunk_and_ops() {
        let d: Directive = serde_json::from_str(
            r#"{"action":"emit","chunk":{"data_b64":"aGk="},"ops":[{"op":"set_context","value":{"n":1}}]}"#,
        )
        .unwrap();
        match d {
            Directive::Emit { chunk, ops } => {
                assert_eq!(chunk.data_b64, "aGk=");
                assert_eq!(ops.len(), 1);
            }
            other => panic!("expected emit, got {other:?}"),
        }
    }

    #[test]
    fn parses_emit_without_ops_defaults_empty() {
        let d: Directive =
            serde_json::from_str(r#"{"action":"emit","chunk":{"data_b64":"aGk="}}"#).unwrap();
        assert!(matches!(d, Directive::Emit { ops, .. } if ops.is_empty()));
    }

    #[test]
    fn parses_emit_ignores_extra_chunk_fields() {
        // A step that echoes back the whole chunk envelope shape (seq,
        // final, delta) rather than just `data_b64` must still parse.
        let d: Directive = serde_json::from_str(
            r#"{"action":"emit","chunk":{"data_b64":"aGk=","seq":3,"final":false,"delta":null}}"#,
        )
        .unwrap();
        assert!(matches!(d, Directive::Emit { chunk, .. } if chunk.data_b64 == "aGk="));
    }

    #[test]
    fn parses_drop_without_ops() {
        let d: Directive = serde_json::from_str(r#"{"action":"drop"}"#).unwrap();
        assert!(matches!(d, Directive::Drop { ops } if ops.is_empty()));
    }

    #[test]
    fn parses_drop_with_ops() {
        let d: Directive = serde_json::from_str(
            r#"{"action":"drop","ops":[{"op":"set_context","value":{"dropped":true}}]}"#,
        )
        .unwrap();
        match d {
            Directive::Drop { ops } => assert_eq!(ops.len(), 1),
            other => panic!("expected drop, got {other:?}"),
        }
    }

    #[test]
    fn apply_set_and_delete_header() {
        let mut m = msg();
        m.set_header("x-old", "1");
        let mut ctx = serde_json::Map::new();
        let ops = vec![
            Op::SetHeader {
                name: "X-Tag".into(),
                value: "seen".into(),
            },
            Op::DeleteHeader {
                name: "x-old".into(),
            },
        ];
        apply_ops(&mut m, &mut ctx, "s", &ops, 65536).unwrap();
        assert_eq!(m.headers.get("x-tag").map(String::as_str), Some("seen"));
        assert!(!m.headers.contains_key("x-old"));
    }

    #[test]
    fn apply_set_body_and_path() {
        let mut m = msg();
        let mut ctx = serde_json::Map::new();
        apply_ops(
            &mut m,
            &mut ctx,
            "s",
            &[
                Op::SetBody {
                    body_b64: "aGk=".into(),
                },
                Op::SetPath { path: "/v2".into() },
            ],
            65536,
        )
        .unwrap();
        assert_eq!(m.body_bytes().unwrap(), b"hi");
        assert_eq!(m.path, "/v2");
    }

    #[test]
    fn set_context_writes_under_self_namespace() {
        let mut m = msg();
        let mut ctx = serde_json::Map::new();
        apply_ops(
            &mut m,
            &mut ctx,
            "redact",
            &[Op::SetContext {
                value: serde_json::json!({"count": 3}),
            }],
            65536,
        )
        .unwrap();
        assert_eq!(ctx["redact"]["count"], 3);
    }

    #[test]
    fn rejects_protected_header() {
        let mut m = msg();
        let mut ctx = serde_json::Map::new();
        let err = apply_ops(
            &mut m,
            &mut ctx,
            "s",
            &[Op::SetHeader {
                name: "Content-Length".into(),
                value: "5".into(),
            }],
            65536,
        )
        .unwrap_err();
        assert_eq!(err, OpError::ProtectedHeader("content-length".into()));
    }

    #[test]
    fn set_context_under_cap_succeeds() {
        let mut m = msg();
        let mut ctx = serde_json::Map::new();
        apply_ops(
            &mut m,
            &mut ctx,
            "redact",
            &[Op::SetContext {
                value: serde_json::json!({"count": 3}),
            }],
            10_000,
        )
        .unwrap();
        assert_eq!(ctx["redact"]["count"], 3);
    }

    #[test]
    fn set_context_over_cap_rejected_and_leaves_context_unchanged() {
        let mut m = msg();
        let mut ctx = serde_json::Map::new();
        let big_value = serde_json::json!({"data": "x".repeat(100)});
        let err = apply_ops(
            &mut m,
            &mut ctx,
            "redact",
            &[Op::SetContext { value: big_value }],
            10,
        )
        .unwrap_err();
        match err {
            OpError::ContextTooLarge {
                namespace,
                size,
                cap,
            } => {
                assert_eq!(namespace, "redact");
                assert!(size > cap);
                assert_eq!(cap, 10);
            }
            other => panic!("expected ContextTooLarge, got {other:?}"),
        }
        assert!(!ctx.contains_key("redact"));
    }
}
