//! A simulated LLM upstream: deterministic, offline, with configurable
//! time-to-first-token and inter-chunk pacing so scenarios exercise
//! realistic streaming shapes without any provider dependency.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::time::Duration;

use bytes::Bytes;
use futures_util::StreamExt;
use http_body_util::{Full, StreamBody};
use hyper::body::Frame;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;

#[derive(Clone, Copy)]
pub struct Sim {
    pub ttft: Duration,
    pub inter_chunk: Duration,
    pub chunks: usize,
}

impl Sim {
    pub fn default_bench() -> Self {
        Self {
            ttft: Duration::from_millis(2),
            inter_chunk: Duration::from_millis(1),
            chunks: 20,
        }
    }
}

/// Anthropic-messages-shaped response body; the translation scenario
/// translates THIS, so it must stay a valid anthropic response.
const JSON_BODY: &str = r#"{"id":"msg_bench","type":"message","role":"assistant","model":"claude-opus-4-1-20250805","content":[{"type":"text","text":"benchmark response"}],"stop_reason":"end_turn","stop_sequence":null,"usage":{"input_tokens":10,"output_tokens":5}}"#;

type BoxBody = http_body_util::combinators::BoxBody<Bytes, Infallible>;

async fn handle(req: Request<hyper::body::Incoming>, sim: Sim) -> Response<BoxBody> {
    use http_body_util::BodyExt;
    match req.uri().path() {
        "/v1/stream" => {
            let stream = futures_util::stream::iter(0..=sim.chunks).then(move |i| async move {
                if i == 0 {
                    tokio::time::sleep(sim.ttft).await;
                } else {
                    tokio::time::sleep(sim.inter_chunk).await;
                }
                let chunk = if i == sim.chunks {
                    "data: [DONE]\n\n".to_string()
                } else {
                    format!("data: {{\"i\":{i}}}\n\n")
                };
                Ok::<_, Infallible>(Frame::data(Bytes::from(chunk)))
            });
            Response::builder()
                .header("content-type", "text/event-stream")
                .body(BodyExt::boxed(StreamBody::new(stream)))
                .unwrap()
        }
        _ => {
            tokio::time::sleep(sim.ttft).await;
            Response::builder()
                .header("content-type", "application/json")
                .body(Full::new(Bytes::from(JSON_BODY)).boxed())
                .unwrap()
        }
    }
}

/// Bind on an ephemeral port, spawn the accept loop onto the current
/// runtime, and return the bound address.
pub fn start(sim: Sim) -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind upstream");
    listener.set_nonblocking(true).expect("nonblocking");
    let addr = listener.local_addr().expect("local addr");
    let listener = tokio::net::TcpListener::from_std(listener).expect("tokio listener");
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(
                        TokioIo::new(stream),
                        service_fn(move |req| async move {
                            Ok::<_, Infallible>(handle(req, sim).await)
                        }),
                    )
                    .await;
            });
        }
    });
    addr
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn json_endpoint_returns_anthropic_shaped_body() {
        let addr = start(Sim::default_bench());
        let resp = reqwest::Client::new()
            .post(format!("http://{addr}/v1/messages"))
            .body("{}")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let v: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(v["type"], "message");
        assert_eq!(v["model"], "claude-opus-4-1-20250805");
    }

    #[tokio::test]
    async fn stream_endpoint_emits_all_chunks_then_done() {
        let addr = start(Sim {
            ttft: std::time::Duration::from_millis(1),
            inter_chunk: std::time::Duration::from_millis(1),
            chunks: 3,
        });
        let body = reqwest::Client::new()
            .post(format!("http://{addr}/v1/stream"))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert_eq!(body.matches("data: {\"i\":").count(), 3);
        assert!(body.ends_with("data: [DONE]\n\n"));
    }
}
