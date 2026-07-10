use std::time::Duration;

use crate::directive::Directive;
use crate::envelope::Envelope;
use crate::step::StepError;

pub struct UrlTransform {
    pub url: String,
    pub timeout: Duration,
}

impl UrlTransform {
    pub fn new(url: String, timeout_ms: u64) -> Self {
        Self {
            url,
            timeout: Duration::from_millis(timeout_ms),
        }
    }

    /// POST the envelope to the step service and read back a directive.
    pub async fn run(
        &self,
        client: &reqwest::Client,
        env: &Envelope,
    ) -> Result<Directive, StepError> {
        let bytes = client
            .post(&self.url)
            .timeout(self.timeout)
            .json(env)
            .send()
            .await?
            .error_for_status()?
            .bytes()
            .await?;
        let directive: Directive = serde_json::from_slice(&bytes)?;
        Ok(directive)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::envelope::build_request_envelope;
    use crate::http_msg::HttpMsg;
    use std::collections::BTreeMap;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn posts_envelope_and_parses_directive() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/redact"))
            .respond_with(ResponseTemplate::new(200).set_body_string(
                r#"{"action":"continue","ops":[{"op":"set_header","name":"x-tag","value":"seen"}]}"#,
            ))
            .mount(&server)
            .await;

        let msg = HttpMsg {
            method: "POST".into(),
            path: "/v1/messages".into(),
            headers: BTreeMap::new(),
            body_b64: String::new(),
        };
        let env = build_request_envelope(
            "claude",
            "redact",
            &msg,
            &serde_json::Map::new(),
            "corr-test",
            None,
        );
        let step = UrlTransform::new(format!("{}/redact", server.uri()), 1000);
        let client = reqwest::Client::new();

        let directive = step.run(&client, &env).await.unwrap();
        match directive {
            Directive::Continue { ops } => assert_eq!(ops.len(), 1),
            other => panic!("expected continue, got {other:?}"),
        }
    }
}
