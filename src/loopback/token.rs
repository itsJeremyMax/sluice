//! Signed `x-chain-token` used by `mode = "loopback"` url steps (design doc
//! §4.5, M12): a compact, tamper-evident token the gateway hands back to a
//! step so a subsequent request can resume the same chain at the right
//! route/step/hop without the gateway needing server-side session state.
//!
//! Wire format: `base64url(json payload) + "." + base64url(HMAC-SHA256(payload, secret))`.
//! The MAC is computed over the *raw payload bytes* (before JSON parsing),
//! so verification recomputes the MAC from the still-encoded payload and
//! only decodes/parses JSON once the MAC has been accepted.

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use thiserror::Error;

type HmacSha256 = Hmac<Sha256>;

/// The claims carried by a signed chain token: enough to resume a loopback
/// chain (which conversation, which route, which step to resume from, how
/// many hops have elapsed) plus an expiry so a leaked/replayed token can't
/// be used forever.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChainToken {
    pub cid: String,
    pub route_id: String,
    pub resume_index: usize,
    pub hop: u32,
    pub expires_at_unix: u64,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum TokenError {
    #[error("chain token is malformed")]
    BadFormat,
    #[error("chain token MAC verification failed")]
    BadMac,
    #[error("chain token has expired")]
    Expired,
}

impl ChainToken {
    /// Returns a copy of this token with `hop` incremented by one. Used
    /// each time a loopback chain advances to the next step/hop so the
    /// `max_hops` guard (design doc §4.5) can be enforced by the caller.
    pub fn with_incremented_hop(self) -> Self {
        Self {
            hop: self.hop + 1,
            ..self
        }
    }

    /// Signs this token: `base64url(json) + "." + base64url(hmac_sha256(json, secret))`.
    pub fn sign(&self, secret: &[u8]) -> String {
        let payload =
            serde_json::to_vec(self).expect("ChainToken contains no non-serializable fields");
        let mac = mac_bytes(secret, &payload);
        format!(
            "{}.{}",
            URL_SAFE_NO_PAD.encode(&payload),
            URL_SAFE_NO_PAD.encode(mac)
        )
    }

    /// Verifies the MAC and decodes the claims WITHOUT checking expiry.
    /// Recomputes the HMAC over the decoded payload bytes and compares it to
    /// the token's MAC in constant time (`Mac::verify_slice`) before parsing
    /// or trusting any claim.
    ///
    /// This is the lower-level primitive [`verify`](Self::verify) layers its
    /// expiry check on top of. It's also exposed directly for callers that
    /// want a token's decoded claims even when expired — `sluice token
    /// verify` (M12 CLI) is exactly that: it wants to *show* an expired
    /// token's fields alongside reporting that it's expired, not just a bare
    /// error. Request-path callers resuming a real chain (`proxy::handle_callback`)
    /// must use `verify`, never this, so an expired token is always rejected
    /// on the request path.
    pub fn decode(token: &str, secret: &[u8]) -> Result<Self, TokenError> {
        let (payload_b64, mac_b64) = token.split_once('.').ok_or(TokenError::BadFormat)?;

        let payload = URL_SAFE_NO_PAD
            .decode(payload_b64)
            .map_err(|_| TokenError::BadFormat)?;
        let claimed_mac = URL_SAFE_NO_PAD
            .decode(mac_b64)
            .map_err(|_| TokenError::BadFormat)?;

        let mut mac =
            HmacSha256::new_from_slice(secret).expect("HMAC-SHA256 accepts a key of any length");
        mac.update(&payload);
        mac.verify_slice(&claimed_mac)
            .map_err(|_| TokenError::BadMac)?;

        let claims: ChainToken =
            serde_json::from_slice(&payload).map_err(|_| TokenError::BadFormat)?;

        Ok(claims)
    }

    /// Verifies and decodes a signed token, additionally rejecting one that
    /// has expired. The request-path entrypoint (`proxy::handle_callback`)
    /// always uses this, never [`decode`](Self::decode) directly.
    pub fn verify(token: &str, secret: &[u8], now_unix: u64) -> Result<Self, TokenError> {
        let claims = Self::decode(token, secret)?;

        if claims.expires_at_unix <= now_unix {
            return Err(TokenError::Expired);
        }

        Ok(claims)
    }
}

fn mac_bytes(secret: &[u8], payload: &[u8]) -> Vec<u8> {
    let mut mac =
        HmacSha256::new_from_slice(secret).expect("HMAC-SHA256 accepts a key of any length");
    mac.update(payload);
    mac.finalize().into_bytes().to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> ChainToken {
        ChainToken {
            cid: "conv-1".to_string(),
            route_id: "claude".to_string(),
            resume_index: 2,
            hop: 0,
            expires_at_unix: 1_000,
        }
    }

    #[test]
    fn sign_then_verify_round_trips_all_fields() {
        let token = sample();
        let signed = token.sign(b"secret");
        let verified = ChainToken::verify(&signed, b"secret", 500).unwrap();
        assert_eq!(verified, token);
    }

    #[test]
    fn verify_rejects_wrong_secret_as_bad_mac() {
        let token = sample();
        let signed = token.sign(b"secret");
        let err = ChainToken::verify(&signed, b"wrong-secret", 500).unwrap_err();
        assert_eq!(err, TokenError::BadMac);
    }

    #[test]
    fn verify_rejects_tampered_payload_as_bad_mac() {
        let token = sample();
        let signed = token.sign(b"secret");
        let (payload_b64, mac_b64) = signed.split_once('.').unwrap();
        let mut claims: ChainToken =
            serde_json::from_slice(&URL_SAFE_NO_PAD.decode(payload_b64).unwrap()).unwrap();
        claims.hop = 99;
        let tampered_payload = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims).unwrap());
        let tampered = format!("{tampered_payload}.{mac_b64}");

        let err = ChainToken::verify(&tampered, b"secret", 500).unwrap_err();
        assert_eq!(err, TokenError::BadMac);
    }

    #[test]
    fn verify_rejects_tampered_mac_as_bad_mac() {
        let token = sample();
        let signed = token.sign(b"secret");
        let (payload_b64, _mac_b64) = signed.split_once('.').unwrap();
        let bogus_mac = URL_SAFE_NO_PAD.encode([0u8; 32]);
        let tampered = format!("{payload_b64}.{bogus_mac}");

        let err = ChainToken::verify(&tampered, b"secret", 500).unwrap_err();
        assert_eq!(err, TokenError::BadMac);
    }

    #[test]
    fn verify_rejects_expired_token() {
        let token = sample(); // expires_at_unix = 1_000
        let signed = token.sign(b"secret");
        let err = ChainToken::verify(&signed, b"secret", 1_000).unwrap_err();
        assert_eq!(err, TokenError::Expired);

        let err = ChainToken::verify(&signed, b"secret", 1_001).unwrap_err();
        assert_eq!(err, TokenError::Expired);
    }

    #[test]
    fn verify_accepts_token_just_before_expiry() {
        let token = sample();
        let signed = token.sign(b"secret");
        let verified = ChainToken::verify(&signed, b"secret", 999).unwrap();
        assert_eq!(verified, token);
    }

    #[test]
    fn verify_rejects_missing_dot_as_bad_format() {
        let err = ChainToken::verify("not-a-token", b"secret", 0).unwrap_err();
        assert_eq!(err, TokenError::BadFormat);
    }

    #[test]
    fn verify_rejects_non_base64_payload_as_bad_format() {
        let err = ChainToken::verify("not!base64.also!bad", b"secret", 0).unwrap_err();
        assert_eq!(err, TokenError::BadFormat);
    }

    #[test]
    fn with_incremented_hop_increments_only_hop() {
        let token = sample();
        let bumped = token.clone().with_incremented_hop();
        assert_eq!(bumped.hop, token.hop + 1);
        assert_eq!(bumped.cid, token.cid);
        assert_eq!(bumped.route_id, token.route_id);
        assert_eq!(bumped.resume_index, token.resume_index);
        assert_eq!(bumped.expires_at_unix, token.expires_at_unix);
    }

    #[test]
    fn with_incremented_hop_can_be_chained() {
        let token = sample();
        let bumped = token.with_incremented_hop().with_incremented_hop();
        assert_eq!(bumped.hop, 2);
    }
}
