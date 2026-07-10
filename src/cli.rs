use std::path::PathBuf;

use clap::Parser;

/// Sluice — a modular self-hosted request gateway.
#[derive(Debug, Parser)]
#[command(name = "sluice", version, about)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, clap::Subcommand)]
pub enum Command {
    /// Run the gateway (default long-running process).
    Serve {
        #[arg(long, default_value = "sluice.toml", conflicts_with = "config_dir")]
        config: PathBuf,
        /// Load config from a directory (`gateway.toml` + `routes.d/*.toml`)
        /// instead of a single file. Mutually exclusive with `--config`.
        #[arg(long)]
        config_dir: Option<PathBuf>,
        /// Serve only the named `gateways.d` gateway's listener (design doc
        /// M13). Requires a `--config-dir` config that defines a gateway
        /// with this name; an unknown name is a startup error.
        #[arg(long)]
        gateway: Option<String>,
    },
    /// Validate config without serving; non-zero exit on error.
    Check {
        #[arg(long, default_value = "sluice.toml", conflicts_with = "config_dir")]
        config: PathBuf,
        /// Validate a config directory instead of a single file. Mutually
        /// exclusive with `--config`.
        #[arg(long)]
        config_dir: Option<PathBuf>,
    },
    /// Print the effective, merged configuration (gateway settings, routes,
    /// steps) without serving; non-zero exit on error.
    Routes {
        #[arg(long, default_value = "sluice.toml", conflicts_with = "config_dir")]
        config: PathBuf,
        /// Render a config directory instead of a single file. Mutually
        /// exclusive with `--config`.
        #[arg(long)]
        config_dir: Option<PathBuf>,
    },
    /// Inspect or refresh the model-facts registry (`sluice list/diff/update`).
    Models {
        #[command(subcommand)]
        action: ModelsAction,
    },
    /// Send a synthetic probe request through a route's `on_request` chain
    /// and report each step's directive and timing, without ever forwarding
    /// to the real upstream (design doc §14).
    Test {
        /// The route id to probe (must be defined in the effective config).
        route: String,
        #[arg(long, default_value = "sluice.toml", conflicts_with = "config_dir")]
        config: PathBuf,
        /// Probe a route from a config directory instead of a single file.
        /// Mutually exclusive with `--config`.
        #[arg(long)]
        config_dir: Option<PathBuf>,
    },
    /// Inspect a signed `x-chain-token` (design doc §4.5, M12).
    Token {
        #[command(subcommand)]
        action: TokenAction,
    },
}

/// `sluice token` subcommands. Entirely offline (no server, no network) — a
/// chain token is a self-contained, HMAC-signed value, so decoding one only
/// needs the same secret the gateway itself was configured with.
#[derive(Debug, clap::Subcommand)]
pub enum TokenAction {
    /// Verify a chain token's signature and print its decoded claims (cid,
    /// route_id, resume_index, hop, expires_at) plus its validity (valid /
    /// bad mac / bad format / expired). An expired-but-correctly-signed
    /// token still has its claims printed, alongside noting it's expired.
    Verify {
        /// HMAC secret to verify against (must match the gateway's
        /// `[gateway] loopback_secret`).
        #[arg(long)]
        secret: String,
        /// The signed token itself.
        token: String,
    },
}

/// `sluice models` subcommands. See design doc §9.6: the registry is
/// refreshed offline via `update`/`diff` against a local models.dev-shaped
/// directory, never fetched on the request path.
#[derive(Debug, clap::Subcommand)]
pub enum ModelsAction {
    /// Print the currently loaded registry (`Registry::load()`): the local
    /// registry file if `models update` has written one, else the embedded
    /// seed.
    List {
        /// Only show models for this provider id (e.g. `anthropic`).
        #[arg(long)]
        provider: Option<String>,
        /// Print as a JSON array instead of a text table.
        #[arg(long)]
        json: bool,
    },
    /// Resolve a models.dev-shaped source — a local directory (`--source`)
    /// or a live fetch of the models.dev dataset (`--from-network`) — and
    /// write it as the local registry file. Writes ONLY that file — never
    /// config or routes.
    Update {
        /// Local directory in models.dev's `models/` + `providers/` shape.
        /// Mutually exclusive with `--from-network`; exactly one is
        /// required.
        #[arg(
            long,
            conflicts_with = "from_network",
            required_unless_present = "from_network"
        )]
        source: Option<PathBuf>,
        /// Resolve from a live fetch of the models.dev dataset instead of a
        /// local directory. Mutually exclusive with `--source`; exactly one
        /// is required.
        #[arg(long, conflicts_with = "source")]
        from_network: bool,
        /// Override the models.dev URL `--from-network` fetches (default:
        /// `registry::models_dev::MODELS_DEV_URL`). Hidden — test-only, so
        /// integration tests can point at a mock server instead of the real
        /// network.
        #[arg(long, hide = true)]
        network_url: Option<String>,
        /// Where to write the resolved registry JSON. Default must match
        /// `registry::DEFAULT_REGISTRY_PATH`, the path `Registry::load`
        /// checks for a locally-refreshed registry.
        #[arg(long, default_value = "sluice-models.json")]
        out: PathBuf,
    },
    /// Resolve a models.dev-shaped source — a local directory (`--source`)
    /// or a live fetch of the models.dev dataset (`--from-network`) — and
    /// print what would change vs. the currently loaded registry, without
    /// writing anything.
    Diff {
        /// Local directory in models.dev's `models/` + `providers/` shape.
        /// Mutually exclusive with `--from-network`; exactly one is
        /// required.
        #[arg(
            long,
            conflicts_with = "from_network",
            required_unless_present = "from_network"
        )]
        source: Option<PathBuf>,
        /// Resolve from a live fetch of the models.dev dataset instead of a
        /// local directory. Mutually exclusive with `--source`; exactly one
        /// is required.
        #[arg(long, conflicts_with = "source")]
        from_network: bool,
        /// Override the models.dev URL `--from-network` fetches. Hidden —
        /// test-only (see `Update::network_url`).
        #[arg(long, hide = true)]
        network_url: Option<String>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_check_with_explicit_config() {
        let cli = Cli::try_parse_from(["sluice", "check", "--config", "custom.toml"]).unwrap();
        match cli.command {
            Command::Check { config, config_dir } => {
                assert_eq!(config, PathBuf::from("custom.toml"));
                assert_eq!(config_dir, None);
            }
            other => panic!("expected Check, got {other:?}"),
        }
    }

    #[test]
    fn serve_defaults_config_path() {
        let cli = Cli::try_parse_from(["sluice", "serve"]).unwrap();
        match cli.command {
            Command::Serve {
                config,
                config_dir,
                gateway,
            } => {
                assert_eq!(config, PathBuf::from("sluice.toml"));
                assert_eq!(config_dir, None);
                assert_eq!(gateway, None);
            }
            other => panic!("expected Serve, got {other:?}"),
        }
    }

    #[test]
    fn parses_serve_with_gateway_flag() {
        let cli = Cli::try_parse_from([
            "sluice",
            "serve",
            "--config-dir",
            "conf.d",
            "--gateway",
            "public",
        ])
        .unwrap();
        match cli.command {
            Command::Serve { gateway, .. } => {
                assert_eq!(gateway, Some("public".to_string()));
            }
            other => panic!("expected Serve, got {other:?}"),
        }
    }

    #[test]
    fn parses_serve_with_config_dir() {
        let cli = Cli::try_parse_from(["sluice", "serve", "--config-dir", "conf.d"]).unwrap();
        match cli.command {
            Command::Serve { config_dir, .. } => {
                assert_eq!(config_dir, Some(PathBuf::from("conf.d")));
            }
            other => panic!("expected Serve, got {other:?}"),
        }
    }

    #[test]
    fn parses_check_with_config_dir() {
        let cli = Cli::try_parse_from(["sluice", "check", "--config-dir", "conf.d"]).unwrap();
        match cli.command {
            Command::Check { config_dir, .. } => {
                assert_eq!(config_dir, Some(PathBuf::from("conf.d")));
            }
            other => panic!("expected Check, got {other:?}"),
        }
    }

    #[test]
    fn rejects_config_and_config_dir_together_for_serve() {
        let err = Cli::try_parse_from([
            "sluice",
            "serve",
            "--config",
            "a.toml",
            "--config-dir",
            "conf.d",
        ])
        .unwrap_err();
        assert_eq!(
            err.kind(),
            clap::error::ErrorKind::ArgumentConflict,
            "{err}"
        );
    }

    #[test]
    fn rejects_config_and_config_dir_together_for_check() {
        let err = Cli::try_parse_from([
            "sluice",
            "check",
            "--config",
            "a.toml",
            "--config-dir",
            "conf.d",
        ])
        .unwrap_err();
        assert_eq!(
            err.kind(),
            clap::error::ErrorKind::ArgumentConflict,
            "{err}"
        );
    }

    #[test]
    fn parses_routes_with_config_dir() {
        let cli = Cli::try_parse_from(["sluice", "routes", "--config-dir", "conf.d"]).unwrap();
        match cli.command {
            Command::Routes { config_dir, .. } => {
                assert_eq!(config_dir, Some(PathBuf::from("conf.d")));
            }
            other => panic!("expected Routes, got {other:?}"),
        }
    }

    #[test]
    fn routes_defaults_config_path() {
        let cli = Cli::try_parse_from(["sluice", "routes"]).unwrap();
        match cli.command {
            Command::Routes { config, config_dir } => {
                assert_eq!(config, PathBuf::from("sluice.toml"));
                assert_eq!(config_dir, None);
            }
            other => panic!("expected Routes, got {other:?}"),
        }
    }

    #[test]
    fn rejects_config_and_config_dir_together_for_routes() {
        let err = Cli::try_parse_from([
            "sluice",
            "routes",
            "--config",
            "a.toml",
            "--config-dir",
            "conf.d",
        ])
        .unwrap_err();
        assert_eq!(
            err.kind(),
            clap::error::ErrorKind::ArgumentConflict,
            "{err}"
        );
    }

    #[test]
    fn parses_test_with_explicit_config() {
        let cli =
            Cli::try_parse_from(["sluice", "test", "claude", "--config", "custom.toml"]).unwrap();
        match cli.command {
            Command::Test {
                route,
                config,
                config_dir,
            } => {
                assert_eq!(route, "claude");
                assert_eq!(config, PathBuf::from("custom.toml"));
                assert_eq!(config_dir, None);
            }
            other => panic!("expected Test, got {other:?}"),
        }
    }

    #[test]
    fn test_defaults_config_path() {
        let cli = Cli::try_parse_from(["sluice", "test", "claude"]).unwrap();
        match cli.command {
            Command::Test { config, .. } => {
                assert_eq!(config, PathBuf::from("sluice.toml"));
            }
            other => panic!("expected Test, got {other:?}"),
        }
    }

    #[test]
    fn parses_test_with_config_dir() {
        let cli =
            Cli::try_parse_from(["sluice", "test", "claude", "--config-dir", "conf.d"]).unwrap();
        match cli.command {
            Command::Test { config_dir, .. } => {
                assert_eq!(config_dir, Some(PathBuf::from("conf.d")));
            }
            other => panic!("expected Test, got {other:?}"),
        }
    }

    #[test]
    fn rejects_config_and_config_dir_together_for_test() {
        let err = Cli::try_parse_from([
            "sluice",
            "test",
            "claude",
            "--config",
            "a.toml",
            "--config-dir",
            "conf.d",
        ])
        .unwrap_err();
        assert_eq!(
            err.kind(),
            clap::error::ErrorKind::ArgumentConflict,
            "{err}"
        );
    }

    #[test]
    fn test_requires_route_argument() {
        let err = Cli::try_parse_from(["sluice", "test"]).unwrap_err();
        assert_eq!(
            err.kind(),
            clap::error::ErrorKind::MissingRequiredArgument,
            "{err}"
        );
    }

    #[test]
    fn parses_models_list_defaults() {
        let cli = Cli::try_parse_from(["sluice", "models", "list"]).unwrap();
        match cli.command {
            Command::Models {
                action: ModelsAction::List { provider, json },
            } => {
                assert_eq!(provider, None);
                assert!(!json);
            }
            other => panic!("expected Models::List, got {other:?}"),
        }
    }

    #[test]
    fn parses_models_list_with_provider_and_json() {
        let cli = Cli::try_parse_from([
            "sluice",
            "models",
            "list",
            "--provider",
            "anthropic",
            "--json",
        ])
        .unwrap();
        match cli.command {
            Command::Models {
                action: ModelsAction::List { provider, json },
            } => {
                assert_eq!(provider, Some("anthropic".to_string()));
                assert!(json);
            }
            other => panic!("expected Models::List, got {other:?}"),
        }
    }

    #[test]
    fn parses_models_update_with_default_out() {
        let cli =
            Cli::try_parse_from(["sluice", "models", "update", "--source", "src-dir"]).unwrap();
        match cli.command {
            Command::Models {
                action: ModelsAction::Update { source, out, .. },
            } => {
                assert_eq!(source, Some(PathBuf::from("src-dir")));
                assert_eq!(out, PathBuf::from("sluice-models.json"));
            }
            other => panic!("expected Models::Update, got {other:?}"),
        }
    }

    #[test]
    fn parses_models_update_with_explicit_out() {
        let cli = Cli::try_parse_from([
            "sluice", "models", "update", "--source", "src-dir", "--out", "out.json",
        ])
        .unwrap();
        match cli.command {
            Command::Models {
                action: ModelsAction::Update { out, .. },
            } => {
                assert_eq!(out, PathBuf::from("out.json"));
            }
            other => panic!("expected Models::Update, got {other:?}"),
        }
    }

    #[test]
    fn parses_models_diff() {
        let cli = Cli::try_parse_from(["sluice", "models", "diff", "--source", "src-dir"]).unwrap();
        match cli.command {
            Command::Models {
                action: ModelsAction::Diff { source, .. },
            } => {
                assert_eq!(source, Some(PathBuf::from("src-dir")));
            }
            other => panic!("expected Models::Diff, got {other:?}"),
        }
    }

    #[test]
    fn models_update_requires_source() {
        let err = Cli::try_parse_from(["sluice", "models", "update"]).unwrap_err();
        assert_eq!(
            err.kind(),
            clap::error::ErrorKind::MissingRequiredArgument,
            "{err}"
        );
    }

    #[test]
    fn models_diff_requires_source() {
        let err = Cli::try_parse_from(["sluice", "models", "diff"]).unwrap_err();
        assert_eq!(
            err.kind(),
            clap::error::ErrorKind::MissingRequiredArgument,
            "{err}"
        );
    }

    #[test]
    fn parses_models_update_with_from_network() {
        let cli = Cli::try_parse_from(["sluice", "models", "update", "--from-network"]).unwrap();
        match cli.command {
            Command::Models {
                action:
                    ModelsAction::Update {
                        source,
                        from_network,
                        network_url,
                        out,
                    },
            } => {
                assert_eq!(source, None);
                assert!(from_network);
                assert_eq!(network_url, None);
                assert_eq!(out, PathBuf::from("sluice-models.json"));
            }
            other => panic!("expected Models::Update, got {other:?}"),
        }
    }

    #[test]
    fn parses_models_update_with_from_network_and_hidden_network_url_override() {
        let cli = Cli::try_parse_from([
            "sluice",
            "models",
            "update",
            "--from-network",
            "--network-url",
            "http://127.0.0.1:9999/api.json",
        ])
        .unwrap();
        match cli.command {
            Command::Models {
                action: ModelsAction::Update { network_url, .. },
            } => {
                assert_eq!(
                    network_url,
                    Some("http://127.0.0.1:9999/api.json".to_string())
                );
            }
            other => panic!("expected Models::Update, got {other:?}"),
        }
    }

    #[test]
    fn parses_models_diff_with_from_network() {
        let cli = Cli::try_parse_from(["sluice", "models", "diff", "--from-network"]).unwrap();
        match cli.command {
            Command::Models {
                action:
                    ModelsAction::Diff {
                        source,
                        from_network,
                        ..
                    },
            } => {
                assert_eq!(source, None);
                assert!(from_network);
            }
            other => panic!("expected Models::Diff, got {other:?}"),
        }
    }

    #[test]
    fn rejects_source_and_from_network_together_for_models_update() {
        let err = Cli::try_parse_from([
            "sluice",
            "models",
            "update",
            "--source",
            "src-dir",
            "--from-network",
        ])
        .unwrap_err();
        assert_eq!(
            err.kind(),
            clap::error::ErrorKind::ArgumentConflict,
            "{err}"
        );
    }

    #[test]
    fn rejects_source_and_from_network_together_for_models_diff() {
        let err = Cli::try_parse_from([
            "sluice",
            "models",
            "diff",
            "--source",
            "src-dir",
            "--from-network",
        ])
        .unwrap_err();
        assert_eq!(
            err.kind(),
            clap::error::ErrorKind::ArgumentConflict,
            "{err}"
        );
    }

    #[test]
    fn parses_token_verify() {
        let cli =
            Cli::try_parse_from(["sluice", "token", "verify", "--secret", "s3cr3t", "abc.def"])
                .unwrap();
        match cli.command {
            Command::Token {
                action: TokenAction::Verify { secret, token },
            } => {
                assert_eq!(secret, "s3cr3t");
                assert_eq!(token, "abc.def");
            }
            other => panic!("expected Token::Verify, got {other:?}"),
        }
    }

    #[test]
    fn token_verify_requires_secret() {
        let err = Cli::try_parse_from(["sluice", "token", "verify", "abc.def"]).unwrap_err();
        assert_eq!(
            err.kind(),
            clap::error::ErrorKind::MissingRequiredArgument,
            "{err}"
        );
    }
}
