use crate::config::{Config, Route};

#[derive(Debug)]
pub struct RouteMatch<'a> {
    pub route: &'a Route,
    pub upstream_url: String,
}

/// Split a query string off a path+query, e.g. "/a/b?x=1" -> ("/a/b", Some("x=1")).
fn split_path_query(path: &str) -> (&str, Option<&str>) {
    match path.find('?') {
        Some(idx) => (&path[..idx], Some(&path[idx + 1..])),
        None => (path, None),
    }
}

/// Split a (query-free) path into (route id, remainder). The remainder keeps
/// its leading '/' when present, e.g. "claude/v1" -> ("claude", "/v1").
fn split_id_rest(path_no_query: &str) -> (&str, &str) {
    let trimmed = path_no_query.strip_prefix('/').unwrap_or(path_no_query);
    match trimmed.find('/') {
        Some(idx) => (&trimmed[..idx], &trimmed[idx..]),
        None => (trimmed, ""),
    }
}

/// Join an upstream base URL with an inbound gateway path, stripping the
/// leading route-id segment and re-appending any query string. This is the
/// single source of truth for the upstream join so `route_request` and any
/// later recomputation (e.g. after a `set_path` step mutates the request)
/// stay in sync.
pub fn upstream_url_for(upstream_base: &str, path: &str) -> String {
    let (path_no_query, query) = split_path_query(path);
    let (_id, rest) = split_id_rest(path_no_query);
    let base = upstream_base.trim_end_matches('/');
    let suffix = if rest.is_empty() { "/" } else { rest };
    match query {
        Some(q) => format!("{base}{suffix}?{q}"),
        None => format!("{base}{suffix}"),
    }
}

pub fn route_request<'a>(cfg: &'a Config, path: &str) -> Option<RouteMatch<'a>> {
    // Split the query off before extracting the route id, so a bare route
    // followed by a query string (e.g. "/claude?beta=1") still matches.
    let (path_no_query, _query) = split_path_query(path);
    let (id, _rest) = split_id_rest(path_no_query);
    if id.is_empty() {
        return None;
    }
    let route = cfg.routes.iter().find(|r| r.id == id)?;
    let upstream_url = upstream_url_for(&route.upstream, path);
    Some(RouteMatch {
        route,
        upstream_url,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::load::load_str;

    fn cfg() -> Config {
        load_str(
            r#"
            [[route]]
            id = "claude"
            upstream = "https://api.anthropic.com"
        "#,
        )
        .unwrap()
    }

    #[test]
    fn matches_and_appends_remainder() {
        let c = cfg();
        let m = route_request(&c, "/claude/v1/messages").unwrap();
        assert_eq!(m.route.id, "claude");
        assert_eq!(m.upstream_url, "https://api.anthropic.com/v1/messages");
    }

    #[test]
    fn bare_route_appends_root() {
        let c = cfg();
        let m = route_request(&c, "/claude").unwrap();
        assert_eq!(m.upstream_url, "https://api.anthropic.com/");
    }

    #[test]
    fn unknown_route_is_none() {
        assert!(route_request(&cfg(), "/openai/v1").is_none());
    }

    #[test]
    fn empty_path_is_none() {
        assert!(route_request(&cfg(), "/").is_none());
    }

    #[test]
    fn trailing_slash_upstream_not_doubled() {
        let c = load_str(
            r#"
            [[route]]
            id = "x"
            upstream = "http://u/"
        "#,
        )
        .unwrap();
        let m = route_request(&c, "/x/a").unwrap();
        assert_eq!(m.upstream_url, "http://u/a");
    }

    #[test]
    fn bare_route_with_query_string_matches() {
        let c = cfg();
        let m = route_request(&c, "/claude?beta=1").unwrap();
        assert_eq!(m.route.id, "claude");
        assert_eq!(m.upstream_url, "https://api.anthropic.com/?beta=1");
    }

    #[test]
    fn route_with_rest_and_query_string_appends_both() {
        let c = cfg();
        let m = route_request(&c, "/claude/v1?x=1").unwrap();
        assert_eq!(m.route.id, "claude");
        assert_eq!(m.upstream_url, "https://api.anthropic.com/v1?x=1");
    }
}
