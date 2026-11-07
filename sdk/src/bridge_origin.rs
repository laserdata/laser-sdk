use axum::http::HeaderMap;
use axum::http::header::{HOST, ORIGIN};
use std::net::IpAddr;

// Whether a request may reach a bridge HTTP endpoint. A request without an
// `Origin` header comes from a non-browser client. A browser request passes
// when its origin is allowlisted, or when it is same-origin with a `Host` a
// DNS rebinding cannot name: an IP literal or `localhost`. A rebound page sends
// its own name as both origin and host, so a same-origin check on any other
// host would pass it.
pub(crate) fn origin_allowed(headers: &HeaderMap, allowed: &[String]) -> bool {
    let Some(origin) = headers.get(ORIGIN) else {
        return true;
    };
    let Ok(origin) = origin.to_str() else {
        return false;
    };
    if allowed.iter().any(|allowed| allowed == origin) {
        return true;
    }
    let Some(authority) = origin
        .strip_prefix("http://")
        .or_else(|| origin.strip_prefix("https://"))
    else {
        return false;
    };
    headers
        .get(HOST)
        .and_then(|host| host.to_str().ok())
        .is_some_and(|host| host.eq_ignore_ascii_case(authority) && unrebindable(host))
}

// Whether a `Host` value names an IP literal or `localhost`, a host no DNS
// answer can point a foreign page at.
fn unrebindable(host: &str) -> bool {
    let name = match host.strip_prefix('[') {
        Some(bracketed) => bracketed.split(']').next().unwrap_or_default(),
        None => host.rsplit_once(':').map_or(host, |(name, _port)| name),
    };
    name.eq_ignore_ascii_case("localhost") || name.parse::<IpAddr>().is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_origin_headers_when_checked_then_should_allow_listed_same_origin_and_non_browser_requests()
     {
        let headers = |host: Option<&str>, origin: Option<&str>| {
            let mut headers = HeaderMap::new();
            if let Some(host) = host {
                headers.insert(HOST, host.parse().expect("a valid host header"));
            }
            if let Some(origin) = origin {
                headers.insert(ORIGIN, origin.parse().expect("a valid origin header"));
            }
            headers
        };
        let allowed = vec!["https://app.example.com".to_owned()];
        for (host, origin, expected) in [
            (Some("bridge.example:8080"), None, true),
            (
                Some("bridge.example:8080"),
                Some("https://app.example.com"),
                true,
            ),
            (Some("127.0.0.1:8080"), Some("http://127.0.0.1:8080"), true),
            (Some("[::1]:8080"), Some("http://[::1]:8080"), true),
            (Some("LOCALHOST:8080"), Some("http://localhost:8080"), true),
            (
                Some("evil.example:8080"),
                Some("http://evil.example:8080"),
                false,
            ),
            (Some("127.0.0.1:8080"), Some("http://127.0.0.1:9090"), false),
            (Some("127.0.0.1:8080"), Some("null"), false),
            (None, Some("http://127.0.0.1:8080"), false),
            (Some("127.0.0.1:8080"), Some("https://evil.example"), false),
        ] {
            assert_eq!(
                origin_allowed(&headers(host, origin), &allowed),
                expected,
                "{origin:?} on {host:?}"
            );
        }
    }
}
