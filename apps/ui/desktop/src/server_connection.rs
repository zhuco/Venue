pub const DEFAULT_CONTROL_ENDPOINT: &str = "https://clawdbotweb.site";

pub fn default_control_endpoint() -> String {
    std::env::var("VENUE_CONTROL_URL")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_CONTROL_ENDPOINT.to_owned())
}

pub(crate) fn http_client_builder(endpoint: &str) -> reqwest::ClientBuilder {
    control_proxy_policy(reqwest::Client::builder(), endpoint)
}

fn control_proxy_policy(builder: reqwest::ClientBuilder, endpoint: &str) -> reqwest::ClientBuilder {
    #[cfg(not(target_arch = "wasm32"))]
    if reqwest::Url::parse(endpoint).ok().is_some_and(|url| {
        url.host_str().is_some_and(|host| {
            host == "localhost"
                || host
                    .trim_matches(['[', ']'])
                    .parse::<std::net::IpAddr>()
                    .is_ok_and(|ip| ip.is_loopback())
        })
    }) {
        // A local Control tunnel must not depend on the system's Internet proxy.
        return builder.no_proxy();
    }
    let _ = endpoint;
    builder
}

pub(crate) fn normalize_endpoint(value: &str) -> Option<String> {
    let value = value.trim();
    if value.len() > 512 || !crate::account_client::safe_endpoint(value) {
        return None;
    }
    let url = reqwest::Url::parse(value).ok()?;
    Some(url.as_str().trim_end_matches('/').to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn local_control_remains_reachable_when_the_internet_proxy_is_unavailable()
    -> Result<(), Box<dyn std::error::Error>> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            let mut buffer = [0; 1024];
            stream.read(&mut buffer).await?;
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
                .await
        });
        let builder = reqwest::Client::builder()
            .proxy(reqwest::Proxy::all("http://127.0.0.1:1")?)
            .timeout(std::time::Duration::from_secs(2));
        let response = control_proxy_policy(builder, &endpoint)
            .build()?
            .get(&endpoint)
            .send()
            .await?;
        assert_eq!(response.text().await?, "ok");
        server.await??;
        Ok(())
    }

    #[test]
    fn server_address_keeps_https_validation_and_normalizes_input() {
        assert_eq!(
            normalize_endpoint("  https://CLAWDBOTWEB.site/  ").as_deref(),
            Some(DEFAULT_CONTROL_ENDPOINT)
        );
        assert_eq!(
            normalize_endpoint("https://192.0.2.1:8443/").as_deref(),
            Some("https://192.0.2.1:8443")
        );
        assert!(normalize_endpoint("http://127.0.0.1:39180").is_some());
        for value in [
            "",
            "http://192.0.2.1:39180",
            "https://user:password@example.com",
            "https://example.com?token=secret",
            "https://example.com#fragment",
        ] {
            assert!(normalize_endpoint(value).is_none());
        }
    }
}
