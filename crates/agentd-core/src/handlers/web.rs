use crate::CapabilityEngine;
use anyhow::{anyhow, Result};
use regex::Regex;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

const USER_AGENT: &str = "Mozilla/5.0 (compatible; agentd/1.0)";

impl CapabilityEngine {
    /// `web_search` — DuckDuckGo Instant Answer + HTML fallback.
    pub(crate) async fn execute_search(
        &self,
        params: &serde_json::Value,
    ) -> Result<serde_json::Value> {
        let query = params
            .get("query")
            .and_then(|value| value.as_str())
            .ok_or_else(|| anyhow!("web.search requires params.query"))?;
        let encoded: String = url::form_urlencoded::byte_serialize(query.as_bytes()).collect();
        let url = format!(
            "https://api.duckduckgo.com/?q={encoded}&format=json&no_html=1&skip_disambig=1"
        );
        let response = self.http.get(url).send().await?;
        let status = response.status();
        let payload: serde_json::Value = response.json().await.unwrap_or_default();
        let mut results = Vec::new();
        if let Some(text) = payload.get("AbstractText").and_then(|value| value.as_str()) {
            if !text.trim().is_empty() {
                results.push(serde_json::json!({
                    "title": payload.get("Heading").and_then(|value| value.as_str()).unwrap_or("web result"),
                    "url": payload.get("AbstractURL").and_then(|value| value.as_str()).unwrap_or_default(),
                    "snippet": text,
                }));
            }
        }
        if let Some(items) = payload
            .get("RelatedTopics")
            .and_then(|value| value.as_array())
        {
            for item in items.iter().take(5) {
                if let (Some(text), Some(url)) = (
                    item.get("Text").and_then(|value| value.as_str()),
                    item.get("FirstURL").and_then(|value| value.as_str()),
                ) {
                    results.push(serde_json::json!({
                        "title": text.split(" - ").next().unwrap_or(text),
                        "url": url,
                        "snippet": text,
                    }));
                }
            }
        }
        if results.is_empty() {
            results = self
                .execute_search_html_fallback(query)
                .await
                .unwrap_or_default();
        }
        Ok(serde_json::json!({
            "status": status.as_u16(),
            "results": results,
        }))
    }

    /// HTML-scrape fallback when DuckDuckGo's Instant Answer JSON is empty
    /// (which it often is for non-encyclopedic queries).
    pub(crate) async fn execute_search_html_fallback(
        &self,
        query: &str,
    ) -> Result<Vec<serde_json::Value>> {
        let encoded: String = url::form_urlencoded::byte_serialize(query.as_bytes()).collect();
        let url = format!("https://html.duckduckgo.com/html/?q={encoded}");
        let response = self
            .http
            .get(url)
            .header(reqwest::header::USER_AGENT, USER_AGENT)
            .send()
            .await?;
        if !response.status().is_success() {
            return Ok(Vec::new());
        }
        let body = response.text().await.unwrap_or_default();
        Ok(extract_html_search_results(&body))
    }

    /// Fetch a bounded public text resource. This deliberately cannot reach
    /// loopback, private, link-local, or non-HTTP addresses; private service
    /// integrations belong behind an explicit MCP capability.
    pub(crate) async fn execute_fetch(
        &self,
        params: &serde_json::Value,
    ) -> Result<serde_json::Value> {
        const MAX_BYTES: usize = 1024 * 1024;
        let mut url = params
            .get("url")
            .and_then(|value| value.as_str())
            .ok_or_else(|| anyhow!("web_fetch requires url"))?
            .parse::<url::Url>()?;
        for _ in 0..=5 {
            let (host, addresses) = resolve_public_url(&url).await?;
            let client = reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .timeout(std::time::Duration::from_secs(30))
                .resolve_to_addrs(&host, &addresses)
                .build()?;
            let mut response = client
                .get(url.clone())
                .header(reqwest::header::USER_AGENT, USER_AGENT)
                .send()
                .await?;
            if response.status().is_redirection() {
                let location = response
                    .headers()
                    .get(reqwest::header::LOCATION)
                    .and_then(|value| value.to_str().ok())
                    .ok_or_else(|| anyhow!("redirect missing location"))?;
                url = url.join(location)?;
                continue;
            }
            let status = response.status();
            if !status.is_success() {
                return Err(anyhow!("web_fetch failed with status {status}"));
            }
            let content_type = response
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .unwrap_or("text/plain")
                .to_string();
            if !(content_type.starts_with("text/")
                || content_type.contains("json")
                || content_type.contains("xml"))
            {
                return Err(anyhow!("web_fetch only accepts text resources"));
            }
            let mut bytes = Vec::new();
            while let Some(chunk) = response.chunk().await? {
                if bytes.len() + chunk.len() > MAX_BYTES {
                    return Err(anyhow!("web_fetch response exceeds {MAX_BYTES} bytes"));
                }
                bytes.extend_from_slice(&chunk);
            }
            let raw = String::from_utf8_lossy(&bytes);
            let (text, title) = if content_type.contains("text/html") {
                extract_visible_html_snapshot(&raw)
            } else {
                (raw.into_owned(), None)
            };
            return Ok(serde_json::json!({
                "url": url,
                "status": status.as_u16(),
                "content_type": content_type,
                "title": title,
                "text": text,
            }));
        }
        Err(anyhow!("web_fetch exceeded redirect limit"))
    }
}

async fn resolve_public_url(url: &url::Url) -> Result<(String, Vec<SocketAddr>)> {
    if !matches!(url.scheme(), "http" | "https") {
        return Err(anyhow!("web_fetch only supports http and https"));
    }
    let host = url
        .host_str()
        .ok_or_else(|| anyhow!("web_fetch URL has no host"))?;
    let lookup_name = host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(host);
    if lookup_name.eq_ignore_ascii_case("localhost") || lookup_name.ends_with(".localhost") {
        return Err(anyhow!("web_fetch rejects local hosts"));
    }
    let port = url.port_or_known_default().unwrap_or(80);
    let addresses = match lookup_name.parse::<IpAddr>() {
        Ok(ip) => vec![SocketAddr::new(ip, port)],
        Err(_) => tokio::net::lookup_host((lookup_name, port))
            .await?
            .collect::<Vec<_>>(),
    };
    if addresses.is_empty() {
        return Err(anyhow!("web_fetch host did not resolve"));
    }
    for address in &addresses {
        if !is_public_ip(address.ip()) {
            return Err(anyhow!("web_fetch rejects non-public addresses"));
        }
    }
    Ok((lookup_name.to_string(), addresses))
}

fn is_public_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => is_public_v4(ip),
        IpAddr::V6(ip) => is_public_v6(ip),
    }
}

fn is_public_v4(ip: Ipv4Addr) -> bool {
    let octets = ip.octets();
    !(ip.is_private()
        || ip.is_loopback()
        || ip.is_link_local()
        || ip.is_broadcast()
        || ip.is_unspecified()
        || ip.is_multicast()
        || ip.is_documentation()
        || octets[0] == 0
        || octets[0] >= 224
        || (octets[0] == 100 && (64..=127).contains(&octets[1]))
        || (octets[0] == 198 && (18..=19).contains(&octets[1])))
}

fn is_public_v6(ip: Ipv6Addr) -> bool {
    !(ip.is_loopback()
        || ip.is_unspecified()
        || ip.is_multicast()
        || ip.is_unique_local()
        || ip.is_unicast_link_local()
        || ip.segments()[0] == 0x2001 && ip.segments()[1] == 0x0db8)
}

pub(crate) fn extract_html_search_results(body: &str) -> Vec<serde_json::Value> {
    let link_re =
        Regex::new(r#"(?s)<a[^>]*class="[^"]*result__a[^"]*"[^>]*href="([^"]+)"[^>]*>(.*?)</a>"#)
            .expect("valid link regex");
    let snippet_re = Regex::new(r#"(?s)<a[^>]*class="[^"]*result__snippet[^"]*"[^>]*>(.*?)</a>"#)
        .expect("valid snippet regex");
    let tag_re = Regex::new(r"<[^>]+>").expect("valid tag regex");

    let snippets = snippet_re
        .captures_iter(body)
        .filter_map(|caps| {
            caps.get(1)
                .map(|m| html_text(tag_re.replace_all(m.as_str(), "").as_ref()))
        })
        .collect::<Vec<_>>();

    link_re
        .captures_iter(body)
        .take(5)
        .enumerate()
        .filter_map(|(idx, caps)| {
            let url = caps.get(1)?.as_str();
            let title_raw = caps.get(2)?.as_str();
            let title = html_text(tag_re.replace_all(title_raw, "").as_ref());
            if title.is_empty() || url.is_empty() {
                return None;
            }
            Some(serde_json::json!({
                "title": title,
                "url": url,
                "snippet": snippets.get(idx).cloned().unwrap_or_default(),
            }))
        })
        .collect()
}

pub(crate) fn html_text(input: &str) -> String {
    input
        .replace("&amp;", "&")
        .replace("&quot;", "\"")
        .replace("&#x27;", "'")
        .replace("&#39;", "'")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&nbsp;", " ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

pub(crate) fn extract_visible_html_snapshot(body: &str) -> (String, Option<String>) {
    let title_re = Regex::new(r#"(?is)<title[^>]*>(.*?)</title>"#).expect("valid title regex");
    let strip_re = Regex::new(
        r#"(?is)<(script|style|noscript|svg|canvas|template)[^>]*>.*?</(script|style|noscript|svg|canvas|template)>"#,
    )
    .expect("valid strip regex");
    let tag_re = Regex::new(r"(?is)<[^>]+>").expect("valid tag regex");
    let title = title_re
        .captures(body)
        .and_then(|caps| caps.get(1))
        .map(|match_| html_text(match_.as_str()))
        .filter(|text| !text.is_empty());
    let stripped = strip_re.replace_all(body, " ");
    let text = html_text(tag_re.replace_all(&stripped, " ").as_ref());
    (text, title)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn public_url_resolution_rejects_private_addresses() {
        for raw in [
            "http://127.0.0.1/",
            "http://10.0.0.1/",
            "http://169.254.169.254/latest/meta-data/",
            "http://[::1]/",
        ] {
            let error = resolve_public_url(&raw.parse().unwrap()).await.unwrap_err();
            assert!(
                error.to_string().contains("non-public"),
                "unexpected rejection for {raw}: {error}"
            );
        }
    }

    #[tokio::test]
    async fn public_url_resolution_returns_the_checked_socket_addresses() {
        let url = "https://1.1.1.1/".parse().unwrap();
        let (host, addresses) = resolve_public_url(&url).await.unwrap();
        assert_eq!(host, "1.1.1.1");
        assert!(!addresses.is_empty());
        assert!(addresses
            .iter()
            .all(|address| is_public_ip(address.ip()) && address.port() == 443));
    }
}
