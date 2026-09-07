// Copyright 2026 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt;
use std::time::Duration;

use anyhow::Context;
use reqwest::{Client, StatusCode, Url};
use risingwave_common::bail;
use risingwave_common::util::retry::exponential_backoff;
use tokio_retry::RetryIf;
use tokio_retry::strategy::jitter;

use super::PulsarSchemaInfo;
use crate::Get;
use crate::error::ConnectorResult;
use crate::source::pulsar::topic::parse_topic;

pub const PULSAR_SCHEMA_URL_KEY: &str = "schema.pulsar.url";
pub const PULSAR_SCHEMA_AUTH_TOKEN_KEY: &str = "schema.pulsar.auth.token";

const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_RETRY_DELAY_MS: u64 = 100;
const DEFAULT_MAX_RETRY_DELAY: Duration = Duration::from_secs(3);
const DEFAULT_MAX_RETRIES: usize = 3;

#[derive(Debug)]
enum PulsarSchemaRequestError {
    Retryable(anyhow::Error),
    Permanent(anyhow::Error),
}

impl PulsarSchemaRequestError {
    fn is_retryable(&self) -> bool {
        matches!(self, Self::Retryable(_))
    }

    fn as_inner(&self) -> &anyhow::Error {
        match self {
            Self::Retryable(error) | Self::Permanent(error) => error,
        }
    }

    fn into_inner(self) -> anyhow::Error {
        match self {
            Self::Retryable(error) | Self::Permanent(error) => error,
        }
    }
}

#[derive(Clone)]
pub struct PulsarSchemaClientConfig {
    admin_url: String,
    bearer_token: Option<String>,
}

impl fmt::Debug for PulsarSchemaClientConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PulsarSchemaClientConfig")
            .field("admin_url", &self.admin_url)
            .field(
                "bearer_token",
                &self.bearer_token.as_ref().map(|_| "[REDACTED]"),
            )
            .finish()
    }
}

impl PulsarSchemaClientConfig {
    pub fn from_options(options: &impl Get) -> ConnectorResult<Option<Self>> {
        let admin_url = options.get(PULSAR_SCHEMA_URL_KEY).cloned();
        let bearer_token = options.get(PULSAR_SCHEMA_AUTH_TOKEN_KEY).cloned();

        let Some(admin_url) = admin_url else {
            if bearer_token.is_some() {
                bail!("`{PULSAR_SCHEMA_AUTH_TOKEN_KEY}` requires `{PULSAR_SCHEMA_URL_KEY}`");
            }
            return Ok(None);
        };
        if admin_url.is_empty() {
            bail!("`{PULSAR_SCHEMA_URL_KEY}` must not be empty");
        }
        if bearer_token.as_ref().is_some_and(String::is_empty) {
            bail!("`{PULSAR_SCHEMA_AUTH_TOKEN_KEY}` must not be empty");
        }

        Ok(Some(Self {
            admin_url,
            bearer_token,
        }))
    }

    pub fn admin_url(&self) -> &str {
        &self.admin_url
    }

    pub fn bearer_token(&self) -> Option<&str> {
        self.bearer_token.as_deref()
    }
}

#[derive(Clone)]
pub struct PulsarSchemaClient {
    http_client: Client,
    admin_url: Url,
    bearer_token: Option<String>,
}

impl fmt::Debug for PulsarSchemaClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PulsarSchemaClient")
            .field("admin_url", &self.admin_url)
            .field(
                "bearer_token",
                &self.bearer_token.as_ref().map(|_| "[REDACTED]"),
            )
            .finish_non_exhaustive()
    }
}

impl PulsarSchemaClient {
    pub fn new(config: PulsarSchemaClientConfig) -> ConnectorResult<Self> {
        let mut admin_url = Url::parse(&config.admin_url).context("invalid Pulsar schema URL")?;
        if admin_url.cannot_be_a_base() {
            bail!("Pulsar schema URL must be a base URL");
        }
        if !matches!(admin_url.scheme(), "http" | "https") {
            bail!("Pulsar schema URL must use HTTP or HTTPS");
        }
        if !admin_url.username().is_empty() || admin_url.password().is_some() {
            bail!("Pulsar schema URL must not contain credentials");
        }
        admin_url.set_query(None);
        admin_url.set_fragment(None);

        let http_client = Client::builder()
            .timeout(DEFAULT_REQUEST_TIMEOUT)
            .build()
            .context("failed to build Pulsar schema client")?;
        Ok(Self {
            http_client,
            admin_url,
            bearer_token: config.bearer_token,
        })
    }

    fn build_schema_url(&self, topic: &str, version: Option<i64>) -> ConnectorResult<Url> {
        let topic = parse_topic(topic)?;
        let topic_name = topic.topic_str_without_partition()?;
        let mut url = self.admin_url.clone();
        let mut path = url
            .path_segments_mut()
            .map_err(|_| anyhow::anyhow!("Pulsar schema URL must be a base URL"))?;
        path.extend([
            "admin",
            "v2",
            "schemas",
            topic.tenant.as_str(),
            topic.namespace.as_str(),
            topic_name.as_str(),
            "schema",
        ]);
        if let Some(version) = version {
            path.push(version.to_string().as_str());
        }
        drop(path);
        Ok(url)
    }

    async fn request_schema(
        &self,
        url: &Url,
    ) -> Result<PulsarSchemaInfo, PulsarSchemaRequestError> {
        let mut request = self.http_client.get(url.clone());
        if let Some(token) = self.bearer_token.as_ref() {
            request = request.bearer_auth(token);
        }

        let response = request.send().await.map_err(|error| {
            PulsarSchemaRequestError::Retryable(
                anyhow::Error::new(error)
                    .context(format!("failed to fetch Pulsar schema from {url}")),
            )
        })?;
        let retryable_status = response.status() == StatusCode::TOO_MANY_REQUESTS
            || response.status().is_server_error();
        let response = response.error_for_status().map_err(|error| {
            let error = anyhow::Error::new(error)
                .context(format!("Pulsar schema request failed for {url}"));
            if retryable_status {
                PulsarSchemaRequestError::Retryable(error)
            } else {
                PulsarSchemaRequestError::Permanent(error)
            }
        })?;
        response.json().await.map_err(|error| {
            PulsarSchemaRequestError::Permanent(
                anyhow::Error::new(error)
                    .context(format!("failed to parse Pulsar schema response from {url}")),
            )
        })
    }

    pub async fn get_schema(
        &self,
        topic: &str,
        version: Option<i64>,
    ) -> ConnectorResult<PulsarSchemaInfo> {
        let url = self.build_schema_url(topic, version)?;
        let retry_strategy = exponential_backoff(
            Duration::from_millis(DEFAULT_RETRY_DELAY_MS),
            2,
            DEFAULT_MAX_RETRY_DELAY,
        )
        .take(DEFAULT_MAX_RETRIES)
        .map(jitter);
        RetryIf::spawn(
            retry_strategy,
            || self.request_schema(&url),
            |error: &PulsarSchemaRequestError| {
                let retryable = error.is_retryable();
                if retryable {
                    tracing::debug!(error = %error.as_inner(), "retrying Pulsar schema request");
                }
                retryable
            },
        )
        .await
        .map_err(|error| error.into_inner().into())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    #[cfg(not(madsim))]
    use std::io::{Read, Write};
    #[cfg(not(madsim))]
    use std::net::TcpListener;
    #[cfg(not(madsim))]
    use std::sync::mpsc;
    #[cfg(not(madsim))]
    use std::thread;

    use super::*;

    fn config(admin_url: String, token: Option<&str>) -> PulsarSchemaClientConfig {
        let mut options = BTreeMap::from([(PULSAR_SCHEMA_URL_KEY.to_owned(), admin_url)]);
        if let Some(token) = token {
            options.insert(PULSAR_SCHEMA_AUTH_TOKEN_KEY.to_owned(), token.to_owned());
        }
        PulsarSchemaClientConfig::from_options(&options)
            .unwrap()
            .unwrap()
    }

    fn client() -> PulsarSchemaClient {
        PulsarSchemaClient::new(config(
            "http://localhost:8080".to_owned(),
            Some("test-token"),
        ))
        .unwrap()
    }

    #[test]
    fn schema_url_from_full_topic() {
        let client = client();
        assert_eq!(
            client
                .build_schema_url("persistent://tenant/ns/events", None)
                .unwrap()
                .as_str(),
            "http://localhost:8080/admin/v2/schemas/tenant/ns/events/schema"
        );
        assert_eq!(
            client
                .build_schema_url("persistent://tenant/ns/events", Some(42))
                .unwrap()
                .as_str(),
            "http://localhost:8080/admin/v2/schemas/tenant/ns/events/schema/42"
        );
    }

    #[test]
    fn schema_url_from_short_partitioned_and_escaped_topics() {
        let client = client();
        assert_eq!(
            client.build_schema_url("events", None).unwrap().as_str(),
            "http://localhost:8080/admin/v2/schemas/public/default/events/schema"
        );
        assert_eq!(
            client
                .build_schema_url("persistent://tenant/ns/events-partition-1", None)
                .unwrap()
                .as_str(),
            "http://localhost:8080/admin/v2/schemas/tenant/ns/events/schema"
        );
        assert_eq!(
            client
                .build_schema_url("persistent://tenant/ns/events?region=us", None)
                .unwrap()
                .as_str(),
            "http://localhost:8080/admin/v2/schemas/tenant/ns/events%3Fregion=us/schema"
        );
    }

    #[test]
    fn config_is_optional_and_requires_url_for_token() {
        assert!(
            PulsarSchemaClientConfig::from_options(&BTreeMap::<String, String>::new())
                .unwrap()
                .is_none()
        );
        let options =
            BTreeMap::from([(PULSAR_SCHEMA_AUTH_TOKEN_KEY.to_owned(), "token".to_owned())]);
        assert!(PulsarSchemaClientConfig::from_options(&options).is_err());
    }

    #[test]
    fn config_debug_redacts_token() {
        let config = config("http://localhost:8080".to_owned(), Some("secret-token"));
        let debug = format!("{config:?}");
        assert!(!debug.contains("secret-token"));
        assert!(debug.contains("[REDACTED]"));
    }

    #[cfg(not(madsim))]
    fn spawn_http_server(
        responses: Vec<String>,
    ) -> (String, mpsc::Receiver<String>, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let (request_tx, request_rx) = mpsc::channel();
        let handle = thread::spawn(move || {
            for response in responses {
                let (mut stream, _) = listener.accept().unwrap();
                let mut request = Vec::new();
                loop {
                    let mut buffer = [0; 1024];
                    let read = stream.read(&mut buffer).unwrap();
                    assert_ne!(read, 0);
                    request.extend_from_slice(&buffer[..read]);
                    if request.windows(4).any(|window| window == b"\r\n\r\n") {
                        break;
                    }
                }
                request_tx
                    .send(String::from_utf8(request).unwrap())
                    .unwrap();
                stream.write_all(response.as_bytes()).unwrap();
            }
        });
        (format!("http://{addr}"), request_rx, handle)
    }

    #[cfg(not(madsim))]
    fn response(status: &str, body: &str) -> String {
        format!(
            "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn admin_api_redirect_is_followed_and_token_is_sent() {
        let body = r#"{"version":1,"type":"AVRO","data":"{}"}"#;
        let redirect = "HTTP/1.1 307 Temporary Redirect\r\nLocation: /schema\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_owned();
        let (admin_url, requests, handle) =
            spawn_http_server(vec![redirect, response("200 OK", body)]);
        let client = PulsarSchemaClient::new(config(admin_url, Some("test-token"))).unwrap();

        let schema = client.get_schema("tenant/ns/events", None).await.unwrap();
        assert_eq!(schema.version, 1);
        requests.recv().unwrap();
        let redirected_request = requests.recv().unwrap().to_ascii_lowercase();
        assert!(redirected_request.starts_with("get /schema "));
        assert!(redirected_request.contains("authorization: bearer test-token"));
        handle.join().unwrap();
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn server_error_is_retried() {
        let body = r#"{"version":2,"type":"AVRO","data":"{}"}"#;
        let (admin_url, requests, handle) = spawn_http_server(vec![
            response("503 Service Unavailable", ""),
            response("200 OK", body),
        ]);
        let client = PulsarSchemaClient::new(config(admin_url, None)).unwrap();

        let schema = client.get_schema("tenant/ns/events", None).await.unwrap();
        assert_eq!(schema.version, 2);
        requests.recv().unwrap();
        requests.recv().unwrap();
        assert!(requests.try_recv().is_err());
        handle.join().unwrap();
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn client_error_and_malformed_body_are_not_retried() {
        for response in [response("404 Not Found", ""), response("200 OK", "{")] {
            let (admin_url, requests, handle) = spawn_http_server(vec![response]);
            let client = PulsarSchemaClient::new(config(admin_url, None)).unwrap();

            client
                .get_schema("tenant/ns/events", None)
                .await
                .unwrap_err();
            requests.recv().unwrap();
            assert!(requests.try_recv().is_err());
            handle.join().unwrap();
        }
    }
}
