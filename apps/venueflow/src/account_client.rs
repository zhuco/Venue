use crossbeam_channel::{Receiver, Sender, bounded};
use futures_util::StreamExt;
use serde::{Serialize, de::DeserializeOwned};
use venue_control_protocol::accounts::*;
use zeroize::Zeroizing;

pub enum AccountAction {
    Login(LoginRequest),
    Register(LoginRequest),
    Refresh,
    Logout,
    Bind(BindCredentialRequest),
    Verify(String),
    Select(String),
    Delete(DeleteCredentialRequest),
}

pub enum AccountResult {
    Session(SessionResponse, AccountOverview),
    Overview(AccountOverview),
    LoggedOut,
}

pub struct AccountClient {
    sender: Sender<Result<AccountResult, AccountErrorCode>>,
    receiver: Receiver<Result<AccountResult, AccountErrorCode>>,
}

impl Default for AccountClient {
    fn default() -> Self {
        let (sender, receiver) = bounded(2);
        Self { sender, receiver }
    }
}

impl AccountClient {
    #[cfg(test)]
    pub(crate) fn test_sender(&self) -> Sender<Result<AccountResult, AccountErrorCode>> {
        self.sender.clone()
    }
    pub fn drain(&self) -> impl Iterator<Item = Result<AccountResult, AccountErrorCode>> + '_ {
        self.receiver.try_iter()
    }
    pub fn submit(
        &self,
        endpoint: String,
        token: Option<SecretValue>,
        action: AccountAction,
        context: egui::Context,
    ) {
        let sender = self.sender.clone();
        #[cfg(not(target_arch = "wasm32"))]
        {
            let failure_sender = sender.clone();
            let failure_context = context.clone();
            let spawned = std::thread::Builder::new()
                .name("venueflow-account-request".into())
                .spawn(move || {
                    let result = match tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                    {
                        Ok(runtime) => runtime.block_on(execute(endpoint, token, action)),
                        Err(_) => Err(AccountErrorCode::Unavailable),
                    };
                    let _ = sender.send(result);
                    context.request_repaint();
                });
            if spawned.is_err() {
                let _ = failure_sender.send(Err(AccountErrorCode::Unavailable));
                failure_context.request_repaint();
            }
        }
        #[cfg(target_arch = "wasm32")]
        wasm_bindgen_futures::spawn_local(async move {
            let result = execute(endpoint, token, action).await;
            let _ = sender.send(result);
            context.request_repaint();
        });
    }
}

pub fn safe_endpoint(endpoint: &str) -> bool {
    if endpoint.is_empty() {
        #[cfg(target_arch = "wasm32")]
        return web_sys::window()
            .and_then(|w| w.location().origin().ok())
            .is_some_and(|origin| !origin.is_empty() && safe_endpoint(&origin));
        #[cfg(not(target_arch = "wasm32"))]
        return false;
    }
    let Ok(url) = reqwest::Url::parse(endpoint) else {
        return false;
    };
    if !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return false;
    }
    url.scheme() == "https"
        || (url.scheme() == "http"
            && url.host_str().is_some_and(|host| {
                host == "localhost"
                    || host
                        .trim_matches(['[', ']'])
                        .parse::<std::net::IpAddr>()
                        .is_ok_and(|ip| ip.is_loopback())
            }))
}

pub fn authorization_headers(
    token: Option<&SecretValue>,
) -> Result<reqwest::header::HeaderMap, AccountErrorCode> {
    let mut headers = reqwest::header::HeaderMap::new();
    if let Some(token) = token {
        let text = Zeroizing::new(format!("Bearer {}", token.expose()));
        let mut value = reqwest::header::HeaderValue::from_str(&text)
            .map_err(|_| AccountErrorCode::InvalidInput)?;
        value.set_sensitive(true);
        headers.insert(reqwest::header::AUTHORIZATION, value);
    }
    Ok(headers)
}

async fn execute(
    endpoint: String,
    token: Option<SecretValue>,
    action: AccountAction,
) -> Result<AccountResult, AccountErrorCode> {
    if !safe_endpoint(&endpoint) {
        return Err(AccountErrorCode::InvalidInput);
    }
    let builder = reqwest::Client::builder();
    #[cfg(not(target_arch = "wasm32"))]
    let builder = builder
        .connect_timeout(std::time::Duration::from_secs(5))
        .timeout(std::time::Duration::from_secs(35))
        .redirect(reqwest::redirect::Policy::none());
    let client = builder.build().map_err(|_| AccountErrorCode::Unavailable)?;
    let auth = authorization_headers(token.as_ref())?;
    let endpoint = endpoint.trim_end_matches('/');
    match action {
        AccountAction::Login(request) => login(&client, endpoint, request, false).await,
        AccountAction::Register(request) => login(&client, endpoint, request, true).await,
        AccountAction::Logout => {
            let _: () = post(&client, endpoint, LOGOUT_PATH, &auth, &()).await?;
            Ok(AccountResult::LoggedOut)
        }
        AccountAction::Refresh => Ok(AccountResult::Overview(
            get(&client, endpoint, SESSION_PATH, &auth).await?,
        )),
        AccountAction::Bind(request) => {
            let _: CredentialSummary =
                post(&client, endpoint, CREDENTIALS_PATH, &auth, &request).await?;
            Ok(AccountResult::Overview(
                get(&client, endpoint, SESSION_PATH, &auth).await?,
            ))
        }
        AccountAction::Verify(id) => {
            let _: CredentialSummary = post(
                &client,
                endpoint,
                VERIFY_PATH,
                &auth,
                &CredentialRequest { credential_id: id },
            )
            .await?;
            Ok(AccountResult::Overview(
                get(&client, endpoint, SESSION_PATH, &auth).await?,
            ))
        }
        AccountAction::Select(id) => Ok(AccountResult::Overview(
            post(
                &client,
                endpoint,
                SELECT_PATH,
                &auth,
                &CredentialRequest { credential_id: id },
            )
            .await?,
        )),
        AccountAction::Delete(request) => Ok(AccountResult::Overview(
            post(&client, endpoint, DELETE_PATH, &auth, &request).await?,
        )),
    }
}

async fn login(
    client: &reqwest::Client,
    endpoint: &str,
    request: LoginRequest,
    register: bool,
) -> Result<AccountResult, AccountErrorCode> {
    let route = if register { REGISTER_PATH } else { LOGIN_PATH };
    let session: SessionResponse = post(
        client,
        endpoint,
        route,
        &reqwest::header::HeaderMap::new(),
        &request,
    )
    .await?;
    let headers = authorization_headers(Some(&session.token))?;
    let overview = get(client, endpoint, SESSION_PATH, &headers).await?;
    Ok(AccountResult::Session(session, overview))
}

async fn get<T: DeserializeOwned>(
    client: &reqwest::Client,
    endpoint: &str,
    path: &str,
    headers: &reqwest::header::HeaderMap,
) -> Result<T, AccountErrorCode> {
    decode(
        client
            .get(format!("{endpoint}{path}"))
            .timeout(std::time::Duration::from_secs(35))
            .headers(headers.clone())
            .send()
            .await
            .map_err(|_| AccountErrorCode::Unavailable)?,
    )
    .await
}

async fn post<T: DeserializeOwned>(
    client: &reqwest::Client,
    endpoint: &str,
    path: &str,
    headers: &reqwest::header::HeaderMap,
    value: &impl Serialize,
) -> Result<T, AccountErrorCode> {
    let encoded =
        Zeroizing::new(serde_json::to_vec(value).map_err(|_| AccountErrorCode::InvalidInput)?);
    decode(
        client
            .post(format!("{endpoint}{path}"))
            .timeout(std::time::Duration::from_secs(35))
            .headers(headers.clone())
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(encoded.to_vec())
            .send()
            .await
            .map_err(|_| AccountErrorCode::Unavailable)?,
    )
    .await
}

async fn decode<T: DeserializeOwned>(response: reqwest::Response) -> Result<T, AccountErrorCode> {
    let status = response.status();
    let mut stream = response.bytes_stream();
    let mut bytes = Zeroizing::new(Vec::new());
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| AccountErrorCode::Unavailable)?;
        if bytes.len() + chunk.len() > 128 * 1024 {
            return Err(AccountErrorCode::Unavailable);
        }
        bytes.extend_from_slice(&chunk);
    }
    if !status.is_success() {
        return Err(serde_json::from_slice::<AccountErrorResponse>(&bytes)
            .map(|e| e.code)
            .unwrap_or(if status.as_u16() == 401 {
                AccountErrorCode::Unauthorized
            } else {
                AccountErrorCode::Unavailable
            }));
    }
    serde_json::from_slice(&bytes).map_err(|_| AccountErrorCode::Unavailable)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn credentials_require_local_http_or_https_without_url_secrets() {
        assert!(safe_endpoint("http://127.0.0.1:8080"));
        assert!(safe_endpoint("https://control.example.com"));
        assert!(!safe_endpoint("http://control.example.com"));
        assert!(!safe_endpoint("https://user:password@control.example.com"));
        assert!(!safe_endpoint("https://control.example.com?token=secret"));
    }
}
