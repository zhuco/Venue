use super::{
    binance::{PrivateError, PrivateRest, private_http_error, private_response_text, signed_query},
    binance_clock,
};

// Signed reads are idempotent. The local CONNECT proxy can drop a newly opened tunnel while
// several independent residents refresh private facts, so give reads a bounded recovery window.
// Mutations deliberately remain single-attempt below.
const SIGNED_GET_TRANSPORT_ATTEMPTS: u8 = 6;
const SIGNED_GET_RETRY_BASE_MS: u64 = 100;

impl PrivateRest {
    pub(super) fn signed_get(
        &self,
        path: &str,
        parameters: Vec<(&str, String)>,
    ) -> Result<String, PrivateError> {
        for attempt in 0..SIGNED_GET_TRANSPORT_ATTEMPTS {
            match self.signed_request(reqwest::Method::GET, path, parameters.clone(), 60_000) {
                Err(PrivateError::Http) if attempt + 1 < SIGNED_GET_TRANSPORT_ATTEMPTS => {
                    std::thread::sleep(std::time::Duration::from_millis(
                        SIGNED_GET_RETRY_BASE_MS * u64::from(attempt + 1),
                    ));
                }
                result => return result,
            }
        }
        Err(PrivateError::Http)
    }

    pub(super) fn signed_post(
        &self,
        path: &str,
        parameters: Vec<(&str, String)>,
    ) -> Result<String, PrivateError> {
        self.signed_request(reqwest::Method::POST, path, parameters, 5_000)
    }

    pub(super) fn signed_form_post(
        &self,
        path: &str,
        parameters: Vec<(&str, String)>,
    ) -> Result<String, PrivateError> {
        self.signed_form_post_with_retry(path, parameters, true)
    }

    fn signed_form_post_with_retry(
        &self,
        path: &str,
        mut parameters: Vec<(&str, String)>,
        retry_timestamp: bool,
    ) -> Result<String, PrivateError> {
        parameters.push(("recvWindow", "5000".to_owned()));
        parameters.push(("timestamp", self.clock.now_ms()?.to_string()));
        let body = signed_query(&self.credentials.secret, &parameters)?;
        let response = self
            .client
            .post(format!("{}{}", self.base_url, path))
            .header("X-MBX-APIKEY", &self.credentials.api_key)
            .header(
                reqwest::header::CONTENT_TYPE,
                "application/x-www-form-urlencoded",
            )
            .body(body)
            .send()
            .map_err(private_http_error)?;
        let first = private_response_text(response);
        if !retry_timestamp
            || !matches!(
                first,
                Err(PrivateError::Rejected {
                    api_code: Some(-1021),
                    ..
                })
            )
        {
            return first;
        }
        binance_clock::synchronize(&self.client, &self.base_url, &self.clock)?;
        self.signed_form_post_with_retry(path, parameters[..parameters.len() - 2].to_vec(), false)
    }

    pub(super) fn signed_delete(
        &self,
        path: &str,
        parameters: Vec<(&str, String)>,
    ) -> Result<String, PrivateError> {
        self.signed_request(reqwest::Method::DELETE, path, parameters, 5_000)
    }

    fn signed_request(
        &self,
        method: reqwest::Method,
        path: &str,
        parameters: Vec<(&str, String)>,
        recv_window_ms: u32,
    ) -> Result<String, PrivateError> {
        self.signed_request_with_retry(method, path, parameters, recv_window_ms, true)
    }

    fn signed_request_with_retry(
        &self,
        method: reqwest::Method,
        path: &str,
        mut parameters: Vec<(&str, String)>,
        recv_window_ms: u32,
        retry_timestamp: bool,
    ) -> Result<String, PrivateError> {
        parameters.push(("recvWindow", recv_window_ms.to_string()));
        parameters.push(("timestamp", self.clock.now_ms()?.to_string()));
        let query = signed_query(&self.credentials.secret, &parameters)?;
        let response = self
            .client
            .request(method.clone(), format!("{}{}?{query}", self.base_url, path))
            .header("X-MBX-APIKEY", &self.credentials.api_key)
            .send()
            .map_err(private_http_error)?;
        let first = private_response_text(response);
        if !retry_timestamp
            || !matches!(
                first,
                Err(PrivateError::Rejected {
                    api_code: Some(-1021),
                    ..
                })
            )
        {
            return first;
        }
        binance_clock::synchronize(&self.client, &self.base_url, &self.clock)?;
        self.signed_request_with_retry(
            method,
            path,
            parameters[..parameters.len() - 2].to_vec(),
            recv_window_ms,
            false,
        )
    }
}
