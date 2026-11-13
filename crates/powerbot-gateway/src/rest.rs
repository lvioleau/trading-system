//! Typed REST client for PowerBot's order-entry API.
//!
//! The gateway's write path to PowerBot: a thin async wrapper over `reqwest`
//! around the endpoints in `api/openapi.yaml`, authenticated by the `api_key`
//! header on every request (see docs/design.md, "The PowerBot API"). rustls
//! throughout, matching the websocket transport — no OpenSSL.
//!
//! Only the order-entry surface the executor needs is here; it is deliberately
//! hand-written against the generated `powerbot_models` types rather than using
//! the generator's HTTP client (the REST surface is small).

use powerbot_models::models::{
    ContractItem, ErrorResponse, InternalTrade, MarketStatus, OrderEntry, OwnOrder, Trade,
};
use reqwest::StatusCode;
use serde::de::DeserializeOwned;
use url::Url;

/// A REST call to PowerBot went wrong.
#[derive(Debug, thiserror::Error)]
pub enum RestError {
    /// The base URL could not be joined with the request path.
    #[error("invalid PowerBot base URL: {0}")]
    Url(#[from] url::ParseError),
    /// Transport/connection failure, or a body that failed to (de)serialize.
    #[error("PowerBot request failed: {0}")]
    Transport(#[from] reqwest::Error),
    /// PowerBot answered with a non-success status. `message` is the body's
    /// `message` field when present, else the raw body.
    #[error("PowerBot returned {status}: {message}")]
    Api { status: StatusCode, message: String },
    /// `addOrders` returned an empty array — PowerBot accepted the request but
    /// reported no order back, which should not happen for a single order.
    #[error("PowerBot returned no order for the request")]
    EmptyResponse,
    /// The configured API key can't be sent as an HTTP header value (e.g. a
    /// stray newline from the injecting environment).
    #[error("api_key is not a valid header value")]
    InvalidApiKey,
}

/// The `outcome` tag for the `powerbot.order.rest_latency_ms` histogram: a placed order
/// (`success`), a PowerBot 4xx rejecting the order (`rejected`), or any other failure —
/// transport, 5xx, empty response (`error`).
fn order_outcome(result: &Result<OwnOrder, RestError>) -> &'static str {
    match result {
        Ok(_) => "success",
        Err(RestError::Api { status, .. }) if status.is_client_error() => "rejected",
        Err(_) => "error",
    }
}

/// Async client for PowerBot's REST API, holding a base URL and a `reqwest`
/// client preloaded with the `api_key` header. Cheap to clone (the inner
/// `reqwest::Client` is an `Arc`), so share one across the gRPC service.
#[derive(Clone)]
pub struct RestClient {
    http: reqwest::Client,
    /// e.g. `https://<tenant>.powerbot.volue.com/<tenant>/` (trailing slash so
    /// path segments join onto it rather than replacing it).
    base_url: Url,
}

impl RestClient {
    /// Build a client for `base_url` (e.g.
    /// `https://<tenant>.powerbot.volue.com/<tenant>`), sending `api_key` on every
    /// request.
    pub fn new(base_url: &str, api_key: &str) -> Result<Self, RestError> {
        // Ensure a trailing slash: `Url::join` treats the last segment as a
        // file and replaces it otherwise, which would drop `/<tenant>`.
        let base = if base_url.ends_with('/') {
            base_url.to_string()
        } else {
            format!("{base_url}/")
        };
        let base_url = Url::parse(&base)?;

        let mut headers = reqwest::header::HeaderMap::new();
        let mut key = reqwest::header::HeaderValue::from_str(api_key)
            .map_err(|_| RestError::InvalidApiKey)?;
        // The key is a secret: keep it out of Debug output and logs.
        key.set_sensitive(true);
        headers.insert("api_key", key);

        let http = reqwest::Client::builder()
            .default_headers(headers)
            .build()?;

        Ok(Self { http, base_url })
    }

    /// `addOrders` — `POST /{exchange}/v2/api/orders/list`. Sends the single
    /// `order` (PowerBot's endpoint takes an array; we send a one-element list)
    /// and returns the `OwnOrder` PowerBot echoes back. Note the `/list` suffix:
    /// the base `/orders` path only serves GET/DELETE/PUT — POST placing orders
    /// lives on `/orders/list` (openapi.yaml: `addOrders`).
    ///
    /// Note the PowerBot spec's own caveat: this response is advisory. The
    /// `ownorders` feed, not this return value, is the source of truth for the
    /// resulting order state.
    pub async fn add_order(
        &self,
        exchange: &str,
        order: OrderEntry,
    ) -> Result<OwnOrder, RestError> {
        // exchange is a single path segment; join relative to the base's path. A join failure is
        // a config error, not a round trip, so it stays outside the latency timer.
        let url = self
            .base_url
            .join(&format!("{exchange}/v2/api/orders/list"))?;

        tracing::info!(exchange, ?order, "PowerBot REST addOrder");
        // Time the whole round trip for the `powerbot.order.rest_latency_ms` histogram; the
        // inner block owns every exit path so latency is recorded whatever the outcome.
        let started = std::time::Instant::now();
        let result = async {
            let response = self.http.post(url).json(&[order]).send().await?;

            let status = response.status();
            tracing::info!(exchange, %status, "PowerBot REST addOrder response");
            if !status.is_success() {
                let body = response.text().await.unwrap_or_default();
                // Bodies are `{ "message": "..." }` per the spec; fall back to the
                // raw body if it doesn't parse.
                let message = serde_json::from_str::<ErrorResponse>(&body)
                    .map(|e| e.message)
                    .unwrap_or(body);
                return Err(RestError::Api { status, message });
            }

            let orders: Vec<OwnOrder> = response.json().await?;
            orders.into_iter().next().ok_or(RestError::EmptyResponse)
        }
        .await;
        crate::metrics::order_action_latency(order_outcome(&result), started.elapsed());
        result
    }

    /// `getStatus` — `GET /{exchange}/v2/api/market`: the status of the market
    /// and the PowerBot server plus configuration information (delivery areas,
    /// exchange limits, quota...). The spec's guidance is to check the status is
    /// `OK` before any trading activity; the market poll fetches this on an
    /// interval to log and export it (see [`market`](crate::market)).
    pub async fn get_market_status(&self, exchange: &str) -> Result<MarketStatus, RestError> {
        let url = self.base_url.join(&format!("{exchange}/v2/api/market"))?;
        let response = self.http.get(url).send().await?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            let message = serde_json::from_str::<ErrorResponse>(&body)
                .map(|e| e.message)
                .unwrap_or(body);
            tracing::warn!(exchange, %status, message = %message, "PowerBot REST getStatus failed");
            return Err(RestError::Api { status, message });
        }
        Ok(response.json().await?)
    }

    /// `findContracts` — `GET /{exchange}/v2/api/contracts`, the contracts
    /// whose delivery starts at or after `delivery_from` (RFC3339), scoped to
    /// `delivery_area_eic`. Seeds and re-sweeps the contract store the order gate
    /// checks (see [`contracts`](crate::contracts)). Unpaged: the endpoint
    /// takes no offset/limit, and a delivery-area day is a few hundred
    /// contracts at most.
    pub async fn find_contracts(
        &self,
        exchange: &str,
        delivery_area_eic: &str,
        delivery_from: &str,
    ) -> Result<Vec<ContractItem>, RestError> {
        let url = self
            .base_url
            .join(&format!("{exchange}/v2/api/contracts"))?;
        let query = [
            ("delivery_areas", delivery_area_eic),
            ("delivery_from", delivery_from),
        ];
        tracing::info!(exchange, ?query, "PowerBot REST findContracts");
        let response = self.http.get(url).query(&query).send().await?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            let message = serde_json::from_str::<ErrorResponse>(&body)
                .map(|e| e.message)
                .unwrap_or(body);
            tracing::warn!(exchange, %status, message = %message, "PowerBot REST findContracts failed");
            return Err(RestError::Api { status, message });
        }
        Ok(response.json().await?)
    }

    /// `getOwnOrdersHistory` — `GET /{exchange}/v2/api/orders/history`, every
    /// order revision with an `api_timestamp` in `[from_api_timestamp,
    /// to_api_timestamp)` (RFC3339). The snapshot that seeds/reconciles the order
    /// store: unlike `getOwnOrders` (active-only, whose `active_only` filter is
    /// deprecated and returns nothing here) this gives the full revision history,
    /// which matches the store's never-regress-by-`revision_no` fold. Not
    /// portfolio-scoped at the API — the caller filters to its portfolio.
    pub async fn get_own_orders_history(
        &self,
        exchange: &str,
        from_api_timestamp: &str,
        to_api_timestamp: &str,
    ) -> Result<Vec<OwnOrder>, RestError> {
        self.get_paged(
            &format!("{exchange}/v2/api/orders/history"),
            vec![
                ("from_api_timestamp", from_api_timestamp.to_string()),
                ("to_api_timestamp", to_api_timestamp.to_string()),
            ],
        )
        .await
    }

    /// `getTrades` — `GET /{exchange}/v2/api/trades`, external trades executed at
    /// or after `from_execution_time` (RFC3339, inclusive) for `portfolio_id`. The
    /// snapshot that seeds/reconciles the external-trade store. Filters on
    /// execution time, not `api_timestamp` (which lags and can be restated), per
    /// the spec. (No `active_only` — deprecated.)
    pub async fn get_trades(
        &self,
        exchange: &str,
        portfolio_id: &str,
        from_execution_time: &str,
    ) -> Result<Vec<Trade>, RestError> {
        self.get_paged(
            &format!("{exchange}/v2/api/trades"),
            vec![
                ("portfolio_id", portfolio_id.to_string()),
                ("from_execution_time", from_execution_time.to_string()),
            ],
        )
        .await
    }

    /// `getInternalTrades` — `GET /{exchange}/v2/api/internal-trades`, internal
    /// trades executed at or after `from_execution_time` for `portfolio_id`. The
    /// snapshot that seeds/reconciles the internal-trade store. (Without a
    /// timeframe filter PowerBot returns only the last 48 h, so pass one.)
    pub async fn get_internal_trades(
        &self,
        exchange: &str,
        portfolio_id: &str,
        from_execution_time: &str,
    ) -> Result<Vec<InternalTrade>, RestError> {
        self.get_paged(
            &format!("{exchange}/v2/api/internal-trades"),
            vec![
                ("portfolio_id", portfolio_id.to_string()),
                ("from_execution_time", from_execution_time.to_string()),
            ],
        )
        .await
    }

    /// GET a paged PowerBot collection, following `offset` until the whole set is
    /// read. `base_query` carries the endpoint-specific filters; `offset`/`limit`
    /// are added per page. Termination is driven by the `X-Total-Count` header
    /// when present (so a server-side page cap below `PAGE` doesn't stop us
    /// early), falling back to a short final page otherwise.
    async fn get_paged<T: DeserializeOwned>(
        &self,
        path: &str,
        base_query: Vec<(&str, String)>,
    ) -> Result<Vec<T>, RestError> {
        const PAGE: usize = 500;
        let url = self.base_url.join(path)?;
        let mut all: Vec<T> = Vec::new();
        let mut offset = 0usize;
        loop {
            let mut query = base_query.clone();
            query.push(("offset", offset.to_string()));
            query.push(("limit", PAGE.to_string()));

            tracing::info!(path, ?query, "PowerBot REST GET");
            let response = self.http.get(url.clone()).query(&query).send().await?;
            let status = response.status();
            if !status.is_success() {
                let body = response.text().await.unwrap_or_default();
                let message = serde_json::from_str::<ErrorResponse>(&body)
                    .map(|e| e.message)
                    .unwrap_or(body);
                tracing::warn!(path, %status, message = %message, "PowerBot REST GET failed");
                return Err(RestError::Api { status, message });
            }

            let total = response
                .headers()
                .get("X-Total-Count")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<usize>().ok());

            let page: Vec<T> = response.json().await?;
            let got = page.len();
            tracing::info!(path, %status, got, total = ?total, "PowerBot REST GET ok");
            all.extend(page);
            offset += got;

            let done = got == 0 || total.map_or(got < PAGE, |t| all.len() >= t);
            if done {
                break;
            }
        }
        Ok(all)
    }
}
