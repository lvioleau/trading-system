//! Runtime configuration, from env vars with `--flag value` overrides.

use eic::EicCode;

/// Where the tester dials and the fixed order parameters it entered orders with.
#[derive(Clone)]
pub struct Config {
    /// Gateway gRPC endpoint (order entry + execution feeds).
    pub gateway_addr: String,
    /// Connector stream-server gRPC endpoint (contracts + depth).
    pub connector_addr: String,
    /// Position Service gRPC endpoint (per-portfolio net positions).
    pub position_addr: String,
    /// Portfolio orders are entered on. Defaults to `exec-1`.
    pub portfolio_id: String,
    /// Portfolios to track positions for — one own-trades subscription each,
    /// netted per contract and shown as a column in the contract list.
    pub position_portfolios: Vec<String>,
    /// The delivery area — guards the depth feed, filters contracts, and its EIC
    /// goes on the order.
    pub delivery_area_eic: EicCode,
    /// Exchange path segment routed to by AddOrder (e.g. `nordpool`).
    pub exchange: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            gateway_addr: "http://127.0.0.1:50052".to_string(),
            connector_addr: "http://127.0.0.1:50051".to_string(),
            position_addr: "http://127.0.0.1:50053".to_string(),
            portfolio_id: "exec-1".to_string(),
            position_portfolios: vec![
                "strat-batt-1".to_string(),
                "strat-batt-2".to_string(),
                "exec-1".to_string(),
            ],
            // France/RTE — one EIC end to end: connector feeds and PowerBot orders.
            delivery_area_eic: EicCode::AREA_FR,
            exchange: "nordpool".to_string(),
        }
    }
}

impl Config {
    /// Build from env vars (`TESTER_*`) then apply `--flag value` overrides.
    pub fn load() -> anyhow::Result<Self> {
        let mut cfg = Config::default();
        if let Ok(v) = std::env::var("TESTER_GATEWAY_ADDR") {
            cfg.gateway_addr = v;
        }
        if let Ok(v) = std::env::var("TESTER_CONNECTOR_ADDR") {
            cfg.connector_addr = v;
        }
        if let Ok(v) = std::env::var("TESTER_POSITION_ADDR") {
            cfg.position_addr = v;
        }
        if let Ok(v) = std::env::var("TESTER_PORTFOLIO_ID") {
            cfg.portfolio_id = v;
        }
        if let Ok(v) = std::env::var("TESTER_POSITION_PORTFOLIOS") {
            cfg.position_portfolios = parse_portfolios(&v);
        }
        if let Ok(v) = std::env::var("TESTER_DELIVERY_AREA_EIC") {
            cfg.delivery_area_eic = parse_eic(&v)?;
        }
        if let Ok(v) = std::env::var("TESTER_EXCHANGE") {
            cfg.exchange = v;
        }

        let mut args = std::env::args().skip(1);
        while let Some(flag) = args.next() {
            let mut take = || {
                args.next()
                    .ok_or_else(|| anyhow::anyhow!("missing value for {flag}"))
            };
            match flag.as_str() {
                "--gateway" => cfg.gateway_addr = with_scheme(&take()?),
                "--connector" => cfg.connector_addr = with_scheme(&take()?),
                "--position-service" => cfg.position_addr = with_scheme(&take()?),
                "--portfolio" => cfg.portfolio_id = take()?,
                "--positions" => cfg.position_portfolios = parse_portfolios(&take()?),
                "--area" | "--area-eic" => cfg.delivery_area_eic = parse_eic(&take()?)?,
                "--exchange" => cfg.exchange = take()?,
                "-h" | "--help" => {
                    print_help();
                    std::process::exit(0);
                }
                other => anyhow::bail!("unknown argument: {other} (try --help)"),
            }
        }

        cfg.gateway_addr = with_scheme(&cfg.gateway_addr);
        cfg.connector_addr = with_scheme(&cfg.connector_addr);
        cfg.position_addr = with_scheme(&cfg.position_addr);
        Ok(cfg)
    }
}

/// Parse a comma-separated portfolio list, trimming whitespace and dropping
/// empty entries.
fn parse_portfolios(s: &str) -> Vec<String> {
    s.split(',')
        .map(str::trim)
        .filter(|p| !p.is_empty())
        .map(String::from)
        .collect()
}

/// tonic's `connect` needs a URI with a scheme; add `http://` if the user gave a
/// bare `host:port`.
fn with_scheme(addr: &str) -> String {
    if addr.contains("://") {
        addr.to_string()
    } else {
        format!("http://{addr}")
    }
}

fn print_help() {
    println!(
        "powerbot-tester — TUI for exercising the powerbot gateway\n\n\
         USAGE: powerbot-tester [--flag value ...]\n\n\
         FLAGS (env var in parens):\n  \
         --gateway ADDR    gateway gRPC endpoint      (TESTER_GATEWAY_ADDR)   [http://127.0.0.1:50052]\n  \
         --connector ADDR  connector gRPC endpoint    (TESTER_CONNECTOR_ADDR) [http://127.0.0.1:50051]\n  \
         --position-service ADDR  position svc endpoint (TESTER_POSITION_ADDR) [http://127.0.0.1:50053]\n  \
         --portfolio ID    portfolio for orders       (TESTER_PORTFOLIO_ID)   [exec-1]\n  \
         --positions LIST  position portfolios (csv)  (TESTER_POSITION_PORTFOLIOS) [strat-batt-1,strat-batt-2,exec-1]\n  \
         --area EIC        delivery-area EIC code     (TESTER_DELIVERY_AREA_EIC) [10YFR-RTE------C]\n  \
         --exchange NAME   AddOrder exchange segment  (TESTER_EXCHANGE)       [nordpool]"
    );
}

/// Resolve an EIC string from the environment or a flag; unknown is a startup error naming
/// the offending value.
fn parse_eic(value: &str) -> anyhow::Result<EicCode> {
    EicCode::from_eic(value)
        .ok_or_else(|| anyhow::anyhow!("{value:?} is not a known delivery-area EIC code"))
}
