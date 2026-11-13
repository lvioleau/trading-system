//! The topics the gateway subscribes to and the decode step from a STOMP
//! `MESSAGE` to a typed [`PowerbotEvent`]: the subscription id tells us the
//! topic, the topic tells us the expected `messageClass`, and the body is
//! `serde_json`ed into the generated model (docs/design.md, "Message model").

use powerbot_models as models;

use crate::stomp::StompMessage;

use super::DriverConfig;

/// One decoded PowerBot push message.
///
/// `MarketStatus` is boxed: the `MarketStatus` model is ~700 bytes, and an
/// unboxed variant would make every event that large (these flow through the
/// ingest channel eventually).
#[derive(Debug, Clone)]
pub enum PowerbotEvent {
    MarketStatus(Box<models::MarketStatusChangedEvent>),
    ContractChanged(models::ContractChangedEvent),
    OwnOrders(models::OwnOrderChanges),
    OwnTrades(models::OwnTradeChanges),
}

impl PowerbotEvent {
    /// A short, stable name for the event kind, for logs and (later) metrics.
    pub fn name(&self) -> &'static str {
        match self {
            PowerbotEvent::MarketStatus(_) => "market_status",
            PowerbotEvent::ContractChanged(_) => "contract_changed",
            PowerbotEvent::OwnOrders(_) => "own_orders",
            PowerbotEvent::OwnTrades(_) => "own_trades",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) enum Topic {
    MarketStatus,
    Contracts,
    OwnOrders,
    OwnTrades,
}

/// One STOMP subscription to open: a topic instance. `MarketStatus` and
/// `Contracts` are one subscription each; `OwnOrders` / `OwnTrades` are one per
/// configured portfolio (PowerBot's push topics are per portfolio), so a session
/// opens `2 + 2 × portfolios` of these. Sequence continuity is tracked per
/// subscription — each PowerBot topic runs its own counter.
pub(super) struct Subscription {
    pub id: String,
    pub destination: String,
}

/// The subscriptions a session opens, filled from config. Hard-coded destination
/// templates for now; resolving them via `getSubscriptionTopics` over REST is
/// planned with the REST client (docs/design.md, "STOMP session").
pub(super) fn subscriptions(config: &DriverConfig) -> Vec<Subscription> {
    let exchange = &config.exchange;
    let mut subs = vec![
        Subscription {
            id: Topic::MarketStatus.subscription_id_base().to_string(),
            destination: format!("/topic/marketstatus-{exchange}"),
        },
        Subscription {
            id: Topic::Contracts.subscription_id_base().to_string(),
            destination: format!("/topic/contracts-{exchange}.{}", config.delivery_area_eic),
        },
    ];
    for portfolio in &config.portfolio_ids {
        subs.push(Subscription {
            // The portfolio rides in the subscription id so a MESSAGE routes to
            // its topic by prefix and sequence tracking keys per subscription.
            id: format!("{}.{portfolio}", Topic::OwnOrders.subscription_id_base()),
            destination: format!("/topic/ownorders-{exchange}.{portfolio}"),
        });
        subs.push(Subscription {
            id: format!("{}.{portfolio}", Topic::OwnTrades.subscription_id_base()),
            destination: format!("/topic/owntrades-{exchange}.{portfolio}"),
        });
    }
    subs
}

impl Topic {
    pub(super) const ALL: [Topic; 4] = [
        Topic::MarketStatus,
        Topic::Contracts,
        Topic::OwnOrders,
        Topic::OwnTrades,
    ];

    /// The STOMP subscription-id prefix for this topic. Per-portfolio topics
    /// (own orders/trades) append `.{portfolio}`; the id echoed on every MESSAGE
    /// is how a received message routes back to its topic.
    pub(super) fn subscription_id_base(self) -> &'static str {
        match self {
            Topic::MarketStatus => "sub-marketstatus",
            Topic::Contracts => "sub-contracts",
            Topic::OwnOrders => "sub-ownorders",
            Topic::OwnTrades => "sub-owntrades",
        }
    }

    /// Route a MESSAGE's subscription id back to its topic: the exact base, or
    /// the base plus a `.{portfolio}` suffix for the per-portfolio topics.
    pub(super) fn from_subscription_id(id: &str) -> Option<Topic> {
        Topic::ALL.into_iter().find(|t| {
            id.strip_prefix(t.subscription_id_base())
                .is_some_and(|rest| rest.is_empty() || rest.starts_with('.'))
        })
    }

    /// A short, stable name for the topic — the `topic` tag on the per-topic
    /// metrics (frames/bytes, sequence gaps). Matches the design's topic names.
    pub(super) fn label(self) -> &'static str {
        match self {
            Topic::MarketStatus => "marketstatus",
            Topic::Contracts => "contracts",
            Topic::OwnOrders => "ownorders",
            Topic::OwnTrades => "owntrades",
        }
    }

    /// The `messageClass` discriminator this topic's payloads carry.
    pub(super) fn message_class(self) -> &'static str {
        match self {
            Topic::MarketStatus => "powerbot.v2.model.MarketStatusChangedEvent",
            Topic::Contracts => "powerbot.v2.model.ContractChangedEvent",
            Topic::OwnOrders => "powerbot.v2.model.OwnOrderChanges",
            Topic::OwnTrades => "powerbot.v2.model.OwnTradeChanges",
        }
    }

    fn decode_body(self, body: &str) -> Result<PowerbotEvent, serde_json::Error> {
        Ok(match self {
            Topic::MarketStatus => {
                PowerbotEvent::MarketStatus(Box::new(serde_json::from_str(body)?))
            }
            Topic::Contracts => PowerbotEvent::ContractChanged(serde_json::from_str(body)?),
            Topic::OwnOrders => PowerbotEvent::OwnOrders(serde_json::from_str(body)?),
            Topic::OwnTrades => PowerbotEvent::OwnTrades(serde_json::from_str(body)?),
        })
    }
}

/// Decode one MESSAGE into an event, or `None` for anything that must not kill
/// the session: an unknown subscription id, a `messageClass` other than the
/// topic's (both mean our model of the server is behind — logged, never fatal,
/// per docs/design.md), or a body the generated struct rejects.
pub(super) fn decode(message: &StompMessage) -> Option<PowerbotEvent> {
    let Some(topic) = Topic::from_subscription_id(&message.subscription) else {
        tracing::warn!(
            subscription = %message.subscription,
            destination = %message.destination,
            "message on unknown subscription; dropped"
        );
        return None;
    };
    let message_class = serde_json::from_str::<MessageClassProbe>(&message.body)
        .ok()
        .and_then(|probe| probe.message_class);
    if message_class.as_deref() != Some(topic.message_class()) {
        tracing::warn!(
            subscription = %message.subscription,
            ?message_class,
            expected = topic.message_class(),
            "unexpected messageClass; dropped"
        );
        return None;
    }
    match topic.decode_body(&message.body) {
        Ok(event) => Some(event),
        Err(error) => {
            tracing::warn!(
                %error,
                message_class = topic.message_class(),
                body = %message.body,
                "failed to decode message body; dropped"
            );
            None
        }
    }
}

/// The `messageClass` discriminator every PowerBot push payload carries.
#[derive(serde::Deserialize)]
struct MessageClassProbe {
    #[serde(rename = "messageClass")]
    message_class: Option<String>,
}
