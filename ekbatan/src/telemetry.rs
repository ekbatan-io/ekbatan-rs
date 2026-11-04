//! OpenTelemetry spans and metrics, under Java's instrumentation scopes, span names and attribute
//! names, so one trace and one dashboard read across every Ekbatan.
//!
//! Only OpenTelemetry's API is a dependency, as in Java's `ekbatan-core`: on its own it does
//! nothing, and spans and metrics go nowhere until the app installs an SDK
//! (`opentelemetry::global::set_tracer_provider` / `set_meter_provider`), exactly as Java's
//! `GlobalOpenTelemetry` stays a no-op until one is registered.
//!
//! Spans (scope `io.ekbatan.core`):
//!
//! - `ekbatan.action.execute`: `ekbatan.action.name`, `ekbatan.action.principal`,
//!   `ekbatan.action.outcome` (`success` / `error`), `ekbatan.action.retry.count`;
//! - `ekbatan.action.perform`, `ekbatan.action.persist` (`ekbatan.shard.cross_shard`, and on a
//!   partial commit `ekbatan.shard.partial_commit_failure`, `ekbatan.shard.committed_shards`,
//!   `ekbatan.shard.failed_shard`);
//! - `ekbatan.transaction`: `ekbatan.shard.group`, `ekbatan.shard.member`;
//! - `ekbatan.event.persist`: `ekbatan.action.name`, `ekbatan.event.count`;
//! - `ekbatan.repository`: `db.operation.name` (`BATCH_INSERT` / `BATCH_UPDATE`),
//!   `ekbatan.entity.type`, `ekbatan.batch.size`.
//!
//! A failed span has status `ERROR` and the error recorded as an exception event.
//!
//! Metrics (scope `io.ekbatan.events.localeventhandler`): `ekbatan.events.fanned_out`,
//! `ekbatan.events.notifications_created`, `ekbatan.events.handled` (`outcome`, `handler`),
//! `ekbatan.events.handler.duration` (`handler`, `outcome`), `ekbatan.events.delivery.lag`
//! (`handler`).

use std::borrow::Cow;

use opentelemetry::metrics::Meter;
use opentelemetry::trace::{Status, TraceContextExt, Tracer};
use opentelemetry::{Context, InstrumentationScope, KeyValue, Value, global};

pub(crate) use opentelemetry::context::FutureExt;

use crate::Error;

/// Java: `GlobalOpenTelemetry.get().getTracer("io.ekbatan.core", version)`.
pub const CORE_SCOPE: &str = "io.ekbatan.core";
/// Java: `GlobalOpenTelemetry.get().getMeter("io.ekbatan.events.localeventhandler")`.
pub const LOCAL_EVENT_HANDLER_SCOPE: &str = "io.ekbatan.events.localeventhandler";

fn scope(name: &'static str) -> InstrumentationScope {
    InstrumentationScope::builder(name)
        .with_version(env!("CARGO_PKG_VERSION"))
        .build()
}

/// The local event handler's meter. Asked for each time it is used, so a provider the app
/// installs after the jobs were built is still the one that counts.
pub(crate) fn local_event_handler_meter() -> Meter {
    global::meter_with_scope(scope(LOCAL_EVENT_HANDLER_SCOPE))
}

/// A span, a child of the current one; it ends when dropped. Run the work it covers inside it
/// with `.with_context(span.context())` (async) or `span.context().attach()` (blocking).
pub(crate) struct Span {
    cx: Context,
}

impl Span {
    pub(crate) fn start(name: &'static str, attributes: Vec<KeyValue>) -> Span {
        let tracer = global::tracer_with_scope(scope(CORE_SCOPE));
        let parent = Context::current();
        let span = tracer
            .span_builder(Cow::Borrowed(name))
            .with_attributes(attributes)
            .start_with_context(&tracer, &parent);
        Span {
            cx: parent.with_span(span),
        }
    }

    pub(crate) fn context(&self) -> Context {
        self.cx.clone()
    }

    pub(crate) fn set(&self, key: &'static str, value: impl Into<Value>) {
        self.cx.span().set_attribute(KeyValue::new(key, value));
    }

    /// Status `ERROR` with the error's text, and the error as an exception event.
    pub(crate) fn fail(&self, error: &Error) {
        let span = self.cx.span();
        span.set_status(Status::error(error.to_string()));
        span.record_error(error);
    }

    /// `result`, after marking the span failed if it is an `Err`.
    pub(crate) fn finish<T>(&self, result: Result<T, Error>) -> Result<T, Error> {
        if let Err(error) = &result {
            self.fail(error);
        }
        result
    }
}

impl Drop for Span {
    fn drop(&mut self) {
        self.cx.span().end();
    }
}

/// `ekbatan.transaction`, for one shard's transaction.
pub(crate) fn transaction_span(shard: crate::ShardIdentifier) -> Span {
    Span::start(
        "ekbatan.transaction",
        vec![
            KeyValue::new("ekbatan.shard.group", i64::from(shard.group)),
            KeyValue::new("ekbatan.shard.member", i64::from(shard.member)),
        ],
    )
}
