//! `ModelEvent`: what a model emits with a change, written to the event log. Java's
//! `ModelEvent`.

use std::fmt;

use crate::Error;

/// An event a model emits: Java's `ModelEvent`. `#[model_event(Model)]` implements it, and
/// derives serde's `Serialize` / `Deserialize` with Java's camelCase field names.
///
/// Java's event holds `modelId` and `modelName` itself; here the event log takes them from the
/// model the event is attached to, and writes them first in the payload as Java's JSON has them.
pub trait ModelEvent<M>: AnyModelEvent {}

/// What every event can do whatever its model: name its type, write its JSON, be downcast.
/// `#[model_event]` implements it.
pub trait AnyModelEvent: fmt::Debug + Send + Sync + 'static {
    /// The event's type name, as Java uses the simple class name: `WalletCreatedEvent`.
    fn event_type(&self) -> &'static str {
        short_name(std::any::type_name::<Self>())
    }

    /// The full type path, to tell apart two event types with one simple name.
    fn type_path(&self) -> &'static str {
        std::any::type_name::<Self>()
    }

    /// The event's own fields as JSON text, in declaration order.
    fn to_json(&self) -> Result<String, Error>;

    fn as_any(&self) -> &dyn std::any::Any;
}

/// `wallet_example::model::WalletCreatedEvent` -> `WalletCreatedEvent`: Java's simple class name.
/// The event type name an event of type `E` is logged under: its simple type name, as Java's
/// `getSimpleName()`.
pub fn event_type_name<E: AnyModelEvent>() -> &'static str {
    short_name(std::any::type_name::<E>())
}

pub(crate) fn short_name(full: &'static str) -> &'static str {
    full.rsplit("::").next().unwrap_or(full)
}
