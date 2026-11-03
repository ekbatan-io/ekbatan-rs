use ekbatan::Error;
use ekbatan::action::{Action, ActionPlan};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::model::Notification;

/// Java's `CreateNotificationAction.Params`.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Params {
    pub wallet_id: Uuid,
    pub kind: String,
    pub recipient: String,
    pub message: String,
}

/// Adds a notification: an entity, so it writes no events of its own.
#[derive(Default)]
pub struct CreateNotificationAction;

impl Action for CreateNotificationAction {
    type Params = Params;
    type Result = Notification;

    async fn perform(
        &self,
        plan: &mut ActionPlan,
        _principal: Option<&str>,
        params: &Params,
    ) -> Result<Notification, Error> {
        let notification = Notification::create_notification(
            params.wallet_id,
            &params.kind,
            &params.recipient,
            &params.message,
        )
        .build()?;
        plan.add(notification)
    }
}
