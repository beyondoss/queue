pub mod messages;
pub mod queues;
pub mod topics;

use axum::Router;
use axum::extract::State;
use axum::response::IntoResponse;
use axum::routing::{delete, get, post};
use utoipa::OpenApi;

use crate::AppState;

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Beyond Queue",
        version = "1",
        description = "PostgreSQL-native message queue with SQS-compatible and REST APIs."
    ),
    paths(
        queues::create_queue,
        queues::list_queues,
        queues::get_queue,
        queues::delete_queue,
        queues::purge_queue,
        queues::list_subscriptions,
        messages::send_messages,
        messages::receive_messages,
        messages::delete_message,
        messages::delete_batch,
        messages::change_visibility,
        topics::send_topic,
        topics::subscribe_queue,
        topics::unsubscribe_queue,
        topics::list_subscriptions,
    ),
    components(schemas(
        crate::error::ErrorResponse,
        queues::CreateQueueRequest,
        queues::QueueResponse,
        queues::QueueMetricsResponse,
        queues::PurgeResponse,
        messages::SendRequest,
        messages::SendBody,
        messages::SendResponse,
        messages::MessageResponse,
        messages::ChangeVisibilityRequest,
        messages::ChangeVisibilityResponse,
        messages::DeleteBatchRequest,
        messages::DeletedResponse,
        topics::TopicSendRequest,
        topics::TopicSendResponse,
        topics::SubscribeRequest,
        crate::ops::topic::TopicMessage,
        crate::ops::topic::TopicSubscription,
    )),
    tags(
        (name = "queues", description = "Queue lifecycle and metrics"),
        (name = "messages", description = "Send, receive, delete, and visibility"),
        (name = "topics", description = "Topic fan-out and subscriptions"),
    )
)]
pub struct ApiDoc;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/cert", get(serve_cert))
        .route(
            "/queues",
            post(queues::create_queue).get(queues::list_queues),
        )
        .route(
            "/queues/{name}",
            get(queues::get_queue).delete(queues::delete_queue),
        )
        .route("/queues/{name}/purge", post(queues::purge_queue))
        .route(
            "/queues/{name}/messages",
            post(messages::send_messages)
                .get(messages::receive_messages)
                .delete(messages::delete_batch),
        )
        .route(
            "/queues/{name}/messages/{id}",
            delete(messages::delete_message).patch(messages::change_visibility),
        )
        .route(
            "/queues/{name}/subscriptions",
            get(queues::list_subscriptions),
        )
        .route("/topics/{routing_key}", post(topics::send_topic))
        .route(
            "/topics/{pattern}/subscriptions",
            post(topics::subscribe_queue).get(topics::list_subscriptions),
        )
        .route(
            "/topics/{pattern}/subscriptions/{id}",
            delete(topics::unsubscribe_queue),
        )
}

async fn serve_cert(State(state): State<AppState>) -> impl IntoResponse {
    (
        [("content-type", "application/x-pem-file")],
        state.signer.cert_pem().to_string(),
    )
}
