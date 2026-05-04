pub mod messages;
pub mod queues;
pub mod topics;

use axum::Router;
use axum::extract::State;
use axum::response::IntoResponse;
use axum::routing::{delete, get, post};

use crate::AppState;

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
