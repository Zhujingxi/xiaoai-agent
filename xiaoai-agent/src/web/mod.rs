pub mod api;
pub mod config_store;
pub mod restart;
pub mod status;

#[derive(Clone)]
pub struct WebState {
    pub store: std::sync::Arc<config_store::ConfigStore>,
    pub status: std::sync::Arc<status::RuntimeStatus>,
    pub restarter: std::sync::Arc<dyn restart::RestartController>,
}

pub async fn serve(listener: tokio::net::TcpListener, state: WebState) -> std::io::Result<()> {
    axum::serve(listener, api::router(state)).await
}
