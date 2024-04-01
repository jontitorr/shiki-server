mod api;
mod auth;
mod gateway;
mod middleware;
mod rtc;

use crate::redis::RedisFetcher;
use actix_web::web;
use mongodb::Client;

pub const DB_NAME: &str = "shiki";
pub const ATTACHMENT_COLL_NAME: &str = "attachments";
pub const CHANNEL_COLL_NAME: &str = "channels";
pub const MESSAGE_COLL_NAME: &str = "messages";
pub const USER_COLL_NAME: &str = "users";

pub async fn setup_indexes(client: &Client) -> anyhow::Result<()> {
	auth::setup_indexes(client).await
}

pub fn routes(client: &RedisFetcher, cfg: &mut web::ServiceConfig) {
	cfg.configure(|cfg| {
		api::routes(client, cfg);
	});

	cfg.configure(auth::routes)
		.configure(gateway::routes)
		.configure(rtc::routes);
}
