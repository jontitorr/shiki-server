use crate::ws::server::CreateMessage;
use actix_web::{FromRequest, HttpMessage};
use chrono::Utc;
use deadpool_redis::redis;
use redis_derive::{FromRedisValue, ToRedisArgs};
use serde::{Deserialize, Serialize};
use std::future::ready;

#[derive(Clone, Debug, Deserialize, Serialize, ToRedisArgs, FromRedisValue)]
pub struct Attachment {
	pub id: i64,
	pub filename: String,
	pub size: usize,
	pub url: String,
	pub width: u32,
	pub height: u32,
	pub content_type: String,
}

#[derive(
	Clone,
	Debug,
	PartialEq,
	Eq,
	Deserialize,
	Serialize,
	Default,
	ToRedisArgs,
	FromRedisValue,
)]
pub struct Channel {
	/// The id of the channel
	pub id: i64,
	/// The name of the channel.
	pub name: String,
	/// The description of the channel.
	pub description: Option<String>,
	/// Unix timestamp for when channel was created.
	pub created_at: usize,
	/// The id of the user who created the channel.
	pub owner_id: i64,
}

impl Channel {
	pub fn new(
		id: i64, name: &str, description: Option<String>, owner_id: i64,
	) -> Self {
		Channel {
			id,
			name: name.to_string(),
			description,
			created_at: Utc::now().timestamp() as usize,
			owner_id,
		}
	}
}

#[derive(
	Clone,
	Debug,
	PartialEq,
	Eq,
	Deserialize,
	Serialize,
	Default,
	ToRedisArgs,
	FromRedisValue,
)]
pub struct Message {
	/// The id of the message
	pub id: i64,
	/// The id of the channel the message was sent in
	pub channel_id: i64,
	/// The id of the user who sent the message
	pub author_id: i64,
	/// The content of the message
	pub content: String,
	/// Unix timestamp for when the message was created
	pub created_at: usize,
}

impl Message {
	pub fn new(
		id: i64, channel_id: i64, author_id: i64, content: &str,
	) -> Self {
		Message {
			id,
			channel_id,
			author_id,
			content: content.to_string(),
			created_at: Utc::now().timestamp() as usize,
		}
	}
}

impl From<CreateMessage> for Message {
	fn from(msg: CreateMessage) -> Self {
		Self::new(msg.id, msg.channel_id, msg.author.id, &msg.content)
	}
}

#[derive(
	Clone,
	Debug,
	PartialEq,
	Eq,
	Deserialize,
	Serialize,
	Default,
	ToRedisArgs,
	FromRedisValue,
)]
pub struct User {
	pub id: i64,
	pub email: String,
	pub username: String,
	pub password: String,
	/// The user's authentication token.
	pub token: String,
	/// Unix timestamp for when user was created.
	pub created_at: usize,
	pub avatar: Option<String>,
}

impl User {
	pub fn new(id: i64, email: &str, username: &str, password: &str) -> Self {
		User {
			id,
			email: email.to_string(),
			username: username.to_string(),
			password: password.to_string(),
			token: uuid::Uuid::new_v4().to_string(),
			created_at: Utc::now().timestamp() as usize,
			avatar: None,
		}
	}
}

impl FromRequest for User {
	type Error = actix_web::Error;
	type Future = std::future::Ready<Result<Self, Self::Error>>;

	fn from_request(
		req: &actix_web::HttpRequest, _: &mut actix_web::dev::Payload,
	) -> Self::Future {
		let extensions = req.extensions();
		let user = extensions.get::<User>();

		if let Some(user) = user {
			ready(Ok(user.clone()))
		} else {
			ready(Err(actix_web::error::ErrorUnauthorized("Unauthorized")))
		}
	}
}
