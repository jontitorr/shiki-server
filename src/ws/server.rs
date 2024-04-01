use super::events::{self, Event};
use crate::{
	models,
	redis::RedisFetcher,
	utils::{self},
	ws::events::Ready,
};
use actix::prelude::*;
use chrono::Utc;
use mongodb::bson::doc;
use rand::{self, rngs::ThreadRng, Rng};
use serde::{Deserialize, Serialize};
use std::{
	collections::{HashMap, HashSet},
	sync::{
		atomic::{AtomicUsize, Ordering},
		Arc,
	},
};

/// New chat session is created
#[derive(Message)]
#[rtype(usize)]
pub struct Connect {
	pub addr: Recipient<Event>,
}

/// Session is disconnected
#[derive(Message)]
#[rtype(result = "()")]
pub struct Disconnect {
	pub id: usize,
}

/// Payload sent from client to identify itself.
#[derive(Message)]
#[rtype(result = "()")]
pub struct Identify {
	pub id: usize,
	pub token: String,
}

/// Create new channel
#[derive(Message, Serialize, Debug, Clone, Deserialize)]
#[rtype(result = "Option<Channel>")]
pub struct Channel {
	/// Channel ID
	pub id: i64,
	/// Guild ID
	pub guild_id: Option<i64>,
	/// Channel name
	pub name: String,
	/// IDs of sessions in the channel
	#[serde(skip_serializing, skip_deserializing)]
	pub sessions: HashSet<usize>,
}

impl From<models::Channel> for Channel {
	fn from(channel: models::Channel) -> Self {
		Self {
			id: channel.id,
			guild_id: None,
			name: channel.name,
			sessions: HashSet::new(),
		}
	}
}

/// Create new Guild
#[derive(Message, Serialize, Debug, Clone, Deserialize)]
#[rtype(result = "Option<Guild>")]
pub struct Guild {
	/// ID of the guild
	pub id: i64,
	/// Guild name
	pub name: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, Default)]
pub struct User {
	pub id: i64,
	pub username: String,
	pub joined: usize,
	/// Avatar URL
	pub avatar: Option<String>,
}

impl From<models::User> for User {
	fn from(user: models::User) -> Self {
		Self {
			id: user.id,
			username: user.username,
			joined: user.created_at,
			avatar: user.avatar,
		}
	}
}

/// Create new message
#[derive(Message, Serialize, Deserialize, Debug, Clone)]
#[rtype(result = "Option<CreateMessage>")]
pub struct CreateMessage {
	/// Message ID
	#[serde(skip_deserializing)]
	pub id: i64,
	/// Channel ID
	#[serde(skip_deserializing)]
	pub channel_id: i64,
	/// Message content
	pub content: String,
	/// User who sent the message
	#[serde(skip_deserializing)]
	pub author: User,
	/// Message creation time
	#[serde(default = "current_utc_timestamp", skip_deserializing)]
	pub created_at: usize,
	/// Attachments' IDs (previously uploaded from the API)
	pub attachments: Option<Vec<i64>>,
}

fn current_utc_timestamp() -> usize {
	let utc_now = Utc::now();
	utc_now.timestamp() as usize
}

/// List of available channels
#[derive(Message)]
#[rtype(result = "Vec<Channel>")]
pub struct ListChannels;

/// Join channel, if channel does not exists create new one.
#[derive(Message)]
#[rtype(result = "Option<Channel>")]
pub struct Join {
	/// Client ID
	pub client_id: usize,
	/// Channel ID
	pub channel_id: i64,
}

/// `ShikiServer` manages chat channels and responsible for coordinating chat session.
///
/// Implementation is very naïve.
#[derive(Debug)]
pub struct ShikiServer {
	/// MongoDB client
	client: RedisFetcher,
	/// The actual connected clients to the gateway.
	sessions: HashMap<usize, Recipient<Event>>,
	/// Chat channels. In this case they're individual channels where messages are propagated to users in the same channel. This could be a channel, guild, etc.
	channels: HashMap<i64, Channel>,
	/// Random generator for making unique IDs.
	rng: ThreadRng,
	/// Number of connected clients
	visitor_count: Arc<AtomicUsize>,
}

impl ShikiServer {
	pub fn new(client: RedisFetcher, visitor_count: Arc<AtomicUsize>) -> Self {
		Self {
			client,
			sessions: HashMap::new(),
			channels: HashMap::new(),
			rng: rand::thread_rng(),
			visitor_count,
		}
	}
}

impl ShikiServer {
	/// Send message to all users in the channel
	fn send_channel_message(
		&self, channel: i64, message: Event, skip_id: usize,
	) {
		if let Some(sessions) =
			self.channels.get(&channel).map(|channel| &channel.sessions)
		{
			log::debug!(
				"Sending message to {} sessions in channel {channel}",
				sessions.len()
			);

			for id in sessions {
				if *id != skip_id {
					if let Some(addr) = self.sessions.get(id) {
						addr.do_send(message.clone());
					}
				}
			}
		}
	}

	/// Send message to literally everyone.
	fn send_to_everyone(&self, message: Event, skip_id: usize) {
		// message every session.
		for (id, addr) in &self.sessions {
			if *id != skip_id {
				addr.do_send(message.clone());
			}
		}
	}
}

/// Make actor from `ChatServer`
impl Actor for ShikiServer {
	/// We are going to use simple Context, we just need ability to communicate
	/// with other actors.
	type Context = Context<Self>;

	fn started(&mut self, ctx: &mut Self::Context) {
		let client_clone = self.client.clone();

		async move {
			let channels = client_clone.fetch_channels(None).await;

			if let Err(e) = channels {
				log::error!("Failed to load channels: {}", e);
				return Err(e);
			}

			let channels = channels.unwrap();

			let channels: HashMap<i64, Channel> =
				channels.into_iter().map(|c| (c.id, c.into())).collect();

			Ok(channels)
		}
		.into_actor(self)
		.then(move |res, act, ctx| {
			match res {
				Ok(channels) => {
					act.channels = channels;
				}
				Err(_) => {
					log::error!("Failed to load channels, closing server");
					ctx.stop();
				}
			}

			log::info!("Loaded {} channels", act.channels.len());
			fut::ready(())
		})
		.wait(ctx);
	}
}

impl Handler<Connect> for ShikiServer {
	type Result = usize;

	fn handle(&mut self, msg: Connect, _: &mut Context<Self>) -> Self::Result {
		log::debug!("Someone joined");

		// register session with random id
		let id = self.rng.gen::<usize>();
		self.sessions.insert(id, msg.addr.clone());

		// Insert the user into every single channel's sessions.
		for channel in self.channels.values_mut() {
			channel.sessions.insert(id);
		}

		// Send an Identify event to the client so they may authenticate themselves.
		msg.addr.do_send(Event::Hello);

		let count = self.visitor_count.fetch_add(1, Ordering::SeqCst);

		log::info!("{} visitors online", count);

		id
	}
}

impl Handler<Disconnect> for ShikiServer {
	type Result = ();

	fn handle(&mut self, msg: Disconnect, _: &mut Context<Self>) {
		log::info!("{} disconnected", msg.id);

		let count = self.visitor_count.fetch_update(
			Ordering::SeqCst,
			Ordering::SeqCst,
			|current| {
				if current > 0 {
					Some(current - 1)
				} else {
					None
				}
			},
		);

		if let Ok(updated_count) = count {
			log::info!("{} visitors online", updated_count);
		}

		let mut channels: Vec<i64> = Vec::new();

		if self.sessions.remove(&msg.id).is_some() {
			for channel in self.channels.values_mut() {
				if channel.sessions.remove(&msg.id) {
					channels.push(channel.id);
				}
			}
		}

		for channel in channels {
			self.send_channel_message(
				channel,
				Event::Custom(format!("{} left", msg.id)),
				0,
			);
		}
	}
}

impl Handler<Identify> for ShikiServer {
	type Result = ();

	fn handle(&mut self, msg: Identify, ctx: &mut Context<Self>) {
		let session = if let Some(s) = self.sessions.get(&msg.id).cloned() {
			s
		} else {
			return;
		};

		let channels = self.channels.clone();
		let client_clone = self.client.clone();

		async move {
			let res =
				utils::validate_token(client_clone.clone(), msg.token.clone())
					.await;

			let user = match res {
				Ok(Some(user)) => user,
				Ok(None) => {
					log::warn!("Invalid token");
					return session.do_send(Event::BadToken);
				}
				Err(e) => {
					log::error!("Failed to validate token: {}", e);
					log::debug!(
						"Disconnecting session for failed token validation"
					);
					return session.do_send(Event::BadToken);
				}
			};

			session.do_send(Event::SetToken(msg.token));

			log::info!(
				"User {} authenticated, sending Ready payload...",
				user.username
			);

			let users = client_clone
				.fetch_users(None)
				.await
				.unwrap_or(Vec::new())
				.into_iter()
				.map(|u| User {
					username: u.username,
					id: u.id,
					avatar: u.avatar,
					joined: u.created_at,
				})
				.collect();

			session.do_send(Event::Ready(Ready {
				channels: channels.values().cloned().collect(),
				user: User {
					username: user.username,
					id: user.id,
					avatar: user.avatar,
					joined: user.created_at,
				},
				users,
			}));
		}
		.into_actor(self)
		.then(|_, _, _| fut::ready(()))
		.wait(ctx);
	}
}

impl Handler<Channel> for ShikiServer {
	type Result = MessageResult<Channel>;

	fn handle(
		&mut self, mut msg: Channel, _: &mut Context<Self>,
	) -> Self::Result {
		log::info!("Channel created");

		if self.channels.contains_key(&msg.id) {
			return MessageResult(None);
		}

		msg.sessions = self.sessions.keys().cloned().collect();
		self.channels.insert(msg.id, msg.clone());

		self.send_to_everyone(
			Event::ChannelCreate(events::ChannelCreate::new(
				msg.id,
				msg.name.clone(),
			)),
			0,
		);

		MessageResult(Some(msg))
	}
}

impl Handler<CreateMessage> for ShikiServer {
	type Result = MessageResult<CreateMessage>;

	fn handle(
		&mut self, msg: CreateMessage, _: &mut Context<Self>,
	) -> Self::Result {
		log::info!("Create message request: {:?}", msg);

		if !self.channels.contains_key(&msg.channel_id) {
			return MessageResult(None);
		}

		let event = events::MessageCreate::from(msg.clone());

		self.send_channel_message(
			msg.channel_id,
			Event::MessageCreate(event),
			0,
		);

		MessageResult(Some(msg))
	}
}

impl Handler<ListChannels> for ShikiServer {
	type Result = MessageResult<ListChannels>;

	fn handle(
		&mut self, _: ListChannels, _: &mut Context<Self>,
	) -> Self::Result {
		MessageResult(self.channels.values().cloned().collect())
	}
}

impl Handler<Join> for ShikiServer {
	type Result = MessageResult<Join>;

	fn handle(&mut self, msg: Join, _: &mut Context<Self>) -> Self::Result {
		let Join { client_id, channel_id } = msg;

		match self.channels.get_mut(&channel_id) {
			Some(channel) => {
				channel.sessions.insert(client_id);
			}

			None => {
				return MessageResult(None);
			}
		}

		self.send_channel_message(
			channel_id,
			Event::Custom("Someone connected".to_owned()),
			client_id,
		);

		let channel = self.channels.get(&channel_id).unwrap().clone();

		MessageResult(Some(channel))
	}
}
