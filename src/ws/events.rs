use super::server::{Channel, CreateMessage};
use crate::{models, ws::server::User};
use actix::Message;
use derives::HasOpcode;
use serde::{Deserialize, Serialize, Serializer};

// This file contains all the events that can be sent from the server to the client.

pub trait HasOpcode {
	fn opcode() -> Opcode;
}

// Opcode enum used for sending and receiving many of the Gateway events similar to the Discord Gateway.
#[derive(Deserialize, Debug, PartialEq, Eq, Copy, Clone)]
#[repr(u8)]
pub enum Opcode {
	Identify,
	Ready,
	MessageCreate,
	ChannelCreate,
	Custom,
}

impl Opcode {
	pub fn from_json(value: Option<&serde_json::Value>) -> Option<Opcode> {
		value.and_then(|opcode| opcode.as_u64()).and_then(|opcode| match opcode
		{
			0 => Some(Opcode::Identify),
			1 => Some(Opcode::Ready),
			2 => Some(Opcode::MessageCreate),
			3 => Some(Opcode::ChannelCreate),
			_ => None,
		})
	}
}

impl Serialize for Opcode {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_u8(*self as u8)
	}
}

/// Identify Payload which is sent from the server to the client. This will contain a lot of data, like the guilds they are in, channels, etc. To be used as the entry point/hello world of the application.
#[derive(Message, Serialize, Deserialize, Debug, Clone, HasOpcode)]
#[opcode(value = "Opcode::Ready")]
#[rtype(result = "()")]
pub struct Ready {
	/// List of available channels.
	pub channels: Vec<Channel>,
	/// The user who connected
	pub user: User,
	/// List of all the users that are in the guild. Including the user who connected.
	pub users: Vec<User>,
}

#[derive(Message, Serialize, Deserialize, Debug, Clone, HasOpcode)]
#[opcode(value = "Opcode::MessageCreate")]
#[rtype(result = "()")]
pub struct MessageCreate {
	/// The id of the message
	pub id: i64,
	/// The content of the message
	pub content: String,
	/// The ID of the channel.
	pub channel_id: i64,
	/// The author of the message.
	pub author: User,
	/// The creation date of the message
	pub created_at: usize,
	/// The attachments of the message.
	pub attachments: Option<Vec<models::Attachment>>,
}

impl From<CreateMessage> for MessageCreate {
	fn from(msg: CreateMessage) -> Self {
		Self {
			id: msg.id,
			content: msg.content,
			channel_id: msg.channel_id,
			author: msg.author,
			created_at: msg.created_at,
			attachments: msg.attachments_raw,
		}
	}
}

#[derive(Message, Serialize, Deserialize, Debug, Clone, HasOpcode)]
#[opcode(value = "Opcode::ChannelCreate")]
#[rtype(result = "()")]
pub struct ChannelCreate {
	/// The id of the channel
	pub id: i64,
	/// The name of the channel
	pub name: String,
}

impl ChannelCreate {
	pub fn new(id: i64, name: String) -> Self {
		Self { id, name }
	}
}

/// Chat server sends this messages to session
#[derive(Message, Debug, Clone)]
#[rtype(result = "()")]
pub enum Event {
	ChannelCreate(ChannelCreate),
	MessageCreate(MessageCreate),
	Ready(Ready),

	BadToken,
	Hello,
	SetToken(String),
	Custom(String),
}

impl Event {
	pub fn opcode(&self) -> Opcode {
		match self {
			Event::ChannelCreate(_) => ChannelCreate::opcode(),
			Event::MessageCreate(_) => MessageCreate::opcode(),
			Event::Ready(_) => Ready::opcode(),

			Event::Custom(_) => Opcode::Custom,
			Event::BadToken => Opcode::Custom,
			Event::Hello => Opcode::Custom,
			Event::SetToken(_) => Opcode::Custom,
		}
	}
}

impl Serialize for Event {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		match self {
			Event::ChannelCreate(channel) => channel.serialize(serializer),
			Event::MessageCreate(message) => message.serialize(serializer),
			Event::Ready(ready) => ready.serialize(serializer),

			Event::Custom(msg) => serializer.serialize_str(msg),
			Event::BadToken => serializer.serialize_str(""),
			Event::Hello => serializer.serialize_str(""),
			Event::SetToken(token) => serializer.serialize_str(token),
		}
	}
}
