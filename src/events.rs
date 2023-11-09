use actix::Message;
use serde::{Deserialize, Serialize};

// Opcode enum used for sending and receiving many of the Gateway events similar to the Discord Gateway.
#[derive(Deserialize, Debug, PartialEq, Eq, Copy, Clone)]
#[repr(u8)]
pub enum Opcode {
	Identify,
	Ready,
	MessageCreate,
	ChannelCreate,
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
		S: serde::Serializer,
	{
		serializer.serialize_u8(*self as u8)
	}
}

/// Identify Payload which is sent from the server to all clients notifying them someone is online.
#[derive(Message, Serialize, Deserialize, Debug)]
#[rtype(result = "()")]
pub struct Ready {
	op: Opcode,
	pub id: i64,
	pub name: String,
}

impl Ready {
	pub fn new(id: i64, name: String) -> Self {
		Self { op: Opcode::Ready, id, name }
	}
}

#[derive(Message, Serialize, Deserialize, Debug)]
#[rtype(result = "()")]
pub struct MessageCreate {
	op: Opcode,
	/// The id of the message
	pub id: i64,
	/// The content of the message
	pub content: String,
	/// The author of the message
	pub author_id: Option<i64>,
	/// The ID of the channel/room.
	pub channel_id: i64,
}

impl MessageCreate {
	pub fn new(
		id: i64, content: String, author_id: Option<i64>, channel_id: i64,
	) -> Self {
		Self { op: Opcode::MessageCreate, id, content, author_id, channel_id }
	}
}

#[derive(Message, Serialize, Deserialize, Debug)]
#[rtype(result = "()")]
pub struct ChannelCreate {
	op: Opcode,
	/// The id of the channel
	pub id: i64,
	/// The name of the channel
	pub name: String,
}

impl ChannelCreate {
	pub fn new(id: i64, name: String) -> Self {
		Self { op: Opcode::ChannelCreate, id, name }
	}
}
