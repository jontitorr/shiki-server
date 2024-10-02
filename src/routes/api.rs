use super::middleware::Auth;
use crate::{
	models::{
		Attachment,
		Channel,
		Message,
		User,
	},
	redis::{
		ModifyUser,
		RedisFetcher,
	},
	routes::{
		DB_NAME,
		MESSAGE_COLL_NAME,
	},
	ws::server::{
		self,
		CreateMessage,
		Join,
		ListChannels,
		ShikiServer,
	},
	CloudinaryConfig,
};
use actix::Addr;
use actix_multipart::form::{
	tempfile::TempFile,
	MultipartForm,
};
use actix_web::{
	get,
	patch,
	post,
	web,
	HttpResponse,
	Responder,
};
use cloudinary::upload::{
	Source,
	Upload,
	UploadOptions,
};
use futures::TryStreamExt;
use futures_util::lock::Mutex;
use image::GenericImageView;
use lazy_static::lazy_static;
use mongodb::{
	bson::doc,
	options::FindOptions,
	Client,
};
use serde::{
	Deserialize,
	Serialize,
};
use snowflake::SnowflakeIdGenerator;
use std::{
	collections::{
		HashMap,
		HashSet,
	},
	sync::atomic::{
		AtomicUsize,
		Ordering,
	},
};

use validator::Validate;

/// Displays state
#[get("/count")]
async fn get_count(count: web::Data<AtomicUsize>) -> impl Responder {
	let current_count = count.load(Ordering::SeqCst);
	format!("Visitors: {current_count}")
}

#[derive(MultipartForm)]
struct CreateAttachment {
	file: TempFile,
}

lazy_static! {
	static ref VALID_CONTENT_TYPES: HashSet<&'static str> = {
		let mut set = HashSet::new();
		set.insert("image/png");
		set.insert("image/jpeg");
		set.insert("image/gif");
		set.insert("image/webp");
		set
	};
}

// TODO: Might just make a default HttpResponse with the INTERNAL_ERROR.
const INTERNAL_ERROR: &str = "Something went wrong";

#[derive(Serialize)]
struct AttachmentResponse {
	id: i64,
	url: String,
}

/// Creates an attachment/Uploads an image. Should restrict to images only for
/// now.
#[post("/channels/{channel_id}/attachments")]
async fn create_attachment(
	cloudinary: web::Data<CloudinaryConfig>, channel_id: web::Path<i64>,
	form: MultipartForm<CreateAttachment>, fetcher: web::Data<RedisFetcher>,
	snowflake_gen: web::Data<Mutex<SnowflakeIdGenerator>>,
) -> HttpResponse {
	const MAX_FILE_SIZE: usize = 1024 * 1024 * 5;

	// Check if the channel exists.
	match fetcher.fetch_channels(Some(&[*channel_id])).await {
		Ok(channels) => {
			if channels.is_empty() {
				return HttpResponse::NotFound().finish();
			}
		}
		_ => return HttpResponse::NotFound().finish(),
	}

	// Validate the file
	match form.file.size {
		0 => return HttpResponse::BadRequest().finish(),
		length if length > MAX_FILE_SIZE => {
			return HttpResponse::BadRequest().body(format!(
				"The uploaded file is too large. Maximum size is {} bytes.",
				MAX_FILE_SIZE
			));
		}
		_ => {}
	};

	let content_type =
		match form.file.content_type.as_ref().map(|s| s.to_string()) {
			Some(content_type) => content_type,
			None => return HttpResponse::BadRequest().body(INTERNAL_ERROR),
		};

	if !VALID_CONTENT_TYPES.contains(&content_type.as_str()) {
		return HttpResponse::BadRequest().body(INTERNAL_ERROR);
	}

	let (format, img) = match tokio::fs::read(form.file.file.path()).await {
		Ok(bytes) => {
			match (image::guess_format(&bytes), image::load_from_memory(&bytes))
			{
				(Ok(format), Ok(img)) => (Some(format), img),
				_ => return HttpResponse::BadRequest().body(INTERNAL_ERROR),
			}
		}
		_ => return HttpResponse::BadRequest().body(INTERNAL_ERROR),
	};

	if format.is_none() {
		return HttpResponse::BadRequest().body(INTERNAL_ERROR);
	}

	let (w, h) = img.dimensions();

	// Max Height 512 (for storage reasons)
	if h > 512 {
		let ratio = 512_f32 / h as f32;
		let new_w = (w as f32 * ratio) as u32;

		match img
			.resize(new_w, 512, image::imageops::FilterType::Lanczos3)
			.save_with_format(form.file.file.path(), format.unwrap())
		{
			Ok(_) => {}
			Err(err) => {
				return HttpResponse::InternalServerError()
					.body(err.to_string());
			}
		}
	}

	let sanitized = match form.file.file_name {
		Some(ref file_name) => sanitize_filename::sanitize(file_name),
		None => {
			return HttpResponse::InternalServerError()
				.body("Missing file name")
		}
	};

	let id = snowflake_gen.lock().await.real_time_generate();
	let mut file_ext = String::new();
	let filename_noext = {
		if let Some(idx) = sanitized.rfind('.') {
			file_ext = sanitized[idx..].to_string();
			sanitized[..idx].to_string()
		} else {
			sanitized.clone()
		}
	};
	// We don't want to create an additional folder.
	let options = UploadOptions::new().set_public_id(format!(
		"attachments/{}/{}-{}",
		channel_id,
		id,
		filename_noext.clone()
	));
	let upload = Upload::new(
		cloudinary.api_key.clone(),
		cloudinary.cloud_name.clone(),
		cloudinary.api_secret.clone(),
	);

	if upload
		.image(Source::Path(form.file.file.path().to_path_buf()), &options)
		.await
		.is_err()
	{
		return HttpResponse::InternalServerError().body(INTERNAL_ERROR);
	}

	let attachment = Attachment {
		id,
		filename: sanitized.clone(),
		size: form.file.size,
		url: format!(
			// TODO: Make customizable.
			"https://cdn.shiki.space/{}{}",
			format!("attachments/{}/{}/{}", channel_id, id, filename_noext),
			file_ext
		),
		width: w,
		height: h,
		content_type,
	};

	match fetcher.insert_attachment(attachment.clone()).await {
		Ok(_) => HttpResponse::Ok().json(attachment),
		_ => HttpResponse::InternalServerError().body(INTERNAL_ERROR),
	}
}

/// Shows all the channels available
#[get("/channels")]
async fn get_channels_list(
	srv: web::Data<Addr<crate::ws::server::ShikiServer>>,
) -> HttpResponse {
	match srv.send(ListChannels).await {
		Ok(channels) => HttpResponse::Ok().json(channels),
		Err(err) => HttpResponse::InternalServerError().body(err.to_string()),
	}
}

#[derive(Deserialize, Validate, Serialize)]
struct CreateChannel {
	#[validate(length(min = 1), non_control_character)]
	pub name: String,
}

/// Creates a new channel.
#[post("/channels")]
async fn create_channel(
	data: web::Json<CreateChannel>, fetcher: web::Data<RedisFetcher>,
	snowflake_gen: web::Data<Mutex<SnowflakeIdGenerator>>,
	srv: web::Data<Addr<ShikiServer>>, user: User,
) -> HttpResponse {
	if let Err(err) = data.validate() {
		return HttpResponse::BadRequest().json(err);
	}

	let data = data.into_inner();
	let id = snowflake_gen.lock().await.real_time_generate();
	let channel = Channel::new(id, &data.name, None, user.id);
	let res = fetcher.insert_channel(channel).await;

	if res.is_err() {
		return HttpResponse::InternalServerError().body(INTERNAL_ERROR);
	}

	match srv
		.send(server::Channel {
			id,
			guild_id: None,
			name: data.name,
			sessions: HashSet::new(),
		})
		.await
	{
		Ok(Some(channel)) => HttpResponse::Ok().json(channel),
		Ok(None) => HttpResponse::BadRequest().body("Channel already exists"),
		Err(err) => HttpResponse::InternalServerError().body(err.to_string()),
	}
}

/// Joins a channel
// NOTE: This is should be an internal feature, caused by the future addition of
// channel viewing permissions. Editing said permissions should allow a user to
// effectively "join" a channel.
#[post("/channels/{channel_id}/join")]
async fn join_channel(
	channel_id: web::Path<i64>, srv: web::Data<Addr<ShikiServer>>,
) -> HttpResponse {
	match srv.send(Join { client_id: 0, channel_id: *channel_id }).await {
		Ok(Some(channel)) => HttpResponse::Ok().json(channel),
		Ok(None) => HttpResponse::BadRequest().body("Channel does not exist"),
		Err(err) => HttpResponse::InternalServerError().body(err.to_string()),
	}
}

#[derive(Deserialize)]
struct GetMessages {
	/// Get messages before this message ID
	#[serde(default = "default_before")]
	before: Option<i64>,
	/// Get messages after this message ID
	#[serde(default = "default_after")]
	after: Option<i64>,
	/// Max number of messages to return (1-100)
	#[serde(default = "default_limit")]
	limit: i64,
}

fn default_before() -> Option<i64> {
	None
}

fn default_after() -> Option<i64> {
	None
}

fn default_limit() -> i64 {
	50
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, Default)]
pub struct GetMessage {
	/// The id of the message
	pub id: i64,
	/// The id of the channel the message was sent in
	pub channel_id: i64,
	/// The content of the message
	pub content: String,
	/// Unix timestamp for when the message was created
	pub created_at: usize,
	/// User who sent the message
	pub author: server::User,
	/// Attachments of the message
	#[serde(skip_serializing_if = "Option::is_none")]
	pub attachments: Option<Vec<Attachment>>,
}

/// Fetches the messages in a channel
#[get("/channels/{channel_id}/messages")]
async fn get_messages(
	channel_id: web::Path<i64>, client: web::Data<Client>,
	data: web::Query<GetMessages>, fetcher: web::Data<RedisFetcher>,
) -> HttpResponse {
	if data.limit < 1 || data.limit > 100 {
		return HttpResponse::BadRequest()
			.body("Limit must be between 1 and 100");
	}

	let mut query = doc! {
		"channel_id": *channel_id
	};

	if let Some(before) = data.before {
		query.insert(
			"id",
			doc! {
				"$lt": before
			},
		);
	}

	if let Some(after) = data.after {
		query.insert(
			"id",
			doc! {
				"$gt": after
			},
		);
	}

	let cursor = client
		.database(DB_NAME)
		.collection::<Message>(MESSAGE_COLL_NAME)
		.find(
			query,
			Some(
				FindOptions::builder()
					.sort(doc! {"id": 1})
					.limit(data.limit)
					.build(),
			),
		)
		.await;

	let mut messages = match cursor {
		Ok(cursor) => match cursor.try_collect::<Vec<Message>>().await {
			Ok(res) => res,
			Err(_) => {
				return HttpResponse::InternalServerError()
					.body(INTERNAL_ERROR);
			}
		},

		Err(_) => {
			return HttpResponse::InternalServerError().body(INTERNAL_ERROR);
		}
	};

	let attachment_ids: Vec<i64> = messages
		.iter()
		.filter_map(|msg| msg.attachments.as_ref())
		.flat_map(|attachments| attachments.iter().cloned())
		.collect();

	let attachments = fetcher
		.fetch_attachments(Some(&attachment_ids))
		.await
		.unwrap_or_default()
		.into_iter()
		.map(|res| (res.id, res))
		.collect::<HashMap<_, _>>();

	let user_ids = messages
		.iter()
		.map(|msg| msg.author_id)
		.collect::<HashSet<i64>>()
		.into_iter()
		.collect::<Vec<i64>>();

	log::debug!("User IDs: {:?}", user_ids);

	// Fetch the users
	let users = fetcher.fetch_users(Some(&user_ids)).await;
	let users: HashMap<i64, server::User> = match users {
		Ok(users) => {
			users.into_iter().map(|user| (user.id, user.into())).collect()
		}
		Err(_) => {
			return HttpResponse::InternalServerError().body(INTERNAL_ERROR);
		}
	};

	if users.len() < user_ids.len() {
		log::warn!(
			"Missing {:?} users when fetching messages",
			user_ids.len() - users.len()
		);

		// Redact the identity of this missing user from all messages.
		let missing = user_ids
			.into_iter()
			.filter(|id| !users.contains_key(id))
			.collect::<Vec<i64>>();

		for id in missing {
			for msg in messages.iter_mut() {
				if msg.author_id == id {
					msg.author_id = 0;
				}
			}
		}
	}

	let messages: Vec<GetMessage> = messages
		.into_iter()
		.map(|msg| {
			let author = if msg.author_id == 0 {
				server::User {
					username: "Deleted User".to_string(),
					..Default::default()
				}
			} else {
				users.get(&msg.author_id).cloned().unwrap_or_default()
			};

			GetMessage {
				id: msg.id,
				channel_id: msg.channel_id,
				content: msg.content,
				created_at: msg.created_at,
				author,
				attachments: msg.attachments.map(|ids| {
					ids.into_iter()
						.filter_map(|id| attachments.get(&id).cloned())
						.collect::<Vec<Attachment>>()
				}),
			}
		})
		.collect();

	HttpResponse::Ok().json(messages)
}

/// Creates a new message
#[post("/channels/{channel_id}/messages")]
async fn create_message(
	channel_id: web::Path<i64>, data: web::Json<CreateMessage>,
	fetcher: web::Data<RedisFetcher>,
	snowflake_gen: web::Data<Mutex<SnowflakeIdGenerator>>,
	srv: web::Data<Addr<ShikiServer>>, user: User,
) -> HttpResponse {
	let mut data = data.into_inner();

	// Check if the referred attachments exist.
	if let Some(attachments) = &mut data.attachments {
		let res = fetcher
			.fetch_attachments(Some(attachments))
			.await
			.unwrap_or_default();

		if res.len() != attachments.len() {
			return HttpResponse::BadRequest()
				.body("Attachments do not exist!");
		}

		data.attachments_raw = Some(res);
	}

	data.id = snowflake_gen.lock().await.real_time_generate();
	data.channel_id = channel_id.into_inner();
	data.author = server::User {
		id: user.id,
		username: user.username,
		joined: user.created_at,
		avatar: user.avatar,
	};

	let res = fetcher.insert_message(Message::from(data.clone())).await;

	if res.is_err() {
		return HttpResponse::InternalServerError().body(INTERNAL_ERROR);
	}

	// TODO: Refactor this so the response is not dependent on the gateway's
	// response. Messages should still return 200s even if the gateway were to
	// be down.
	match srv.send(data).await {
		Ok(Some(msg)) => HttpResponse::Ok().json(msg),
		Ok(None) => HttpResponse::BadRequest().body("Channel does not exist!"),
		Err(e) => {
			log::error!("Failed to send message: {:?}", e);
			HttpResponse::InternalServerError().body(INTERNAL_ERROR)
		}
	}
}

/// Modify the requester's user account settings. Returns a user object on
/// success.
// TODO: Fire a User Update Gateway event.
#[patch("/users/@me")]
async fn modify_user(
	data: web::Json<ModifyUser>, fetcher: web::Data<RedisFetcher>,
	mut user: User,
) -> HttpResponse {
	if let Err(err) = data.validate() {
		return HttpResponse::BadRequest().json(err);
	}

	match fetcher.modify_user(&mut user, data.into_inner()).await {
		Ok(_) => HttpResponse::Ok().json(server::User {
			id: user.id,
			username: user.username,
			joined: user.created_at,
			avatar: user.avatar,
		}),
		Err(err) => {
			log::error!("{:?}", err);
			HttpResponse::InternalServerError().body(INTERNAL_ERROR)
		}
	}
}

pub fn routes(client: &RedisFetcher, cfg: &mut web::ServiceConfig) {
	cfg.service(
		web::scope("/api")
			.service(get_count)
			.service(create_attachment)
			.service(get_channels_list)
			.service(create_channel)
			.service(join_channel)
			.service(create_message)
			.service(get_messages)
			.service(modify_user)
			.wrap(Auth::new(client.clone())),
	);
}
