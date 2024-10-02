# Shiki Server

This is the backend service for Shiki, a real-time chat application built with Rust and Actix. It provides WebSocket, HTTP API, and WebRTC functionality for a robust, asynchronous communication platform.

## Features

- WebSocket server for real-time messaging
- RESTful API endpoints
- WebRTC support for peer-to-peer communication
- Asynchronous architecture for high performance
- MongoDB integration for data persistence
- Redis for session management and caching
- Cloudinary integration for media handling

## Technologies Used

- Rust
- Actix web framework
- WebSocket (via Actix)
- WebRTC (via webrtc_unreliable)
- MongoDB
- Redis
- Cloudinary

## Setup

1. lone the repository
2. Create a .env file in the root directory with the following variables:

```bash
MONGODB_URI=your_mongodb_connection_string
SESSION_KEY=your_session_key
SERVER_URL=your_server_url
CLOUDINARY_CLOUD_NAME=your_cloudinary_cloud_name
CLOUDINARY_API_KEY=your_cloudinary_api_key
CLOUDINARY_API_SECRET=your_cloudinary_api_secret
```

3. Run `cargo run`

## Architecture

The application uses Actix for the web server and WebSocket handling. It integrates MongoDB for data storage and Redis for session management. The WebRTC functionality is provided through the `webrtc_unreliable` crate.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

MIT License
