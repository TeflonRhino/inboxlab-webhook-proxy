# Heartbeat Client Success MCP

Read-only MCP server for ChatGPT that talks directly to Heartbeat.

## Tools
- list_heartbeat_channels
- find_client_chat(name)
- get_chat_messages(channel_id)
- get_client_context(name)

## Heartbeat endpoint used
GET https://api.heartbeat.chat/v0/chatChannel/{channelID}/messages

## Environment
- HEARTBEAT_API_KEY
- MCP_PATH_TOKEN

## Run
npm install
npm start

Health: /health
MCP: /mcp/<MCP_PATH_TOKEN>
