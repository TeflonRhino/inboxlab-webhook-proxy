import express from "express";
import { McpServer } from "@modelcontextprotocol/server";
import { createMcpHandler } from "@modelcontextprotocol/server";
import { createMcpExpressApp } from "@modelcontextprotocol/express";
import { toNodeHandler } from "@modelcontextprotocol/node";
import * as z from "zod/v4";
import pg from "pg";

const HEARTBEAT_BASE_URL = "https://api.heartbeat.chat/v0";
const HEARTBEAT_API_KEY = process.env.HEARTBEAT_API_KEY;
const MCP_PATH_TOKEN = process.env.MCP_PATH_TOKEN;
const PORT = Number(process.env.PORT || 3000);
const DATABASE_URL = process.env.DATABASE_URL;
const HEARTBEAT_FROM_USER_ID = process.env.HEARTBEAT_FROM_USER_ID || "d8a956be-e3e4-4ef1-9978-6f1b448d3cba";
const pool = DATABASE_URL ? new pg.Pool({ connectionString: DATABASE_URL, ssl: { rejectUnauthorized: false } }) : null;

if (!HEARTBEAT_API_KEY) throw new Error("HEARTBEAT_API_KEY is required");
if (!MCP_PATH_TOKEN || MCP_PATH_TOKEN.length < 24) throw new Error("MCP_PATH_TOKEN is required and should be at least 24 characters");

type HeartbeatChannel = { id: string; name: string; type?: string; emoji?: string; [k: string]: unknown };
type HeartbeatMessage = {
  id?: string;
  userID?: string;
  createdAt?: string;
  content?: string;
  images?: string[];
  files?: string[];
  [k: string]: unknown;
};
type HeartbeatUser = { id?: string; name?: string; fullName?: string; firstName?: string; lastName?: string; email?: string; [k: string]: unknown };

let userCache: { at: number; byId: Map<string, string> } | null = null;

async function ensureQueueTable() {
  if (!pool) throw new Error("DATABASE_URL is not configured");
  await pool.query(`
    CREATE TABLE IF NOT EXISTS heartbeat_hold_queue (
      id BIGSERIAL PRIMARY KEY,
      client_name TEXT NOT NULL,
      channel_id UUID NOT NULL,
      draft TEXT NOT NULL,
      reason TEXT,
      status TEXT NOT NULL DEFAULT 'HOLD',
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      approved_at TIMESTAMPTZ,
      posted_at TIMESTAMPTZ
    )
  `);
}

async function sendHeartbeatMessage(channelID: string, text: string) {
  const url = `${HEARTBEAT_BASE_URL}/chatChannel/${encodeURIComponent(channelID)}/message`;
  const response = await fetch(url, {
    method: "PUT",
    headers: {
      Authorization: `Bearer ${HEARTBEAT_API_KEY}`,
      Accept: "application/json",
      "Content-Type": "application/json"
    },
    body: JSON.stringify({ text, from: HEARTBEAT_FROM_USER_ID }),
    signal: AbortSignal.timeout(20000)
  });
  if (!response.ok) {
    const body = await response.text();
    throw new Error(`Heartbeat send failed ${response.status}: ${body}`);
  }
}

async function queueReply(clientName: string, draft: string, reason?: string) {
  await ensureQueueTable();
  const matches = await findChatChannels(clientName);
  if (matches.length === 0) throw new Error("No matching Heartbeat CHAT channel found");
  if (matches.length > 1) throw new Error("Client name is ambiguous; use a more specific name");
  const channel = matches[0];
  const result = await pool!.query(
    `INSERT INTO heartbeat_hold_queue (client_name, channel_id, draft, reason)
     VALUES ($1,$2,$3,$4)
     RETURNING *`,
    [channel.name, channel.id, draft, reason || null]
  );
  return result.rows[0];
}

async function listQueue(status?: string) {
  await ensureQueueTable();
  const result = status
    ? await pool!.query(
        `SELECT * FROM heartbeat_hold_queue WHERE status = $1 ORDER BY created_at DESC LIMIT 100`,
        [status]
      )
    : await pool!.query(
        `SELECT * FROM heartbeat_hold_queue ORDER BY created_at DESC LIMIT 100`
      );
  return result.rows;
}

async function heartbeat<T = unknown>(path: string, params?: Record<string, string | number | undefined>): Promise<T> {
  const url = new URL(`${HEARTBEAT_BASE_URL}${path}`);
  for (const [key, value] of Object.entries(params || {})) {
    if (value !== undefined && value !== "") url.searchParams.set(key, String(value));
  }

  const response = await fetch(url, {
    headers: { Authorization: `Bearer ${HEARTBEAT_API_KEY}`, Accept: "application/json" },
    signal: AbortSignal.timeout(20000)
  });

  const text = await response.text();
  let body: unknown;
  try { body = text ? JSON.parse(text) : null; } catch { body = text; }

  if (!response.ok) throw new Error(`Heartbeat ${response.status}: ${typeof body === "string" ? body : JSON.stringify(body)}`);
  return body as T;
}

function normalise(s: string): string {
  return s.trim().toLocaleLowerCase("en-GB").replace(/\s+/g, " ");
}

function decodeHtmlEntities(s: string): string {
  return s
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">")
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/gi, "'")
    .replace(/&#x27;/gi, "'")
    .replace(/&#x2F;/gi, "/");
}

function cleanHeartbeatHtml(html: string): string {
  if (!html) return "";
  return decodeHtmlEntities(
    html
      .replace(/<br\s*\/?\s*>/gi, "\n")
      .replace(/<\/p>/gi, "\n")
      .replace(/<\/li>/gi, "\n")
      .replace(/<li[^>]*>/gi, "• ")
      .replace(/<[^>]+>/g, "")
  )
    .replace(/[\u200B-\u200D\uFEFF]/g, "")
    .replace(/\n{3,}/g, "\n\n")
    .replace(/[ \t]+\n/g, "\n")
    .replace(/^[ \t]+|[ \t]+$/gm, "")
    .trim();
}

function attachmentName(url: string): string {
  try {
    const pathname = new URL(url).pathname;
    return decodeURIComponent(pathname.split("/").pop() || "attachment");
  } catch {
    return "attachment";
  }
}

async function getUserNames(): Promise<Map<string, string>> {
  const now = Date.now();
  if (userCache && now - userCache.at < 5 * 60 * 1000) return userCache.byId;

  const body = await heartbeat<unknown>("/users");
  const users = Array.isArray(body)
    ? body
    : Array.isArray((body as { users?: unknown[] })?.users)
      ? (body as { users: unknown[] }).users
      : [];

  const byId = new Map<string, string>();
  for (const raw of users as HeartbeatUser[]) {
    if (!raw?.id) continue;
    const composed = [raw.firstName, raw.lastName].filter(Boolean).join(" ").trim();
    const display = raw.name || raw.fullName || composed || raw.email || raw.id;
    byId.set(raw.id, String(display));
  }
  userCache = { at: now, byId };
  return byId;
}

async function compactMessages(messages: HeartbeatMessage[]) {
  const names = await getUserNames();
  return messages.map((m) => {
    const files = Array.isArray(m.files) ? m.files : [];
    const images = Array.isArray(m.images) ? m.images : [];
    return {
      id: m.id,
      timestamp: m.createdAt,
      sender: m.userID ? (names.get(m.userID) || m.userID) : "Unknown",
      text: cleanHeartbeatHtml(String(m.content || "")),
      attachments: [
        ...files.map((u) => ({ type: "file", name: attachmentName(u) })),
        ...images.map((u) => ({ type: "image", name: attachmentName(u) }))
      ]
    };
  });
}

async function listChannels(): Promise<HeartbeatChannel[]> {
  const body = await heartbeat<unknown>("/channels");
  if (!Array.isArray(body)) throw new Error("Heartbeat returned an unexpected channel payload");
  return body as HeartbeatChannel[];
}

async function findChatChannels(name: string): Promise<HeartbeatChannel[]> {
  const q = normalise(name);
  const chats = (await listChannels()).filter(c => String(c.type || "").toUpperCase() === "CHAT");
  const exact = chats.filter(c => normalise(String(c.name || "")) === q);
  return exact.length ? exact : chats.filter(c => normalise(String(c.name || "")).includes(q));
}

async function getChatHistory(channelID: string, maxMessages = 500): Promise<HeartbeatMessage[]> {
  const messages: HeartbeatMessage[] = [];
  let startingAfter: string | undefined;

  while (messages.length < maxMessages) {
    const limit = Math.min(100, maxMessages - messages.length);
    const page = await heartbeat<{ data?: HeartbeatMessage[]; hasMore?: boolean }>(
      `/chatChannel/${encodeURIComponent(channelID)}/messages`,
      { startingAfter, limit }
    );
    const data = Array.isArray(page?.data) ? page.data : [];
    messages.push(...data);
    if (!page?.hasMore || data.length === 0) break;
    const last = data[data.length - 1];
    if (!last?.id || last.id === startingAfter) break;
    startingAfter = last.id;
  }
  return messages;
}

function textResult(value: unknown) {
  return { content: [{ type: "text" as const, text: JSON.stringify(value, null, 2) }] };
}

function buildServer() {
  const server = new McpServer({ name: "heartbeat-client-success", version: "0.3.0" }, { capabilities: { tools: {} } });

  server.registerTool("list_heartbeat_channels", {
    description: "List Heartbeat channels. Read-only.",
    inputSchema: z.object({}),
    annotations: { readOnlyHint: true, destructiveHint: false, openWorldHint: false },
    securitySchemes: [{ type: "noauth" }]
  }, async () => textResult({ channels: await listChannels() }));

  server.registerTool("find_client_chat", {
    description: "Find a client's dedicated Heartbeat CHAT channel by name. Read-only.",
    inputSchema: z.object({ name: z.string().min(1) }),
    annotations: { readOnlyHint: true, destructiveHint: false, openWorldHint: false },
    securitySchemes: [{ type: "noauth" }]
  }, async ({ name }) => textResult({ query: name, matches: await findChatChannels(name) }));

  server.registerTool("get_chat_messages", {
    description: "Read a compact Heartbeat CHAT timeline by channel ID. Returns sender names, timestamps, cleaned text and compact attachment metadata. Read-only.",
    inputSchema: z.object({
      channel_id: z.string().uuid(),
      max_messages: z.number().int().min(1).max(2000).default(500)
    }),
    annotations: { readOnlyHint: true, destructiveHint: false, openWorldHint: false },
    securitySchemes: [{ type: "noauth" }]
  }, async ({ channel_id, max_messages }) => {
    const messages = await getChatHistory(channel_id, max_messages);
    const compact = await compactMessages(messages);
    return textResult({ channel_id, count: compact.length, messages: compact });
  });

  server.registerTool("get_client_context", {
    description: "Find a client's named Heartbeat CHAT channel and return a compact, analysis-ready timeline with sender names, timestamps, cleaned text and attachment metadata. Read-only.",
    inputSchema: z.object({
      name: z.string().min(1),
      max_messages: z.number().int().min(1).max(2000).default(500)
    }),
    annotations: { readOnlyHint: true, destructiveHint: false, openWorldHint: false },
    securitySchemes: [{ type: "noauth" }]
  }, async ({ name, max_messages }) => {
    const matches = await findChatChannels(name);
    if (matches.length === 0) return textResult({ name, found: false, message: "No matching CHAT channel found." });
    if (matches.length > 1) return textResult({ name, found: false, ambiguous: true, matches });
    const channel = matches[0];
    const messages = await getChatHistory(channel.id, max_messages);
    const compact = await compactMessages(messages);
    const latest = compact[0]?.timestamp || null;
    const oldest = compact[compact.length - 1]?.timestamp || null;
    return textResult({
      name,
      found: true,
      channel: { id: channel.id, name: channel.name, type: channel.type },
      count: compact.length,
      latest_message_at: latest,
      oldest_message_at: oldest,
      messages: compact
    });
  });

  server.registerTool("queue_client_reply", {
    description: "Save a drafted client reply to the persistent HOLD queue. This never sends a message.",
    inputSchema: z.object({
      name: z.string().min(1),
      draft: z.string().min(1),
      reason: z.string().optional()
    }),
    annotations: { readOnlyHint: false, destructiveHint: false, openWorldHint: false },
    securitySchemes: [{ type: "noauth" }]
  }, async ({ name, draft, reason }) => {
    const item = await queueReply(name, draft, reason);
    return textResult({ queued: true, item });
  });

  server.registerTool("list_hold_queue", {
    description: "List queued Heartbeat reply drafts. Defaults to HOLD items.",
    inputSchema: z.object({
      status: z.enum(["HOLD","APPROVED","POSTED","SKIPPED"]).default("HOLD")
    }),
    annotations: { readOnlyHint: true, destructiveHint: false, openWorldHint: false },
    securitySchemes: [{ type: "noauth" }]
  }, async ({ status }) => textResult({ status, items: await listQueue(status) }));

  server.registerTool("edit_hold_item", {
    description: "Edit a queued draft or reason. Editing resets the item to HOLD so it must be approved again.",
    inputSchema: z.object({
      id: z.number().int().positive(),
      draft: z.string().min(1).optional(),
      reason: z.string().optional()
    }),
    annotations: { readOnlyHint: false, destructiveHint: false, openWorldHint: false },
    securitySchemes: [{ type: "noauth" }]
  }, async ({ id, draft, reason }) => {
    await ensureQueueTable();
    const current = await pool!.query(`SELECT * FROM heartbeat_hold_queue WHERE id=$1`, [id]);
    if (!current.rowCount) throw new Error("Queue item not found");
    const nextDraft = draft ?? current.rows[0].draft;
    const nextReason = reason ?? current.rows[0].reason;
    const result = await pool!.query(
      `UPDATE heartbeat_hold_queue
       SET draft=$2, reason=$3, status='HOLD', approved_at=NULL, updated_at=NOW()
       WHERE id=$1 RETURNING *`,
      [id, nextDraft, nextReason]
    );
    return textResult({ updated: true, item: result.rows[0] });
  });

  server.registerTool("approve_hold_item", {
    description: "Approve one HOLD draft for posting. This does not send it yet.",
    inputSchema: z.object({ id: z.number().int().positive() }),
    annotations: { readOnlyHint: false, destructiveHint: false, openWorldHint: false },
    securitySchemes: [{ type: "noauth" }]
  }, async ({ id }) => {
    await ensureQueueTable();
    const result = await pool!.query(
      `UPDATE heartbeat_hold_queue
       SET status='APPROVED', approved_at=NOW(), updated_at=NOW()
       WHERE id=$1 AND status='HOLD' RETURNING *`,
      [id]
    );
    if (!result.rowCount) throw new Error("Item must exist and be in HOLD status");
    return textResult({ approved: true, item: result.rows[0] });
  });

  server.registerTool("skip_hold_item", {
    description: "Mark a HOLD or APPROVED queue item as SKIPPED. This never sends it.",
    inputSchema: z.object({ id: z.number().int().positive() }),
    annotations: { readOnlyHint: false, destructiveHint: false, openWorldHint: false },
    securitySchemes: [{ type: "noauth" }]
  }, async ({ id }) => {
    await ensureQueueTable();
    const result = await pool!.query(
      `UPDATE heartbeat_hold_queue
       SET status='SKIPPED', updated_at=NOW()
       WHERE id=$1 AND status IN ('HOLD','APPROVED') RETURNING *`,
      [id]
    );
    if (!result.rowCount) throw new Error("Item must exist and be HOLD or APPROVED");
    return textResult({ skipped: true, item: result.rows[0] });
  });

  server.registerTool("post_approved_reply", {
    description: "Post the exact draft from an APPROVED queue item to Heartbeat, then mark it POSTED. Refuses HOLD drafts.",
    inputSchema: z.object({ id: z.number().int().positive() }),
    annotations: { readOnlyHint: false, destructiveHint: true, openWorldHint: true },
    securitySchemes: [{ type: "noauth" }]
  }, async ({ id }) => {
    await ensureQueueTable();
    const current = await pool!.query(
      `SELECT * FROM heartbeat_hold_queue WHERE id=$1 AND status='APPROVED'`,
      [id]
    );
    if (!current.rowCount) throw new Error("Item must be explicitly APPROVED before posting");
    const item = current.rows[0];
    await sendHeartbeatMessage(item.channel_id, item.draft);
    const result = await pool!.query(
      `UPDATE heartbeat_hold_queue
       SET status='POSTED', posted_at=NOW(), updated_at=NOW()
       WHERE id=$1 RETURNING *`,
      [id]
    );
    return textResult({ posted: true, item: result.rows[0] });
  });

  return server;
}

const mcpHandler = createMcpHandler(buildServer);
const app = createMcpExpressApp({ host: "0.0.0.0" });
app.get("/health", (_req, res) => res.json({ ok: true, service: "heartbeat-client-success-mcp", version: "0.3.0" }));
const nodeHandler = toNodeHandler(mcpHandler);
app.all(`/mcp/${MCP_PATH_TOKEN}`, (req, res) => void nodeHandler(req, res, req.body));
if (pool) {
  ensureQueueTable()
    .then(() => console.log("Heartbeat hold queue ready"))
    .catch((err) => console.error("Heartbeat hold queue init failed", err));
}
app.listen(PORT, "0.0.0.0", () => console.log(`Heartbeat MCP listening on ${PORT}`));
