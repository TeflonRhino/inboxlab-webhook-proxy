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
const DASHBOARD_USER = process.env.DASHBOARD_USER || "dave";
const DASHBOARD_PASSWORD = process.env.DASHBOARD_PASSWORD;
const DASHBOARD_SESSION_TOKEN = process.env.DASHBOARD_SESSION_TOKEN;
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
      channel_id UUID,
      channel_url TEXT,
      draft TEXT NOT NULL,
      reason TEXT,
      status TEXT NOT NULL DEFAULT 'HOLD',
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      approved_at TIMESTAMPTZ,
      posted_at TIMESTAMPTZ
    );
    ALTER TABLE heartbeat_hold_queue ALTER COLUMN channel_id DROP NOT NULL;
    ALTER TABLE heartbeat_hold_queue ADD COLUMN IF NOT EXISTS channel_url TEXT;
  `);
}

async function ensureOpsTables() {
  if (!pool) throw new Error("DATABASE_URL is not configured");
  await pool.query(`
    CREATE TABLE IF NOT EXISTS dashboard_activity (
      id BIGSERIAL PRIMARY KEY,
      event_type TEXT NOT NULL,
      title TEXT NOT NULL,
      detail TEXT,
      client_name TEXT,
      entity_type TEXT,
      entity_id TEXT,
      actor TEXT NOT NULL DEFAULT 'system',
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS dashboard_items (
      id BIGSERIAL PRIMARY KEY,
      category TEXT NOT NULL,
      client_name TEXT,
      title TEXT NOT NULL,
      summary TEXT,
      priority TEXT NOT NULL DEFAULT 'NORMAL',
      status TEXT NOT NULL DEFAULT 'OPEN',
      source TEXT NOT NULL DEFAULT 'ChatGPT',
      due_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS dashboard_items_category_status_idx ON dashboard_items(category,status);
  `);
}

async function logActivity(input: {
  eventType: string;
  title: string;
  detail?: string | null;
  clientName?: string | null;
  entityType?: string | null;
  entityId?: string | number | null;
  actor?: string;
}) {
  await ensureOpsTables();
  await pool!.query(
    `INSERT INTO dashboard_activity (event_type,title,detail,client_name,entity_type,entity_id,actor)
     VALUES ($1,$2,$3,$4,$5,$6,$7)`,
    [input.eventType, input.title, input.detail || null, input.clientName || null, input.entityType || null, input.entityId == null ? null : String(input.entityId), input.actor || "system"]
  );
}

async function listActivity(limit = 60) {
  await ensureOpsTables();
  const result = await pool!.query(
    `SELECT * FROM dashboard_activity ORDER BY created_at DESC LIMIT $1`,
    [Math.max(1, Math.min(200, limit))]
  );
  return result.rows;
}

async function listDashboardItems(category?: string, status = "OPEN") {
  await ensureOpsTables();
  const values: unknown[] = [];
  const where: string[] = [];
  if (category) { values.push(category); where.push("category=$" + values.length); }
  if (status) { values.push(status); where.push("status=$" + values.length); }
  const sql = `SELECT * FROM dashboard_items ${where.length ? "WHERE " + where.join(" AND ") : ""}
               ORDER BY CASE priority WHEN 'URGENT' THEN 1 WHEN 'HIGH' THEN 2 WHEN 'NORMAL' THEN 3 ELSE 4 END,
                        COALESCE(due_at, '2999-12-31'::timestamptz), updated_at DESC
               LIMIT 200`;
  return (await pool!.query(sql, values)).rows;
}

async function createDashboardItem(input: {
  category: string;
  clientName?: string;
  title: string;
  summary?: string;
  priority?: string;
  source?: string;
  dueAt?: string;
}) {
  await ensureOpsTables();
  const result = await pool!.query(
    `INSERT INTO dashboard_items (category,client_name,title,summary,priority,source,due_at)
     VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING *`,
    [input.category, input.clientName || null, input.title, input.summary || null, input.priority || "NORMAL", input.source || "ChatGPT", input.dueAt || null]
  );
  const item = result.rows[0];
  await logActivity({ eventType: "dashboard_item_created", title: `${item.category}: ${item.title}`, detail: item.summary, clientName: item.client_name, entityType: "dashboard_item", entityId: item.id, actor: "ChatGPT" });
  return item;
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
  const item = result.rows[0];
  await logActivity({ eventType: "hold_created", title: "Heartbeat draft added to Hold Queue", detail: draft, clientName: channel.name, entityType: "hold_queue", entityId: item.id, actor: "ChatGPT" });
  return item;
}

async function queuePendingHeartbeatReply(clientName: string, draft: string, reason?: string) {
  await ensureQueueTable();
  const result = await pool!.query(
    `INSERT INTO heartbeat_hold_queue (client_name, channel_id, channel_url, draft, reason, status)
     VALUES ($1,NULL,NULL,$2,$3,'HOLD') RETURNING *`,
    [clientName, draft, reason || null]
  );
  const item = result.rows[0];
  await logActivity({ eventType: "hold_created_pending_channel", title: "Heartbeat draft waiting for chat", detail: draft, clientName, entityType: "hold_queue", entityId: item.id, actor: "ChatGPT" });
  return item;
}

function extractHeartbeatChannelId(value: string): string | null {
  const match = value.match(/[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-8][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}/);
  return match ? match[0] : null;
}

async function attachHeartbeatChannel(id: number, url: string) {
  await ensureQueueTable();
  const channelId = extractHeartbeatChannelId(url);
  if (!channelId) throw new Error("Could not find a Heartbeat channel ID in that URL");
  const result = await pool!.query(
    `UPDATE heartbeat_hold_queue
     SET channel_id=$2, channel_url=$3, status='HOLD', approved_at=NULL, updated_at=NOW()
     WHERE id=$1 AND status IN ('HOLD','APPROVED') RETURNING *`,
    [id, channelId, url]
  );
  if (!result.rowCount) throw new Error("Queue item must exist and be HOLD or APPROVED");
  const item = result.rows[0];
  await logActivity({ eventType: "heartbeat_channel_attached", title: "Heartbeat chat attached to draft", detail: url, clientName: item.client_name, entityType: "hold_queue", entityId: item.id, actor: "Dave" });
  return item;
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
    if (normalise(name) === "__pending_heartbeat__") {
      let command: any;
      try { command = JSON.parse(draft); } catch { throw new Error("Pending Heartbeat command must be valid JSON"); }
      const clientName = String(command.client_name || "").trim();
      const message = String(command.draft || "").trim();
      if (!clientName || !message) throw new Error("client_name and draft are required");
      const item = await queuePendingHeartbeatReply(clientName, message, command.reason ? String(command.reason) : reason);
      return textResult({ queued: true, pending_channel: true, item });
    }
    if (normalise(name) === "__dashboard__") {
      const [queue, items, activity] = await Promise.all([listQueue(), listDashboardItems(undefined, "OPEN"), listActivity(Math.min(max_messages, 200))]);
      return textResult({ dashboard: true, queue, items, activity });
    }
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
    if (normalise(name) === "__dashboard__") {
      let command: any;
      try { command = JSON.parse(draft); } catch { throw new Error("Dashboard command must be valid JSON"); }
      if (command?.action === "add") {
        const category = String(command.category || "");
        if (!["CLIENT_ATTENTION","COLLECTION","REFUND","CALL","FOLLOW_UP","ONBOARDING"].includes(category)) throw new Error("Invalid dashboard category");
        const item = await createDashboardItem({
          category,
          clientName: command.client_name ? String(command.client_name) : undefined,
          title: String(command.title || "").trim(),
          summary: command.summary ? String(command.summary) : undefined,
          priority: ["URGENT","HIGH","NORMAL","LOW"].includes(String(command.priority)) ? String(command.priority) : "NORMAL",
          source: command.source ? String(command.source) : "ChatGPT",
          dueAt: command.due_at ? String(command.due_at) : undefined
        });
        if (!item.title) throw new Error("Dashboard item title is required");
        return textResult({ dashboard: true, action: "add", item });
      }
      if (command?.action === "resolve") {
        await ensureOpsTables();
        const id = Number(command.id);
        const status = command.status === "DISMISSED" ? "DISMISSED" : "DONE";
        if (!Number.isInteger(id) || id <= 0) throw new Error("Valid dashboard item id is required");
        const result = await pool!.query(
          `UPDATE dashboard_items SET status=$2, updated_at=NOW() WHERE id=$1 AND status='OPEN' RETURNING *`,
          [id, status]
        );
        if (!result.rowCount) throw new Error("Dashboard item must exist and be OPEN");
        const item = result.rows[0];
        await logActivity({ eventType: "dashboard_item_resolved", title: `${status}: ${item.title}`, detail: item.summary, clientName: item.client_name, entityType: "dashboard_item", entityId: item.id, actor: "ChatGPT" });
        return textResult({ dashboard: true, action: "resolve", item });
      }
      throw new Error("Unsupported dashboard action");
    }
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

  server.registerTool("list_dashboard_items", {
    description: "List visual InboxLab dashboard items surfaced by ChatGPT, such as client attention, collections, refunds, calls and follow-ups. Read-only.",
    inputSchema: z.object({
      category: z.enum(["CLIENT_ATTENTION","COLLECTION","REFUND","CALL","FOLLOW_UP","ONBOARDING"]).optional(),
      status: z.enum(["OPEN","DONE","DISMISSED"]).default("OPEN")
    }),
    annotations: { readOnlyHint: true, destructiveHint: false, openWorldHint: false },
    securitySchemes: [{ type: "noauth" }]
  }, async ({ category, status }) => textResult({ items: await listDashboardItems(category, status) }));

  server.registerTool("add_dashboard_item", {
    description: "Add one concise item to the visual InboxLab Command Centre after ChatGPT finds something worth surfacing from connected work systems.",
    inputSchema: z.object({
      category: z.enum(["CLIENT_ATTENTION","COLLECTION","REFUND","CALL","FOLLOW_UP","ONBOARDING"]),
      client_name: z.string().optional(),
      title: z.string().min(1),
      summary: z.string().optional(),
      priority: z.enum(["URGENT","HIGH","NORMAL","LOW"]).default("NORMAL"),
      source: z.string().default("ChatGPT"),
      due_at: z.string().optional()
    }),
    annotations: { readOnlyHint: false, destructiveHint: false, openWorldHint: false },
    securitySchemes: [{ type: "noauth" }]
  }, async ({ category, client_name, title, summary, priority, source, due_at }) => {
    const item = await createDashboardItem({ category, clientName: client_name, title, summary, priority, source, dueAt: due_at });
    return textResult({ created: true, item });
  });

  server.registerTool("resolve_dashboard_item", {
    description: "Mark a visual InboxLab dashboard item as DONE or DISMISSED.",
    inputSchema: z.object({
      id: z.number().int().positive(),
      status: z.enum(["DONE","DISMISSED"]).default("DONE")
    }),
    annotations: { readOnlyHint: false, destructiveHint: false, openWorldHint: false },
    securitySchemes: [{ type: "noauth" }]
  }, async ({ id, status }) => {
    await ensureOpsTables();
    const result = await pool!.query(
      `UPDATE dashboard_items SET status=$2, updated_at=NOW() WHERE id=$1 AND status='OPEN' RETURNING *`,
      [id, status]
    );
    if (!result.rowCount) throw new Error("Dashboard item must exist and be OPEN");
    const item = result.rows[0];
    await logActivity({ eventType: "dashboard_item_resolved", title: `${status}: ${item.title}`, detail: item.summary, clientName: item.client_name, entityType: "dashboard_item", entityId: item.id, actor: "ChatGPT" });
    return textResult({ updated: true, item });
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
    if (!item.channel_id) throw new Error("Heartbeat chat is not attached yet");
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

function readCookie(req: express.Request, name: string) {
  const raw = req.headers.cookie || "";
  for (const part of raw.split(";")) {
    const [key, ...rest] = part.trim().split("=");
    if (key === name) return decodeURIComponent(rest.join("="));
  }
  return null;
}

function isDashboardAuthed(req: express.Request) {
  return Boolean(DASHBOARD_SESSION_TOKEN && readCookie(req, "inboxlab_ops_session") === DASHBOARD_SESSION_TOKEN);
}

function requireDashboardAuth(req: express.Request, res: express.Response, next: express.NextFunction) {
  if (isDashboardAuthed(req)) return next();
  if (req.path.startsWith("/api/")) return res.status(401).json({ error: "Not authenticated" });
  return res.redirect("/dashboard/login");
}

app.use("/dashboard/api", express.json());
app.use("/dashboard/login", express.urlencoded({ extended: false }));

app.get("/dashboard/login", (req, res) => {
  if (isDashboardAuthed(req)) return res.redirect("/dashboard");
  res.type("html").send(`<!doctype html>
<html><head><meta name="viewport" content="width=device-width,initial-scale=1"><title>InboxLab Command Centre</title>
<style>
:root{font-family:Inter,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#141414;background:#f5f5f4}
*{box-sizing:border-box}body{margin:0;min-height:100vh;display:grid;place-items:center;padding:24px}
.card{width:min(420px,100%);background:#fff;border:1px solid #e7e5e4;border-radius:22px;padding:28px;box-shadow:0 18px 60px rgba(0,0,0,.08)}
.brand{font-weight:800;font-size:13px;letter-spacing:.12em;text-transform:uppercase;color:#57534e}.title{font-size:28px;font-weight:800;margin:10px 0 6px}.muted{color:#78716c;font-size:14px;line-height:1.5;margin-bottom:22px}
label{font-size:13px;font-weight:700;display:block;margin:14px 0 7px}input{width:100%;font:inherit;padding:13px 14px;border:1px solid #d6d3d1;border-radius:12px;outline:none}input:focus{border-color:#292524;box-shadow:0 0 0 3px rgba(41,37,36,.08)}
button{margin-top:18px;width:100%;border:0;border-radius:12px;padding:13px 16px;background:#1c1917;color:white;font:inherit;font-weight:750;cursor:pointer}.error{background:#fef2f2;color:#991b1b;padding:10px 12px;border-radius:10px;font-size:13px;margin-bottom:12px}
</style></head><body><main class="card"><div class="brand">InboxLab</div><div class="title">Command Centre</div><div class="muted">Private visual workspace for Dave + ChatGPT.</div>
${req.query.error ? '<div class="error">Incorrect username or password.</div>' : ''}
<form method="post" action="/dashboard/login"><label>Username</label><input name="username" autocomplete="username" required value="dave"><label>Password</label><input type="password" name="password" autocomplete="current-password" required><button type="submit">Open dashboard</button></form></main></body></html>`);
});

app.post("/dashboard/login", (req, res) => {
  if (!DASHBOARD_PASSWORD || !DASHBOARD_SESSION_TOKEN) return res.status(503).send("Dashboard authentication is not configured.");
  if (req.body?.username !== DASHBOARD_USER || req.body?.password !== DASHBOARD_PASSWORD) return res.redirect("/dashboard/login?error=1");
  res.setHeader("Set-Cookie", `inboxlab_ops_session=${encodeURIComponent(DASHBOARD_SESSION_TOKEN)}; Path=/dashboard; HttpOnly; Secure; SameSite=Lax; Max-Age=604800`);
  return res.redirect("/dashboard");
});

app.post("/dashboard/logout", (_req, res) => {
  res.setHeader("Set-Cookie", "inboxlab_ops_session=; Path=/dashboard; HttpOnly; Secure; SameSite=Lax; Max-Age=0");
  return res.redirect("/dashboard/login");
});

app.get("/dashboard", requireDashboardAuth, (_req, res) => {
  res.sendFile("dashboard.html", { root: process.cwd() + "/public" });
});

app.get("/dashboard/api/overview", requireDashboardAuth, async (_req, res) => {
  try {
    const [queue, items, activity] = await Promise.all([listQueue(), listDashboardItems(undefined, "OPEN"), listActivity(80)]);
    res.json({ queue, items, activity });
  } catch (err) {
    res.status(500).json({ error: err instanceof Error ? err.message : "Failed to load dashboard" });
  }
});

app.post("/dashboard/api/items/:id/resolve", requireDashboardAuth, async (req, res) => {
  try {
    await ensureOpsTables();
    const id = Number(req.params.id);
    const status = req.body?.status === "DISMISSED" ? "DISMISSED" : "DONE";
    const result = await pool!.query(`UPDATE dashboard_items SET status=$2, updated_at=NOW() WHERE id=$1 AND status='OPEN' RETURNING *`, [id, status]);
    if (!result.rowCount) return res.status(409).json({ error: "Item must be OPEN" });
    const item = result.rows[0];
    await logActivity({ eventType: "dashboard_item_resolved", title: `${status}: ${item.title}`, detail: item.summary, clientName: item.client_name, entityType: "dashboard_item", entityId: item.id, actor: "Dave" });
    res.json({ item });
  } catch (err) {
    res.status(500).json({ error: err instanceof Error ? err.message : "Failed to update item" });
  }
});

app.get("/dashboard/api/hold-queue", requireDashboardAuth, async (req, res) => {
  try {
    const status = typeof req.query.status === "string" && req.query.status ? req.query.status : undefined;
    const items = await listQueue(status);
    res.json({ items });
  } catch (err) {
    res.status(500).json({ error: err instanceof Error ? err.message : "Failed to load queue" });
  }
});

app.patch("/dashboard/api/hold-queue/:id", requireDashboardAuth, async (req, res) => {
  try {
    await ensureQueueTable();
    const id = Number(req.params.id);
    if (!Number.isInteger(id) || id <= 0) return res.status(400).json({ error: "Invalid id" });
    const current = await pool!.query(`SELECT * FROM heartbeat_hold_queue WHERE id=$1`, [id]);
    if (!current.rowCount) return res.status(404).json({ error: "Queue item not found" });
    const draft = typeof req.body?.draft === "string" && req.body.draft.trim() ? req.body.draft.trim() : current.rows[0].draft;
    const reason = typeof req.body?.reason === "string" ? req.body.reason.trim() : current.rows[0].reason;
    const result = await pool!.query(
      `UPDATE heartbeat_hold_queue SET draft=$2, reason=$3, status='HOLD', approved_at=NULL, updated_at=NOW() WHERE id=$1 RETURNING *`,
      [id, draft, reason || null]
    );
    const item = result.rows[0];
    await logActivity({ eventType: "hold_edited", title: "Hold draft edited", detail: item.draft, clientName: item.client_name, entityType: "hold_queue", entityId: item.id, actor: "Dave" });
    res.json({ item });
  } catch (err) {
    res.status(500).json({ error: err instanceof Error ? err.message : "Failed to update queue item" });
  }
});

app.post("/dashboard/api/hold-queue/:id/attach-channel", requireDashboardAuth, async (req, res) => {
  try {
    const id = Number(req.params.id);
    const url = typeof req.body?.url === "string" ? req.body.url.trim() : "";
    if (!Number.isInteger(id) || id <= 0) return res.status(400).json({ error: "Invalid id" });
    if (!url) return res.status(400).json({ error: "Heartbeat chat URL is required" });
    const item = await attachHeartbeatChannel(id, url);
    res.json({ item });
  } catch (err) {
    res.status(500).json({ error: err instanceof Error ? err.message : "Failed to attach Heartbeat chat" });
  }
});

app.post("/dashboard/api/hold-queue/:id/approve", requireDashboardAuth, async (req, res) => {
  try {
    await ensureQueueTable();
    const id = Number(req.params.id);
    const result = await pool!.query(
      `UPDATE heartbeat_hold_queue SET status='APPROVED', approved_at=NOW(), updated_at=NOW() WHERE id=$1 AND status='HOLD' RETURNING *`,
      [id]
    );
    if (!result.rowCount) return res.status(409).json({ error: "Item must be in HOLD status" });
    const item = result.rows[0];
    await logActivity({ eventType: "hold_approved", title: "Heartbeat draft approved", detail: item.draft, clientName: item.client_name, entityType: "hold_queue", entityId: item.id, actor: "Dave" });
    res.json({ item });
  } catch (err) {
    res.status(500).json({ error: err instanceof Error ? err.message : "Failed to approve queue item" });
  }
});

app.post("/dashboard/api/hold-queue/:id/skip", requireDashboardAuth, async (req, res) => {
  try {
    await ensureQueueTable();
    const id = Number(req.params.id);
    const result = await pool!.query(
      `UPDATE heartbeat_hold_queue SET status='SKIPPED', updated_at=NOW() WHERE id=$1 AND status IN ('HOLD','APPROVED') RETURNING *`,
      [id]
    );
    if (!result.rowCount) return res.status(409).json({ error: "Item must be HOLD or APPROVED" });
    const item = result.rows[0];
    await logActivity({ eventType: "hold_skipped", title: "Heartbeat draft skipped", detail: item.draft, clientName: item.client_name, entityType: "hold_queue", entityId: item.id, actor: "Dave" });
    res.json({ item });
  } catch (err) {
    res.status(500).json({ error: err instanceof Error ? err.message : "Failed to skip queue item" });
  }
});

app.post("/dashboard/api/hold-queue/:id/post", requireDashboardAuth, async (req, res) => {
  try {
    await ensureQueueTable();
    const id = Number(req.params.id);
    const current = await pool!.query(`SELECT * FROM heartbeat_hold_queue WHERE id=$1 AND status='APPROVED'`, [id]);
    if (!current.rowCount) return res.status(409).json({ error: "Item must be explicitly APPROVED before posting" });
    const item = current.rows[0];
    await sendHeartbeatMessage(item.channel_id, item.draft);
    const result = await pool!.query(
      `UPDATE heartbeat_hold_queue SET status='POSTED', posted_at=NOW(), updated_at=NOW() WHERE id=$1 RETURNING *`,
      [id]
    );
    const posted = result.rows[0];
    await logActivity({ eventType: "hold_posted", title: "Heartbeat reply posted", detail: posted.draft, clientName: posted.client_name, entityType: "hold_queue", entityId: posted.id, actor: "Dave" });
    res.json({ item: posted });
  } catch (err) {
    res.status(500).json({ error: err instanceof Error ? err.message : "Failed to post reply" });
  }
});
app.get("/health", (_req, res) => res.json({ ok: true, service: "heartbeat-client-success-mcp", version: "0.3.0" }));
const nodeHandler = toNodeHandler(mcpHandler);
app.all(`/mcp/${MCP_PATH_TOKEN}`, (req, res) => void nodeHandler(req, res, req.body));
if (pool) {
  Promise.all([ensureQueueTable(), ensureOpsTables()])
    .then(() => console.log("Heartbeat hold queue and dashboard ops store ready"))
    .catch((err) => console.error("Heartbeat data init failed", err));
}
app.listen(PORT, "0.0.0.0", () => console.log(`Heartbeat MCP listening on ${PORT}`));
