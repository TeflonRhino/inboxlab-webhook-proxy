import express from "express";
import { McpServer } from "@modelcontextprotocol/server";
import { createMcpHandler } from "@modelcontextprotocol/server";
import { createMcpExpressApp } from "@modelcontextprotocol/express";
import { toNodeHandler } from "@modelcontextprotocol/node";
import * as z from "zod/v4";
import pg from "pg";
import { randomUUID } from "node:crypto";

const HEARTBEAT_BASE_URL = "https://api.heartbeat.chat/v0";
const HEARTBEAT_API_KEY = process.env.HEARTBEAT_API_KEY;
const HEARTBEAT_WEB_TOKEN = process.env.HEARTBEAT_WEB_TOKEN;
const CALENDLY_ACCESS_TOKEN = process.env.CALENDLY_ACCESS_TOKEN;
const FATHOM_API_KEY = process.env.FATHOM_API_KEY;
const SLACK_BOT_TOKEN = process.env.SLACK_BOT_TOKEN;
const MCP_PATH_TOKEN = process.env.MCP_PATH_TOKEN;
const PORT = Number(process.env.PORT || 3000);
const DATABASE_URL = process.env.DATABASE_URL;
const HEARTBEAT_FROM_USER_ID = process.env.HEARTBEAT_FROM_USER_ID || "d8a956be-e3e4-4ef1-9978-6f1b448d3cba";
const HEARTBEAT_COMMUNITY_ID = process.env.HEARTBEAT_COMMUNITY_ID || "f81e70fc-d08b-43bd-b297-3e27a8f202a8";
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
let calendlyEventTypeCache: {
  at: number;
  user: { name: string; scheduling_url: string };
  event_types: Array<{ uri: string; name: string; duration: number | null; scheduling_url: string; kind: string | null }>;
} | null = null;
const CALENDLY_EVENT_TYPE_CACHE_MS = 5 * 60 * 1000;

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
    ALTER TABLE dashboard_items ADD COLUMN IF NOT EXISTS metadata JSONB NOT NULL DEFAULT '{}'::jsonb;
    CREATE INDEX IF NOT EXISTS dashboard_items_category_status_idx ON dashboard_items(category,status);

    CREATE TABLE IF NOT EXISTS dashboard_actions (
      id BIGSERIAL PRIMARY KEY,
      action_type TEXT NOT NULL,
      payload JSONB NOT NULL DEFAULT '{}'::jsonb,
      status TEXT NOT NULL DEFAULT 'PENDING',
      requested_by TEXT NOT NULL DEFAULT 'Dave',
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      completed_at TIMESTAMPTZ,
      result_note TEXT
    );
    CREATE INDEX IF NOT EXISTS dashboard_actions_status_idx ON dashboard_actions(status,created_at);

    CREATE TABLE IF NOT EXISTS dashboard_call_overrides (
      id BIGSERIAL PRIMARY KEY,
      calendly_invitee_uri TEXT NOT NULL UNIQUE,
      calendly_event_uri TEXT,
      client_name TEXT,
      call_title TEXT,
      call_start_time TIMESTAMPTZ,
      status TEXT NOT NULL,
      note TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS dashboard_call_overrides_start_idx ON dashboard_call_overrides(call_start_time,status);
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
  metadata?: Record<string, unknown>;
}) {
  await ensureOpsTables();
  const result = await pool!.query(
    `INSERT INTO dashboard_items (category,client_name,title,summary,priority,source,due_at,metadata)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb) RETURNING *`,
    [input.category, input.clientName || null, input.title, input.summary || null, input.priority || "NORMAL", input.source || "ChatGPT", input.dueAt || null, JSON.stringify(input.metadata || {})]
  );
  const item = result.rows[0];
  await logActivity({ eventType: "dashboard_item_created", title: `${item.category}: ${item.title}`, detail: item.summary, clientName: item.client_name, entityType: "dashboard_item", entityId: item.id, actor: "ChatGPT" });
  return item;
}

async function listHeartbeatUsers(): Promise<Array<{ id: string; name: string }>> {
  const body = await heartbeat<unknown>("/users");
  const users = Array.isArray(body)
    ? body
    : Array.isArray((body as { users?: unknown[] })?.users)
      ? (body as { users: unknown[] }).users
      : [];

  return (users as HeartbeatUser[])
    .filter((raw) => Boolean(raw?.id))
    .map((raw) => {
      const composed = [raw.firstName, raw.lastName].filter(Boolean).join(" ").trim();
      return { id: String(raw.id), name: String(raw.name || raw.fullName || composed || raw.email || raw.id) };
    });
}

function escapeHeartbeatHtml(value: string) {
  return value
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

async function formatHeartbeatMessage(text: string) {
  const users = await listHeartbeatUsers();
  const byLength = users
    .filter((u) => u.name && u.id)
    .sort((a, b) => b.name.length - a.name.length);

  let html = escapeHeartbeatHtml(text);

  for (const user of byLength) {
    const literal = "@" + escapeHeartbeatHtml(user.name);
    if (!html.includes(literal)) continue;
    const mention = `<span class="reference" data-index="0" data-denotation-char="@" data-id="mention.user.${escapeHeartbeatHtml(user.id)}" data-value="${escapeHeartbeatHtml(user.name)}">&#65279;<span contenteditable="false"><span class="user-reference"><span class="ql-mention-denotation-char">@</span>${escapeHeartbeatHtml(user.name)}</span></span>&#65279;</span>`;
    html = html.split(literal).join(mention);
  }

  html = html.replace(
    /\bhttps?:\/\/[^\s<]+/gi,
    (url) => `<a href="${url}" target="_blank" rel="noopener noreferrer">${url}</a>`
  );

  return `<p>${html.replace(/\n/g, "<br>")}</p>`;
}

async function sendHeartbeatMessage(channelID: string, text: string) {
  if (!HEARTBEAT_WEB_TOKEN) throw new Error("HEARTBEAT_WEB_TOKEN is required to send Heartbeat messages");
  const message = text.trim();
  if (!message) throw new Error("Heartbeat message cannot be empty");

  const htmlMessage = await formatHeartbeatMessage(message);

  const response = await fetch("https://api.heartbeat.chat/trpc/createChatMessage?batch=1", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${HEARTBEAT_WEB_TOKEN}`,
      Accept: "application/json",
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      "0": {
        json: {
          communityID: HEARTBEAT_COMMUNITY_ID,
          chatID: channelID,
          clientGeneratedID: randomUUID(),
          message: htmlMessage
        }
      }
    }),
    signal: AbortSignal.timeout(20000)
  });

  const body = await response.text();
  if (!response.ok) {
    if (response.status === 401) {
      throw new Error("Heartbeat web token expired or is invalid — update HEARTBEAT_WEB_TOKEN in Render, then retry.");
    }
    throw new Error(`Heartbeat send failed ${response.status}: ${body}`);
  }
}
function regexEscape(value: string) {
  return value.replace(/[.*+?^${}()|[]\\]/g, "\\function regexEscape(value: string) {
  return value.replace(/[.*+?^$\{\}()|[\]\\]/g, "\\async function queueReply(clientName: string, draft: string, reason?: string) {");
}");
}

async function addClientMentionToDraft(clientName: string, draft: string) {
  const cleanName = String(clientName || "").trim();
  const cleanDraft = String(draft || "").trim();
  if (!cleanName || !cleanDraft) return cleanDraft;

  let mentionName = cleanName;
  try {
    const users = await listHeartbeatUsers();
    const target = normalise(cleanName);
    const matches = users
      .filter((u) => {
        const n = normalise(u.name || "");
        return n && (n === target || n.startsWith(target) || target.startsWith(n));
      })
      .sort((a, b) => String(b.name || "").length - String(a.name || "").length);
    if (matches[0]?.name) mentionName = matches[0].name;
  } catch {
    // Keep the client name fallback; send-time formatting will only create a native
    // Heartbeat mention when the resolved name matches a real Heartbeat user.
  }

  if (cleanDraft.includes("@" + mentionName)) return cleanDraft;

  const firstName = mentionName.split(/\s+/)[0];
  const candidates = [mentionName, firstName].filter(Boolean);
  for (const candidate of candidates) {
    const re = new RegExp("(^|[^@A-Za-z0-9_])(" + regexEscape(candidate) + ")(?=[^A-Za-z0-9_]|$)", "i");
    if (re.test(cleanDraft)) {
      return cleanDraft.replace(re, (_match, prefix) => prefix + "@" + mentionName);
    }
  }

  return cleanDraft;
}

async function queueReply(clientName: string, draft: string, reason?: string) {
  await ensureQueueTable();
  const matches = await findChatChannels(clientName);
  if (matches.length === 0) throw new Error("No matching Heartbeat CHAT channel found");
  if (matches.length > 1) throw new Error("Client name is ambiguous; use a more specific name");
  const channel = matches[0];
  const taggedDraft = await addClientMentionToDraft(channel.name, draft);
  const result = await pool!.query(
    `INSERT INTO heartbeat_hold_queue (client_name, channel_id, draft, reason)
     VALUES ($1,$2,$3,$4)
     RETURNING *`,
    [channel.name, channel.id, taggedDraft, reason || null]
  );
  const item = result.rows[0];
  await logActivity({ eventType: "hold_created", title: "Heartbeat draft added to Hold Queue", detail: taggedDraft, clientName: channel.name, entityType: "hold_queue", entityId: item.id, actor: "ChatGPT" });
  return item;
}

async function queuePendingHeartbeatReply(clientName: string, draft: string, reason?: string) {
  await ensureQueueTable();
  const taggedDraft = await addClientMentionToDraft(clientName, draft);
  const result = await pool!.query(
    `INSERT INTO heartbeat_hold_queue (client_name, channel_id, channel_url, draft, reason, status)
     VALUES ($1,NULL,NULL,$2,$3,'HOLD') RETURNING *`,
    [clientName, taggedDraft, reason || null]
  );
  const item = result.rows[0];
  await logActivity({ eventType: "hold_created_pending_channel", title: "Heartbeat draft waiting for chat", detail: taggedDraft, clientName, entityType: "hold_queue", entityId: item.id, actor: "ChatGPT" });
  return item;
}


type CaseDraftStrategy = {
  key: string;
  label: string;
  objective: string;
  draft: string;
};

function firstName(value: string) {
  return String(value || "").trim().split(/\s+/)[0] || "there";
}

function recentHeartbeatText(messages: Array<{ text?: string }>) {
  return messages.slice(0, 12).map((m) => String(m?.text || "")).join("\n ").toLowerCase();
}

function buildCaseDraftStrategy(item: any, recentMessages: Array<{ text?: string }> = []): CaseDraftStrategy {
  const name = String(item?.client_name || "there").trim();
  const first = firstName(name);
  const context = [
    item?.title || "",
    item?.summary || "",
    item?.source || "",
    recentHeartbeatText(recentMessages)
  ].join("\n").toLowerCase();

  if (item?.category === "COLLECTION") {
    if (/frustrat|delay|screwed|support issue|blocker|waiting on|promised|problem with|issue with|not working/.test(context)) {
      return {
        key: "support_first",
        label: "Resolve support issue first",
        objective: "Protect trust before raising the outstanding balance.",
        draft: `Hey ${first}, I wanted to check in on the support side first because I know there has been some frustration around getting things moving. I want to make sure we have that properly sorted before anything else. Once that is moving, there is also an outstanding payment item I need to tidy up with you, but I would rather make sure the delivery side is where it needs to be first. Can you let me know where things currently stand?`
      };
    }
    if (/financial pressure|money problem|money problems|tight on money|lost (his|her|their|my) job|can't afford|cannot afford|mortgage|financial constraint|financial hardship|things are tight/.test(context)) {
      return {
        key: "financial_pressure",
        label: "Financial pressure conversation",
        objective: "Open a low-pressure conversation before asking for payment.",
        draft: `Hey ${first}, I know you mentioned things are a bit tight financially at the moment, so I did not want to just send you a generic payment chase. There is an outstanding balance on the account and I wanted to check in with you directly first. Where are things at on your side at the moment, and what feels realistic?`
      };
    }
    if (/declin|blocked|security|failed charge|payment failed|bank restriction|new card|insufficient funds|too many failed/.test(context)) {
      return {
        key: "payment_method_issue",
        label: "Payment method issue",
        objective: "Get a clear payment-method fix without making the message confrontational.",
        draft: `Hey ${first}, quick one. It looks like the latest payment is still not going through. From what I can see it may be a card or bank issue rather than anything you need to do in the program. Could you take a look when you get a chance? If the bank is blocking it, another card or checking with the bank is usually the quickest fix. If you need the payment link from me, just say and I will get it over to you.`
      };
    }
    if (/positive repl|traction|wins posted|going well|campaign.*active|campaign.*running|book.*call|new interested|progress|scaling/.test(context)) {
      return {
        key: "active_momentum",
        label: "Active client with momentum",
        objective: "Use current progress as a natural point to tidy up the balance.",
        draft: `Hey ${first}, good to see things are moving on the campaign side. One thing I wanted to tidy up alongside that is the outstanding payment on the account. Could you take a look at that for me today? If there is anything getting in the way of it going through, let me know and I can help get it sorted.`
      };
    }
    return {
      key: "standard_collection",
      label: "Straightforward payment follow-up",
      objective: "Flag the missed payment clearly and give an easy next step.",
      draft: `Hey ${first}, just wanted to flag that it looks like the latest payment on the account did not go through. Could you take a quick look when you get a chance? If you have changed cards or need the payment link again, let me know and I will help get it sorted.`
    };
  }

  if (/quoted|price|pricing|chargeback|dispute|claritypay|financ|loan|cancel.*payment plan|remaining loan/.test(context)) {
    return {
      key: "high_risk_payment_dispute",
      label: "Pricing / financing / dispute",
      objective: "Clarify facts before making any refund or payment commitment.",
      draft: `Hey ${first}, I want to make sure I understand this properly before I give you an answer or make any assumptions. From what I can see there is a payment or financing piece tied into the request, so I want to get the facts straight first. Can you tell me, in your own words, what you understood the payment arrangement to be and what outcome you are asking us for now? Once I have that, I can look at the account properly and come back to you clearly.`
    };
  }
  if (/financial pressure|financial hardship|tight on money|lost (his|her|their|my) job|mortgage|parents|bank account|financial constraint|cannot continue|can't continue/.test(context)) {
    return {
      key: "refund_financial_hardship",
      label: "Refund request from financial pressure",
      objective: "Understand whether they need a refund, payment relief, or a pause before discussing policy.",
      draft: `Hey ${first}, thanks for being open about what is going on. Before I make any assumptions about the refund side, I want to understand what would actually help most right now. Are you looking specifically for a full refund, to stop or pause future payments, or mainly to reduce the financial pressure for a while? If you tell me the outcome you are hoping for, I can look at the account from there.`
    };
  }
  if (/guarantee|six full months|6 full months|qualifying activity|roi guarantee|verify.*activity|required step/.test(context)) {
    return {
      key: "guarantee_review",
      label: "Guarantee eligibility review",
      objective: "Gather and verify evidence before giving a decision.",
      draft: `Hey ${first}, I am going to look at this properly against the guarantee criteria before I give you a definitive answer. I want to make sure we are being fair and looking at the full picture rather than guessing. I am checking the activity, implementation and support history now. If there is anything you think is important for me to include when I review it, send it over here and I will factor it in.`
    };
  }
  if (/no client|no clients|no result|no results|not work|didn't work|did not work|too risky|results|dissatisf|not getting/.test(context)) {
    return {
      key: "results_dissatisfaction",
      label: "Results / delivery dissatisfaction",
      objective: "Understand the gap between expected and actual results before deciding on the refund.",
      draft: `Hey ${first}, I want to look at this properly rather than just throwing policy at you. Can you tell me what you feel has not worked and where you think the biggest gap has been between what you expected and what has happened so far? I am going to compare that with the activity and support history on our side as well so we can have a proper conversation about it.`
    };
  }
  if (/resolved|already visible|already refunded|exchange appears resolved|no further action|cancelled.*monthly plan/.test(context)) {
    return {
      key: "close_the_loop",
      label: "Confirm resolution",
      objective: "Close the loop without reopening an issue that may already be solved.",
      draft: `Hey ${first}, just wanted to close the loop on this and make sure we are all squared away. From what I can see the immediate issue looks to have been resolved. Can you confirm everything looks right on your side now? If so, I will mark it as sorted here as well.`
    };
  }
  if (/panic|stress|threat|urgent|upset|angry|frustrat/.test(context)) {
    return {
      key: "deescalate_first",
      label: "De-escalate first",
      objective: "Lower emotion and understand the immediate pressure before discussing policy.",
      draft: `Hey ${first}, I can see this is causing some stress and I do not want to rush into giving you a generic answer. I want to understand what is putting the most pressure on you right now and what you are hoping we can resolve first. Talk me through that and I will take it from there with you.`
    };
  }

  return {
    key: "refund_clarify_request",
    label: "Clarify refund request",
    objective: "Understand what changed and what outcome they want before giving a decision.",
    draft: `Hey ${first}, thanks for flagging this. I want to understand the situation properly before I give you a definitive answer. Can you tell me what has changed for you and what outcome you are hoping for from the refund request? Once I have that context, I can look at the account properly and come back to you clearly.`
  };
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
          dueAt: command.due_at ? String(command.due_at) : undefined,
          metadata: command.metadata && typeof command.metadata === "object" ? command.metadata : undefined
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
      due_at: z.string().optional(),
      metadata: z.record(z.string(), z.unknown()).optional()
    }),
    annotations: { readOnlyHint: false, destructiveHint: false, openWorldHint: false },
    securitySchemes: [{ type: "noauth" }]
  }, async ({ category, client_name, title, summary, priority, source, due_at, metadata }) => {
    const item = await createDashboardItem({ category, clientName: client_name, title, summary, priority, source, dueAt: due_at, metadata });
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

  server.registerTool("list_dashboard_actions", {
    description: "List dashboard actions explicitly approved by Dave and waiting for ChatGPT to execute through connected apps such as Slack or Calendly.",
    inputSchema: z.object({ status: z.enum(["PENDING","COMPLETED","FAILED"]).default("PENDING") }),
    annotations: { readOnlyHint: true, destructiveHint: false, openWorldHint: false },
    securitySchemes: [{ type: "noauth" }]
  }, async ({ status }) => {
    await ensureOpsTables();
    const result = await pool!.query(`SELECT * FROM dashboard_actions WHERE status=$1 ORDER BY created_at ASC LIMIT 100`, [status]);
    return textResult({ actions: result.rows });
  });

  server.registerTool("resolve_dashboard_action", {
    description: "Mark a previously approved dashboard action as COMPLETED or FAILED after ChatGPT executes it through the relevant connected app.",
    inputSchema: z.object({
      id: z.number().int().positive(),
      status: z.enum(["COMPLETED","FAILED"]),
      result_note: z.string().optional()
    }),
    annotations: { readOnlyHint: false, destructiveHint: false, openWorldHint: false },
    securitySchemes: [{ type: "noauth" }]
  }, async ({ id, status, result_note }) => {
    await ensureOpsTables();
    const result = await pool!.query(
      `UPDATE dashboard_actions SET status=$2,result_note=$3,completed_at=NOW(),updated_at=NOW()
       WHERE id=$1 AND status='PENDING' RETURNING *`,
      [id,status,result_note||null]
    );
    if (!result.rowCount) throw new Error("Action must exist and be PENDING");
    const action = result.rows[0];
    if (status === "COMPLETED" && action.action_type === "MARK_CALENDLY_NO_SHOW" && action.payload?.dashboard_item_id) {
      await pool!.query(`UPDATE dashboard_items SET status='DONE', metadata=metadata || $2::jsonb, updated_at=NOW() WHERE id=$1`, [
        Number(action.payload.dashboard_item_id),
        JSON.stringify({ no_show_marked: true, no_show_marked_at: new Date().toISOString() })
      ]);
    }
    await logActivity({ eventType: "dashboard_action_resolved", title: `${status}: ${action.action_type}`, detail: result_note || null, entityType: "dashboard_action", entityId: action.id, actor: "ChatGPT" });
    return textResult({ action });
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


app.get("/dashboard/api/eod/context", requireDashboardAuth, async (req, res) => {
  try {
    await ensureOpsTables();
    const start = typeof req.query.start === "string" ? req.query.start : "";
    const end = typeof req.query.end === "string" ? req.query.end : "";
    if (!start || !end || Number.isNaN(Date.parse(start)) || Number.isNaN(Date.parse(end))) {
      return res.status(400).json({ error: "Valid start and end timestamps are required" });
    }

    const [activityResult, actionsResult, itemsResult, overridesResult] = await Promise.all([
      pool!.query(
        `SELECT * FROM dashboard_activity WHERE created_at >= $1::timestamptz AND created_at < $2::timestamptz ORDER BY created_at ASC`,
        [start, end]
      ),
      pool!.query(
        `SELECT * FROM dashboard_actions
         WHERE (created_at >= $1::timestamptz AND created_at < $2::timestamptz)
            OR (completed_at >= $1::timestamptz AND completed_at < $2::timestamptz)
         ORDER BY created_at ASC`,
        [start, end]
      ),
      pool!.query(
        `SELECT * FROM dashboard_items
         WHERE created_at < $2::timestamptz
           AND (updated_at >= $1::timestamptz OR due_at >= $1::timestamptz)
         ORDER BY updated_at ASC`,
        [start, end]
      ),
      pool!.query(
        `SELECT * FROM dashboard_call_overrides
         WHERE call_start_time >= $1::timestamptz AND call_start_time < $2::timestamptz
         ORDER BY call_start_time ASC`,
        [start, end]
      )
    ]);

    let fathom: any = { available: false, meetings: [], error: null };
    if (FATHOM_API_KEY) {
      try {
        const url = new URL("https://api.fathom.ai/external/v1/meetings");
        url.searchParams.set("created_after", start);
        url.searchParams.set("created_before", end);
        url.searchParams.set("include_summary", "true");
        url.searchParams.set("include_action_items", "true");

        const r = await fetch(url, {
          headers: { "X-Api-Key": FATHOM_API_KEY, Accept: "application/json" },
          signal: AbortSignal.timeout(20000)
        });
        const body: any = await r.json().catch(() => null);
        if (!r.ok) {
          if (r.status === 401 || r.status === 403) throw new Error("Fathom API key is invalid or does not have access");
          throw new Error(`Fathom meetings lookup failed ${r.status}`);
        }

        const rawMeetings = Array.isArray(body?.items) ? body.items : [];
        fathom = {
          available: true,
          meetings: rawMeetings.map((m: any) => ({
            recording_id: m.recording_id || null,
            title: m.title || m.meeting_title || m.meeting_type || "Fathom meeting",
            meeting_title: m.meeting_title || null,
            meeting_type: m.meeting_type || null,
            url: m.url || m.share_url || null,
            meeting_url: m.meeting_url || null,
            created_at: m.created_at || null,
            scheduled_start_time: m.scheduled_start_time || null,
            scheduled_end_time: m.scheduled_end_time || null,
            recording_start_time: m.recording_start_time || null,
            recording_end_time: m.recording_end_time || null,
            calendar_invitees: Array.isArray(m.calendar_invitees) ? m.calendar_invitees.map((i: any) => ({
              name: i?.name || null,
              email: i?.email || null,
              is_external: Boolean(i?.is_external)
            })) : [],
            summary: m.default_summary?.markdown_formatted || m.summary?.markdown_formatted || m.summary || null,
            action_items: Array.isArray(m.action_items) ? m.action_items.map((a: any) => ({
              description: a?.description || null,
              completed: Boolean(a?.completed),
              assignee: a?.assignee?.name || null
            })) : []
          })),
          error: null
        };
      } catch (err) {
        fathom = { available: false, meetings: [], error: err instanceof Error ? err.message : "Fathom lookup failed" };
      }
    } else {
      fathom.error = "FATHOM_API_KEY is not configured";
    }

    let calendly: any = { available: false, events: [], error: null };
    if (CALENDLY_ACCESS_TOKEN) {
      try {
        const headers = { Authorization: `Bearer ${CALENDLY_ACCESS_TOKEN}`, Accept: "application/json" };
        const meResponse = await fetch("https://api.calendly.com/users/me", { headers, signal: AbortSignal.timeout(15000) });
        const meBody: any = await meResponse.json().catch(() => null);
        if (!meResponse.ok || !meBody?.resource?.uri) throw new Error(`Calendly user lookup failed ${meResponse.status}`);

        async function getEvents(status: "active" | "canceled") {
          const url = new URL("https://api.calendly.com/scheduled_events");
          url.searchParams.set("user", meBody.resource.uri);
          url.searchParams.set("status", status);
          url.searchParams.set("min_start_time", start);
          url.searchParams.set("max_start_time", end);
          url.searchParams.set("count", "100");
          url.searchParams.set("sort", "start_time:asc");
          const r = await fetch(url, { headers, signal: AbortSignal.timeout(15000) });
          const body: any = await r.json().catch(() => null);
          if (!r.ok) throw new Error(`Calendly events lookup failed ${r.status}`);
          return Array.isArray(body?.collection) ? body.collection : [];
        }

        const [activeEvents, canceledEvents] = await Promise.all([getEvents("active"), getEvents("canceled")]);
        const rawEvents = [...activeEvents, ...canceledEvents];

        const events = await Promise.all(rawEvents.map(async (event: any) => {
          const eventId = String(event?.uri || "").split("/").filter(Boolean).pop();
          let invitees: any[] = [];
          if (eventId) {
            try {
              const url = new URL(`https://api.calendly.com/scheduled_events/${eventId}/invitees`);
              url.searchParams.set("count", "100");
              const r = await fetch(url, { headers, signal: AbortSignal.timeout(15000) });
              const body: any = await r.json().catch(() => null);
              if (r.ok && Array.isArray(body?.collection)) invitees = body.collection;
            } catch {}
          }
          return {
            uri: event?.uri || null,
            name: event?.name || "Calendly meeting",
            status: event?.status || null,
            start_time: event?.start_time || null,
            end_time: event?.end_time || null,
            event_type: event?.event_type || null,
            invitees: invitees.map((i: any) => ({
              uri: i?.uri || null,
              name: i?.name || null,
              email: i?.email || null,
              timezone: i?.timezone || null,
              status: i?.status || null,
              rescheduled: Boolean(i?.rescheduled),
              old_invitee: i?.old_invitee || null,
              new_invitee: i?.new_invitee || null,
              no_show: i?.no_show || null,
              cancellation: i?.cancellation || null,
              created_at: i?.created_at || null,
              updated_at: i?.updated_at || null
            }))
          };
        }));

        calendly = { available: true, events, error: null };
      } catch (err) {
        calendly = { available: false, events: [], error: err instanceof Error ? err.message : "Calendly lookup failed" };
      }
    } else {
      calendly.error = "CALENDLY_ACCESS_TOKEN is not configured";
    }

    res.json({
      window: { start, end },
      activity: activityResult.rows,
      actions: actionsResult.rows,
      items: itemsResult.rows,
      call_overrides: overridesResult.rows,
      calendly,
      fathom
    });
  } catch (err) {
    res.status(500).json({ error: err instanceof Error ? err.message : "Failed to build EOD context" });
  }
});

app.post("/dashboard/api/eod/no-show", requireDashboardAuth, async (req, res) => {
  try {
    await ensureOpsTables();
    const inviteeUri = typeof req.body?.calendly_invitee_uri === "string" ? req.body.calendly_invitee_uri.trim() : "";
    const eventUri = typeof req.body?.calendly_event_uri === "string" ? req.body.calendly_event_uri.trim() : "";
    const clientName = typeof req.body?.client_name === "string" ? req.body.client_name.trim() : "";
    const callTitle = typeof req.body?.call_title === "string" ? req.body.call_title.trim() : "";
    const callStartTime = typeof req.body?.call_start_time === "string" ? req.body.call_start_time : "";

    if (!inviteeUri) return res.status(400).json({ error: "Calendly invitee reference is required" });
    if (!callStartTime || Number.isNaN(Date.parse(callStartTime))) return res.status(400).json({ error: "Valid call start time is required" });

    const override = await pool!.query(
      `INSERT INTO dashboard_call_overrides
       (calendly_invitee_uri,calendly_event_uri,client_name,call_title,call_start_time,status,note)
       VALUES ($1,$2,$3,$4,$5,'NO_SHOW','Marked by Dave from EOD review')
       ON CONFLICT (calendly_invitee_uri)
       DO UPDATE SET status='NO_SHOW',note='Marked by Dave from EOD review',updated_at=NOW()
       RETURNING *`,
      [inviteeUri, eventUri || null, clientName || null, callTitle || null, callStartTime]
    );

    const action = await pool!.query(
      `INSERT INTO dashboard_actions (action_type,payload,status,requested_by)
       VALUES ('MARK_CALENDLY_NO_SHOW',$1::jsonb,'PENDING','Dave') RETURNING *`,
      [JSON.stringify({
        client_name: clientName || null,
        title: callTitle || null,
        calendly_invitee_uri: inviteeUri,
        calendly_event_uri: eventUri || null,
        source: "EOD_REVIEW"
      })]
    );

    await logActivity({
      eventType: "eod_no_show_marked",
      title: "No-show marked from EOD review",
      detail: callTitle || null,
      clientName: clientName || null,
      entityType: "dashboard_call_override",
      entityId: override.rows[0].id,
      actor: "Dave"
    });

    res.json({ override: override.rows[0], action: action.rows[0] });
  } catch (err) {
    res.status(500).json({ error: err instanceof Error ? err.message : "Failed to mark no-show" });
  }
});

app.post("/dashboard/api/eod/send", requireDashboardAuth, async (req, res) => {
  try {
    const message = typeof req.body?.message === "string" ? req.body.message.trim() : "";
    if (!message) return res.status(400).json({ error: "EOD report is empty" });
    if (!SLACK_BOT_TOKEN) {
      return res.status(503).json({ error: "SLACK_BOT_TOKEN is not configured in Render, so the EOD cannot be sent yet." });
    }

    const slackResponse = await fetch("https://slack.com/api/chat.postMessage", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${SLACK_BOT_TOKEN}`,
        "Content-Type": "application/json; charset=utf-8"
      },
      body: JSON.stringify({
        channel: "C0BEJ7304QJ",
        text: message,
        mrkdwn: true
      }),
      signal: AbortSignal.timeout(15000)
    });

    const slackBody: any = await slackResponse.json().catch(() => null);
    if (!slackResponse.ok || !slackBody?.ok) {
      const code = slackBody?.error || `HTTP ${slackResponse.status}`;
      const needed = slackBody?.needed || slackResponse.headers.get("x-accepted-oauth-scopes") || "";
      const provided = slackBody?.provided || slackResponse.headers.get("x-oauth-scopes") || "";
      const scopeDetail = code === "missing_scope"
        ? ` Needed: ${needed || "unknown"}. Token currently has: ${provided || "unknown"}.`
        : "";
      return res.status(502).json({ error: `Slack send failed: ${code}.${scopeDetail}` });
    }

    await logActivity({
      eventType: "eod_slack_sent",
      title: "EOD report sent to Slack",
      detail: message.slice(0,1000),
      entityType: "slack_message",
      entityId: slackBody.ts || null,
      actor: "Dave"
    });

    res.json({
      sent: true,
      channel_id: "C0BEJ7304QJ",
      message_ts: slackBody.ts || null
    });
  } catch (err) {
    res.status(500).json({ error: err instanceof Error ? err.message : "Failed to send EOD to Slack" });
  }
});


app.post("/dashboard/api/items/:id/draft-message", requireDashboardAuth, async (req, res) => {
  try {
    await ensureOpsTables();
    const id = Number(req.params.id);
    if (!Number.isFinite(id)) return res.status(400).json({ error: "Valid dashboard item ID is required" });

    const found = await pool!.query(
      `SELECT * FROM dashboard_items WHERE id=$1 AND status='OPEN'`,
      [id]
    );
    if (!found.rowCount) return res.status(404).json({ error: "Dashboard item not found" });

    const item = found.rows[0];
    if (!["COLLECTION", "REFUND"].includes(String(item.category))) {
      return res.status(400).json({ error: "Draft Message is only available for Collections and Refunds" });
    }
    if (!item.client_name) return res.status(409).json({ error: "This item is missing a client name" });

    const reasonPrefix = `CASE_DRAFT:${id}:`;
    const existing = await pool!.query(
      `SELECT * FROM heartbeat_hold_queue
       WHERE reason LIKE $1 AND status IN ('HOLD','APPROVED')
       ORDER BY created_at DESC LIMIT 1`,
      [reasonPrefix + "%"]
    );
    if (existing.rowCount) {
      const existingItem = existing.rows[0];
      const strategyKey = String(existingItem.reason || "").split(":")[2] || "existing";
      return res.json({ created: false, existing: true, item: existingItem, strategy: { key: strategyKey, label: "Existing draft already waiting" } });
    }

    let compact: Array<{ text?: string }> = [];
    try {
      const matches = await findChatChannels(String(item.client_name));
      if (matches.length === 1) {
        const messages = await getChatHistory(matches[0].id, 30);
        compact = await compactMessages(messages);
      }
    } catch {
      // Item summary still provides enough context for a safe starting draft.
    }

    const strategy = buildCaseDraftStrategy(item, compact);
    const reason = `${reasonPrefix}${strategy.key} | ${strategy.label} | Objective: ${strategy.objective}`;

    let queued;
    try {
      queued = await queueReply(String(item.client_name), strategy.draft, reason);
    } catch (err) {
      const message = err instanceof Error ? err.message : "";
      if (/No matching Heartbeat CHAT channel found/i.test(message)) {
        queued = await queuePendingHeartbeatReply(String(item.client_name), strategy.draft, reason);
      } else {
        throw err;
      }
    }

    await logActivity({
      eventType: "case_message_drafted",
      title: `${item.category === "COLLECTION" ? "Collection" : "Refund"} message drafted: ${strategy.label}`,
      detail: strategy.objective,
      clientName: item.client_name,
      entityType: "dashboard_item",
      entityId: item.id,
      actor: "Dave"
    });

    res.json({ created: true, item: queued, strategy });
  } catch (err) {
    res.status(500).json({ error: err instanceof Error ? err.message : "Failed to create case draft" });
  }
});

app.post("/dashboard/api/items/:id/no-show", requireDashboardAuth, async (req, res) => {
  try {
    await ensureOpsTables();
    const id = Number(req.params.id);
    const found = await pool!.query(`SELECT * FROM dashboard_items WHERE id=$1 AND status='OPEN'`, [id]);
    if (!found.rowCount) return res.status(404).json({ error: "Call item not found" });
    const item = found.rows[0];
    const invitee = item.metadata?.calendly_invitee_uri;
    if (!invitee) return res.status(409).json({ error: "This call is missing its Calendly invitee reference." });
    const result = await pool!.query(
      `INSERT INTO dashboard_actions (action_type,payload,status,requested_by)
       VALUES ('MARK_CALENDLY_NO_SHOW',$1::jsonb,'PENDING','Dave') RETURNING *`,
      [JSON.stringify({ dashboard_item_id: id, client_name: item.client_name, title: item.title, calendly_invitee_uri: invitee, calendly_event_uri: item.metadata?.calendly_event_uri || null })]
    );
    await logActivity({ eventType: "dashboard_action_requested", title: "Calendly no-show approved", detail: item.title, clientName: item.client_name, entityType: "dashboard_action", entityId: result.rows[0].id, actor: "Dave" });
    res.json({ queued: true, action: result.rows[0] });
  } catch (err) {
    res.status(500).json({ error: err instanceof Error ? err.message : "Failed to queue no-show action" });
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


app.get("/dashboard/api/calendly/event-types", requireDashboardAuth, async (_req, res) => {
  try {
    if (!CALENDLY_ACCESS_TOKEN) return res.status(503).json({ error: "CALENDLY_ACCESS_TOKEN is not configured" });

    if (calendlyEventTypeCache && Date.now() - calendlyEventTypeCache.at < CALENDLY_EVENT_TYPE_CACHE_MS) {
      return res.json({
        user: calendlyEventTypeCache.user,
        event_types: calendlyEventTypeCache.event_types,
        cached: true
      });
    }

    const headers = {
      Authorization: `Bearer ${CALENDLY_ACCESS_TOKEN}`,
      Accept: "application/json"
    };

    const meResponse = await fetch("https://api.calendly.com/users/me", {
      headers,
      signal: AbortSignal.timeout(15000)
    });
    const meText = await meResponse.text();
    let meBody: any;
    try { meBody = meText ? JSON.parse(meText) : null; } catch { meBody = null; }
    if (!meResponse.ok || !meBody?.resource?.uri) {
      if (meResponse.status === 401) return res.status(401).json({ error: "Calendly token expired or is invalid — update CALENDLY_ACCESS_TOKEN in Render." });
      return res.status(502).json({ error: `Calendly user lookup failed ${meResponse.status}` });
    }

    const url = new URL("https://api.calendly.com/event_types");
    url.searchParams.set("user", meBody.resource.uri);
    url.searchParams.set("active", "true");
    url.searchParams.set("count", "100");
    url.searchParams.set("sort", "name:asc");

    const eventsResponse = await fetch(url, {
      headers,
      signal: AbortSignal.timeout(15000)
    });
    const eventsText = await eventsResponse.text();
    let eventsBody: any;
    try { eventsBody = eventsText ? JSON.parse(eventsText) : null; } catch { eventsBody = null; }
    if (!eventsResponse.ok) {
      if (eventsResponse.status === 401) return res.status(401).json({ error: "Calendly token expired or is invalid — update CALENDLY_ACCESS_TOKEN in Render." });
      return res.status(502).json({ error: `Calendly event type lookup failed ${eventsResponse.status}` });
    }

    const eventTypes = (Array.isArray(eventsBody?.collection) ? eventsBody.collection : [])
      .filter((e: any) => e?.active && e?.scheduling_url)
      .map((e: any) => ({
        uri: e.uri,
        name: e.name || "Untitled event",
        duration: e.duration || null,
        scheduling_url: e.scheduling_url,
        kind: e.kind || null
      }));

    const user = { name: meBody.resource.name, scheduling_url: meBody.resource.scheduling_url };
    calendlyEventTypeCache = { at: Date.now(), user, event_types: eventTypes };

    res.json({ user, event_types: eventTypes, cached: false });
  } catch (err) {
    res.status(500).json({ error: err instanceof Error ? err.message : "Failed to load Calendly event types" });
  }
});

app.get("/dashboard/api/messages/mentions", requireDashboardAuth, async (req, res) => {
  try {
    const q = typeof req.query.q === "string" ? normalise(req.query.q) : "";
    if (!q) return res.json({ users: [] });
    const users = (await listHeartbeatUsers())
      .filter((u) => normalise(u.name).includes(q))
      .slice(0, 8);
    res.json({ users });
  } catch (err) {
    res.status(500).json({ error: err instanceof Error ? err.message : "Failed to search Heartbeat users" });
  }
});

app.get("/dashboard/api/messages/search", requireDashboardAuth, async (req, res) => {
  try {
    const q = typeof req.query.q === "string" ? req.query.q.trim() : "";
    if (!q) return res.json({ matches: [] });
    const matches = await findChatChannels(q);
    res.json({ matches: matches.slice(0, 20).map(ch => ({ id: ch.id, name: ch.name, type: ch.type })) });
  } catch (err) {
    res.status(500).json({ error: err instanceof Error ? err.message : "Failed to search Heartbeat chats" });
  }
});

app.get("/dashboard/api/messages/:channelId/context", requireDashboardAuth, async (req, res) => {
  try {
    const channelId = String(req.params.channelId || "");
    const max = Math.max(1, Math.min(100, Number(req.query.limit) || 30));
    const messages = await getChatHistory(channelId, max);
    const compact = await compactMessages(messages);
    res.json({ channel_id: channelId, messages: compact });
  } catch (err) {
    res.status(500).json({ error: err instanceof Error ? err.message : "Failed to load Heartbeat context" });
  }
});

app.post("/dashboard/api/messages/:channelId/send", requireDashboardAuth, async (req, res) => {
  try {
    const channelId = String(req.params.channelId || "");
    const message = typeof req.body?.message === "string" ? req.body.message.trim() : "";
    const clientName = typeof req.body?.client_name === "string" ? req.body.client_name.trim() : "";
    if (!message) return res.status(400).json({ error: "Message cannot be empty" });
    if (!channelId) return res.status(400).json({ error: "Heartbeat chat is required" });
    await sendHeartbeatMessage(channelId, message);
    await logActivity({
      eventType: "heartbeat_manual_message_sent",
      title: "Heartbeat message sent from Command Centre",
      detail: message,
      clientName: clientName || null,
      entityType: "heartbeat_channel",
      entityId: channelId,
      actor: "Dave"
    });
    res.json({ sent: true });
  } catch (err) {
    res.status(500).json({ error: err instanceof Error ? err.message : "Failed to send Heartbeat message" });
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
