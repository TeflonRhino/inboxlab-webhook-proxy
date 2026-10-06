import crypto from 'node:crypto';
import express from 'express';
import cors from 'cors';
import { WebSocketServer } from 'ws';
import rtms from '@zoom/rtms';

const PORT = Number(process.env.PORT || 10000);
const WEBHOOK_SECRET = process.env.ZOOM_WEBHOOK_SECRET_TOKEN || '';

const app = express();
app.use(cors({ origin: true }));
app.use(express.json({
  limit: '1mb',
  verify(req, _res, buf) {
    req.rawBody = buf.toString('utf8');
  }
}));

const httpServer = app.listen(PORT, '0.0.0.0', () => {
  console.log('[Talking Point] listening on ' + PORT);
});

const wss = new WebSocketServer({ server: httpServer, path: '/live' });
const browserClients = new Set();
const rtmsClients = new Map();
const transcripts = new Map();

function broadcast(message) {
  const data = JSON.stringify(message);
  for (const socket of browserClients) {
    if (socket.readyState === 1) socket.send(data);
  }
}

wss.on('connection', socket => {
  browserClients.add(socket);
  socket.send(JSON.stringify({ type: 'status', status: 'connected' }));
  socket.on('close', () => browserClients.delete(socket));
});

app.get('/', (_req, res) => {
  res.json({
    name: 'Talking Point RTMS',
    ok: true,
    zoomConfigured: Boolean(process.env.ZM_RTMS_CLIENT && process.env.ZM_RTMS_SECRET),
    webhookVerificationConfigured: Boolean(WEBHOOK_SECRET)
  });
});

app.get('/health', (_req, res) => {
  res.json({
    ok: true,
    service: 'talking-point-rtms',
    activeStreams: rtmsClients.size,
    browserClients: browserClients.size
  });
});

function validZoomSignature(req) {
  if (!WEBHOOK_SECRET) return true;
  const timestamp = req.header('x-zm-request-timestamp') || '';
  const supplied = req.header('x-zm-signature') || '';
  if (!timestamp || !supplied || !req.rawBody) return false;
  const message = 'v0:' + timestamp + ':' + req.rawBody;
  const expected = 'v0=' + crypto.createHmac('sha256', WEBHOOK_SECRET).update(message).digest('hex');
  try {
    return crypto.timingSafeEqual(Buffer.from(expected), Buffer.from(supplied));
  } catch {
    return false;
  }
}

function startStream(payload) {
  const streamId = payload?.rtms_stream_id;
  if (!streamId || rtmsClients.has(streamId)) return;

  const client = new rtms.Client();
  rtmsClients.set(streamId, client);
  transcripts.set(streamId, []);

  if (typeof client.enableTranscript === 'function') {
    client.enableTranscript(true);
  }

  client.onTranscriptData((buffer, size, timestamp, metadata) => {
    const raw = Buffer.isBuffer(buffer) ? buffer.subarray(0, size || buffer.length) : Buffer.from(buffer);
    const text = raw.toString('utf8').trim();
    if (!text) return;

    const segment = {
      type: 'transcript',
      streamId,
      timestamp,
      speaker: metadata?.userName || 'Speaker',
      userId: metadata?.userId || null,
      text
    };

    const history = transcripts.get(streamId) || [];
    history.push(segment);
    if (history.length > 300) history.shift();
    transcripts.set(streamId, history);
    broadcast(segment);
  });

  if (typeof client.onJoinConfirm === 'function') {
    client.onJoinConfirm(reason => {
      broadcast({ type: 'meeting', event: 'joined', streamId, reason });
    });
  }

  if (typeof client.onLeave === 'function') {
    client.onLeave(reason => {
      rtmsClients.delete(streamId);
      broadcast({ type: 'meeting', event: 'left', streamId, reason });
    });
  }

  client.join(payload);
}

function stopStream(streamId) {
  const client = rtmsClients.get(streamId);
  if (!client) return;
  try { client.leave(); } catch {}
  rtmsClients.delete(streamId);
  broadcast({ type: 'meeting', event: 'stopped', streamId });
}

app.post('/zoom/webhook', (req, res) => {
  const event = req.body?.event;
  const payload = req.body?.payload || {};

  if (event === 'endpoint.url_validation') {
    const plainToken = payload?.plainToken || '';
    if (!WEBHOOK_SECRET) {
      return res.status(503).json({ error: 'ZOOM_WEBHOOK_SECRET_TOKEN is not configured' });
    }
    const encryptedToken = crypto
      .createHmac('sha256', WEBHOOK_SECRET)
      .update(plainToken)
      .digest('hex');
    return res.json({ plainToken, encryptedToken });
  }

  if (!validZoomSignature(req)) {
    return res.status(401).json({ error: 'Invalid Zoom webhook signature' });
  }

  res.status(200).json({ received: true });

  if (event === 'meeting.rtms_started') {
    queueMicrotask(() => startStream(payload));
  } else if (event === 'meeting.rtms_stopped') {
    queueMicrotask(() => stopStream(payload?.rtms_stream_id));
  }
});

app.get('/streams/:streamId/transcript', (req, res) => {
  res.json({
    streamId: req.params.streamId,
    transcript: transcripts.get(req.params.streamId) || []
  });
});

app.post('/coach', (req, res) => {
  const framework = Array.isArray(req.body?.framework) ? req.body.framework : [];
  const transcript = Array.isArray(req.body?.transcript) ? req.body.transcript : [];
  const joined = transcript.map(x => String(x?.text || x)).join(' ').toLowerCase();

  const items = framework.map(item => {
    const hints = [item?.title, item?.description, item?.prompt]
      .filter(Boolean)
      .join(' ')
      .toLowerCase()
      .split(/[^a-z0-9]+/)
      .filter(word => word.length >= 5);

    const hits = hints.filter(word => joined.includes(word)).length;
    const status = hits >= Math.max(2, Math.ceil(hints.length * 0.35))
      ? 'covered'
      : hits > 0 ? 'partial' : 'not_covered';

    return { id: item?.id, status };
  });

  const next = framework.find(item => {
    const match = items.find(state => state.id === item?.id);
    return match?.status !== 'covered';
  });

  res.json({
    mode: 'local',
    items,
    nextBestMove: next?.prompt || next?.title || null
  });
});
