import crypto from 'node:crypto';
import express from 'express';
import cors from 'cors';
import WebSocket, { WebSocketServer } from 'ws';

const PORT = Number(process.env.PORT || 10000);
const ZOOM_CLIENT_ID = process.env.ZOOM_CLIENT_ID || '';
const ZOOM_CLIENT_SECRET = process.env.ZOOM_CLIENT_SECRET || '';
const ZOOM_WEBHOOK_SECRET_TOKEN = process.env.ZOOM_WEBHOOK_SECRET_TOKEN || '';

const app = express();
app.use(cors({ origin: true }));
app.use(express.json({
  limit: '1mb',
  verify(req, _res, buf) {
    req.rawBody = buf.toString('utf8');
  }
}));

const server = app.listen(PORT, '0.0.0.0', () => {
  console.log('[Talking Point] backend listening on ' + PORT);
});

const browserWss = new WebSocketServer({ server, path: '/live' });
const browserClients = new Set();
const activeStreams = new Map();
const transcripts = new Map();

browserWss.on('connection', socket => {
  browserClients.add(socket);
  socket.send(JSON.stringify({ type: 'status', status: 'connected' }));
  socket.on('close', () => browserClients.delete(socket));
});

function broadcast(message) {
  const payload = JSON.stringify(message);
  for (const socket of browserClients) {
    if (socket.readyState === WebSocket.OPEN) socket.send(payload);
  }
}

function generateRtmsSignature(meetingUuid, rtmsStreamId) {
  const message = ZOOM_CLIENT_ID + ',' + meetingUuid + ',' + rtmsStreamId;
  return crypto.createHmac('sha256', ZOOM_CLIENT_SECRET).update(message).digest('hex');
}

function verifyWebhookSignature(req) {
  if (!ZOOM_WEBHOOK_SECRET_TOKEN) return true;
  const timestamp = req.header('x-zm-request-timestamp') || '';
  const supplied = req.header('x-zm-signature') || '';
  if (!timestamp || !supplied || !req.rawBody) return false;
  const message = 'v0:' + timestamp + ':' + req.rawBody;
  const expected = 'v0=' + crypto
    .createHmac('sha256', ZOOM_WEBHOOK_SECRET_TOKEN)
    .update(message)
    .digest('hex');
  try {
    return crypto.timingSafeEqual(Buffer.from(expected), Buffer.from(supplied));
  } catch {
    return false;
  }
}

function closeStream(streamId) {
  const stream = activeStreams.get(streamId);
  if (!stream) return;
  try { stream.signaling?.close(); } catch {}
  try { stream.media?.close(); } catch {}
  activeStreams.delete(streamId);
  broadcast({ type: 'meeting', event: 'stopped', streamId });
}

function connectMedia(mediaUrl, meetingUuid, rtmsStreamId, signalingWs, streamState) {
  const mediaWs = new WebSocket(mediaUrl);
  streamState.media = mediaWs;

  mediaWs.on('open', () => {
    const handshake = {
      msg_type: 3,
      protocol_version: 1,
      sequence: 0,
      meeting_uuid: meetingUuid,
      rtms_stream_id: rtmsStreamId,
      signature: generateRtmsSignature(meetingUuid, rtmsStreamId),
      media_type: 8
    };
    mediaWs.send(JSON.stringify(handshake));
  });

  mediaWs.on('message', data => {
    let msg;
    try { msg = JSON.parse(data.toString()); } catch { return; }

    if (msg.msg_type === 4) {
      if (msg.status_code === 0) {
        signalingWs.send(JSON.stringify({
          msg_type: 7,
          rtms_stream_id: rtmsStreamId
        }));
        broadcast({ type: 'meeting', event: 'joined', streamId: rtmsStreamId, meetingUuid });
      } else {
        console.error('[Talking Point] media handshake failed', msg.status_code, msg.reason);
      }
      return;
    }

    if (msg.msg_type === 12) {
      mediaWs.send(JSON.stringify({ msg_type: 13, timestamp: msg.timestamp }));
      return;
    }

    if (msg.msg_type === 17 && msg.content) {
      const content = msg.content;
      const segment = {
        type: 'transcript',
        streamId: rtmsStreamId,
        meetingUuid,
        userId: content.user_id ?? null,
        speaker: content.user_name || 'Speaker',
        startTime: content.start_time ?? null,
        endTime: content.end_time ?? null,
        timestamp: content.timestamp ?? Date.now(),
        language: content.language ?? null,
        text: String(content.data || '').trim()
      };

      if (!segment.text) return;
      const history = transcripts.get(rtmsStreamId) || [];
      history.push(segment);
      if (history.length > 500) history.shift();
      transcripts.set(rtmsStreamId, history);
      broadcast(segment);
    }
  });

  mediaWs.on('error', error => {
    console.error('[Talking Point] media websocket error', error.message);
  });
}

function connectToRtms(payload) {
  const meetingUuid = payload?.meeting_uuid;
  const rtmsStreamId = payload?.rtms_stream_id;
  const serverUrls = payload?.server_urls;

  if (!meetingUuid || !rtmsStreamId || !serverUrls) return;
  if (!ZOOM_CLIENT_ID || !ZOOM_CLIENT_SECRET) {
    console.error('[Talking Point] Zoom credentials are not configured');
    return;
  }
  if (activeStreams.has(rtmsStreamId)) return;

  const signalingWs = new WebSocket(serverUrls);
  const streamState = { meetingUuid, rtmsStreamId, signaling: signalingWs, media: null };
  activeStreams.set(rtmsStreamId, streamState);
  transcripts.set(rtmsStreamId, []);

  signalingWs.on('open', () => {
    signalingWs.send(JSON.stringify({
      msg_type: 1,
      protocol_version: 1,
      sequence: 1,
      meeting_uuid: meetingUuid,
      rtms_stream_id: rtmsStreamId,
      signature: generateRtmsSignature(meetingUuid, rtmsStreamId),
      buffer_data: false
    }));
  });

  signalingWs.on('message', data => {
    let msg;
    try { msg = JSON.parse(data.toString()); } catch { return; }

    if (msg.msg_type === 2) {
      if (msg.status_code !== 0) {
        console.error('[Talking Point] signaling handshake failed', msg.status_code, msg.reason);
        return;
      }
      const mediaUrl =
        msg?.media_server?.server_urls?.transcript ||
        msg?.media_server?.server_urls?.all;
      if (!mediaUrl) {
        console.error('[Talking Point] no transcript media URL returned');
        return;
      }
      connectMedia(mediaUrl, meetingUuid, rtmsStreamId, signalingWs, streamState);
      return;
    }

    if (msg.msg_type === 12) {
      signalingWs.send(JSON.stringify({ msg_type: 13, timestamp: msg.timestamp }));
    }
  });

  signalingWs.on('error', error => {
    console.error('[Talking Point] signaling websocket error', error.message);
  });

  signalingWs.on('close', () => {
    const current = activeStreams.get(rtmsStreamId);
    if (current?.signaling === signalingWs) activeStreams.delete(rtmsStreamId);
  });
}

app.get('/', (_req, res) => {
  res.json({
    name: 'Talking Point RTMS',
    ok: true,
    transport: 'zoom-websocket',
    zoomConfigured: Boolean(ZOOM_CLIENT_ID && ZOOM_CLIENT_SECRET),
    webhookVerificationConfigured: Boolean(ZOOM_WEBHOOK_SECRET_TOKEN)
  });
});

app.get('/health', (_req, res) => {
  res.json({
    ok: true,
    activeStreams: activeStreams.size,
    browserClients: browserClients.size
  });
});

app.post('/zoom/webhook', (req, res) => {
  const event = req.body?.event;
  const payload = req.body?.payload || {};

  if (event === 'endpoint.url_validation') {
    const plainToken = payload?.plainToken || '';
    if (!ZOOM_WEBHOOK_SECRET_TOKEN) {
      return res.status(503).json({ error: 'ZOOM_WEBHOOK_SECRET_TOKEN is not configured' });
    }
    const encryptedToken = crypto
      .createHmac('sha256', ZOOM_WEBHOOK_SECRET_TOKEN)
      .update(plainToken)
      .digest('hex');
    return res.json({ plainToken, encryptedToken });
  }

  if (!verifyWebhookSignature(req)) {
    return res.status(401).json({ error: 'Invalid Zoom webhook signature' });
  }

  res.status(200).json({ received: true });

  if (event === 'meeting.rtms_started') {
    queueMicrotask(() => connectToRtms(payload));
  } else if (event === 'meeting.rtms_stopped') {
    queueMicrotask(() => closeStream(payload?.rtms_stream_id));
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
