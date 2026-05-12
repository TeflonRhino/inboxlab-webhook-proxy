export default async function handler(req, res) {
  if (req.method !== "POST") {
    return res.status(405).json({
      ok: false,
      message: "Method not allowed"
    });
  }

  try {
    const appsScriptUrl = process.env.GOOGLE_APPS_SCRIPT_WEBHOOK_URL;

    if (!appsScriptUrl) {
      console.error("Missing GOOGLE_APPS_SCRIPT_WEBHOOK_URL");

      return res.status(200).json({
        ok: true,
        received: true,
        source: "heartbeat",
        warning: "Missing Apps Script URL"
      });
    }

    const originalPayload = req.body || {};

    const payload = {
      source: "heartbeat",
      project: "inboxlab",
      ...originalPayload
    };

    await fetch(appsScriptUrl, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Webhook-Proxy-Source": "heartbeat",
        "X-Webhook-Proxy-Project": "inboxlab"
      },
      body: JSON.stringify(payload),
      redirect: "follow"
    });

    return res.status(200).json({
      ok: true,
      received: true,
      forwarded: true,
      source: "heartbeat",
      project: "inboxlab"
    });
  } catch (error) {
    console.error("Heartbeat webhook proxy error:", error);

    return res.status(200).json({
      ok: true,
      received: true,
      source: "heartbeat",
      project: "inboxlab",
      internalErrorLogged: true
    });
  }
}
