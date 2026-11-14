/**
 * ABA PayWay Telegram Group Listener Bridge — Multi-Group Dynamic Sync Edition (listener.mjs)
 * Watches ALL configured Telegram Groups for @PayWayByABA_bot notifications (Indoor ABA PAY & Outdoor KHQR)
 * using your MTProto session and pushes parsed payments in parallel to:
 *   1. Firestore `system_settings/aba_live_feed` (0ms single-doc real-time stream for POS PaymentModal)
 *   2. Firestore `aba_payments/{trxId}` (persistent transaction ledger & claim guard)
 *   3. Built-in HTTP `/recent`, `/groups`, & `/events` (SSE) endpoints for instant diagnostics
 *
 * NEW FIX: Dynamically merges `process.env.PAYWAY_CHAT_ID` with all `paywayChatId` values configured
 * inside Firestore `shops` and `payway_secrets` collections so newly added branch groups work automatically!
 */
import http from "node:http";
import { TelegramClient } from "telegram";
import { StringSession } from "telegram/sessions/index.js";
import { NewMessage } from "telegram/events/index.js";
import { initializeApp, applicationDefault, cert } from "firebase-admin/app";
import { getFirestore } from "firebase-admin/firestore";

// 1. Initialize Firebase Admin SDK (Cloud Env or Local serviceAccountKey.json)
const serviceAccountEnv =
  process.env.FIREBASE_SERVICE_ACCOUNT || process.env.FIREBASE_SERVICE_ACCOUNT_JSON;
if (serviceAccountEnv) {
  try {
    const parsedCreds =
      typeof serviceAccountEnv === "string"
        ? JSON.parse(serviceAccountEnv)
        : serviceAccountEnv;
    initializeApp({ credential: cert(parsedCreds) });
  } catch (e) {
    console.error("Failed to parse Firebase service account JSON:", e.message);
    initializeApp({ credential: applicationDefault() });
  }
} else {
  initializeApp({ credential: applicationDefault() });
}

const db = getFirestore();
db.settings({ ignoreUndefinedProperties: true, preferRest: false });

// 2. Helper to normalize any Telegram Chat ID (strips -100 or - prefix for canonical comparison)
function normalizeChatId(idInput) {
  return String(idInput || "")
    .trim()
    .replace(/^-100/, "")
    .replace(/^-/, "")
    .trim();
}

// Default built-in list of all 8 Solaris ABA PayWay Telegram Groups (can be extended via .env or Firestore UI)
const DEFAULT_PAYWAY_CHAT_IDS = [
  "-1003938546235",
  "-1003861176164",
 
];

const rawEnvTargetChats = String(process.env.PAYWAY_CHAT_ID || "").trim();
const allowedChatIdsSet = new Set();

function registerAllowedChatIdsFromString(rawStr) {
  if (!rawStr) return;
  String(rawStr)
    .split(/[\s,|;]+/)
    .map(normalizeChatId)
    .filter(Boolean)
    .forEach((id) => allowedChatIdsSet.add(id));
}

// Seed from both DEFAULT_PAYWAY_CHAT_IDS and process.env.PAYWAY_CHAT_ID
registerAllowedChatIdsFromString(DEFAULT_PAYWAY_CHAT_IDS.join(","));
registerAllowedChatIdsFromString(rawEnvTargetChats);

// Dynamically sync `paywayChatId` from Firestore `shops` and `payway_secrets` so UI changes take effect without restarting
async function syncAllowedChatIdsFromFirestore() {
  try {
    const [shopsSnap, secretsSnap] = await Promise.all([
      db.collection("shops").get().catch(() => null),
      db.collection("payway_secrets").get().catch(() => null),
    ]);

    let addedCount = 0;
    const beforeSize = allowedChatIdsSet.size;

    if (shopsSnap) {
      shopsSnap.forEach((docSnap) => {
        const d = docSnap.data() || {};
        if (d.khqrConfig?.paywayChatId) {
          registerAllowedChatIdsFromString(d.khqrConfig.paywayChatId);
        }
      });
    }

    if (secretsSnap) {
      secretsSnap.forEach((docSnap) => {
        const d = docSnap.data() || {};
        if (d.paywayChatId) registerAllowedChatIdsFromString(d.paywayChatId);
        if (d.khqrConfigBackup?.paywayChatId) {
          registerAllowedChatIdsFromString(d.khqrConfigBackup.paywayChatId);
        }
      });
    }

    addedCount = allowedChatIdsSet.size - beforeSize;
    if (addedCount > 0) {
      console.log(
        `🔄 Synced +${addedCount} new PayWay Chat ID(s) from Firestore! Total watched groups: ${allowedChatIdsSet.size}`
      );
      lastDialogRefreshMs = 0; // Trigger dialog re-filter on next sweep
    }
  } catch (e) {
    console.warn("Firestore Chat ID sync warning:", e.message);
  }
}

const isChatAllowed = (chatIdStr) => {
  // If user explicitly sets PAYWAY_ALLOW_ALL_GROUPS=true, accept valid ABA PayWay messages from any group
  if (String(process.env.PAYWAY_ALLOW_ALL_GROUPS || "").toLowerCase() === "true") {
    return true;
  }
  if (allowedChatIdsSet.size === 0) return true;
  const stripped = normalizeChatId(chatIdStr);
  if (!stripped) return true;
  return allowedChatIdsSet.has(stripped);
};

// Pre-warm Firestore gRPC HTTP/2 channel & sync dynamic branch Chat IDs every 45s
async function prewarmFirestoreChannel() {
  try {
    await Promise.all([
      db
        .collection("system_settings")
        .doc("aba_live_feed")
        .set(
          {
            listenerHeartbeatAt: Date.now(),
            watchedChatIdsCount: allowedChatIdsSet.size,
            watchedChatIds: Array.from(allowedChatIdsSet),
          },
          { merge: true }
        ),
      syncAllowedChatIdsFromFirestore(),
    ]);
  } catch (e) {}
}
await prewarmFirestoreChannel();
setInterval(prewarmFirestoreChannel, 45000);

// 3. In-Memory Ring Buffer & SSE Clients for <5ms Instant Delivery
const recentPaymentsBuffer = [];
const sseClients = new Set();

function broadcastToSseClients(paymentObj) {
  const payload = `data: ${JSON.stringify(paymentObj)}\n\n`;
  for (const res of sseClients) {
    try {
      res.write(payload);
    } catch (e) {
      sseClients.delete(res);
    }
  }
}

// 4. HTTP Health-Check + Instant `/recent`, `/groups`, & `/events` SSE Server
const port = Number(process.env.PORT || 3000);
http
  .createServer((req, res) => {
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Methods", "GET, OPTIONS");
    res.setHeader("Access-Control-Allow-Headers", "Content-Type");

    if (req.method === "OPTIONS") {
      res.writeHead(204);
      res.end();
      return;
    }

    if (req.url === "/events") {
      res.writeHead(200, {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache, no-transform",
        Connection: "keep-alive",
      });
      res.write(`data: ${JSON.stringify({ type: "CONNECTED", ts: Date.now() })}\n\n`);
      sseClients.add(res);
      req.on("close", () => sseClients.delete(res));
      return;
    }

    if (req.url === "/recent") {
      res.writeHead(200, { "Content-Type": "application/json", "Cache-Control": "no-store" });
      res.end(
        JSON.stringify({
          ok: true,
          watchedChatIds: Array.from(allowedChatIdsSet),
          payments: recentPaymentsBuffer.slice(0, 30),
        })
      );
      return;
    }

    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(
      JSON.stringify({
        status: "ok",
        service: "solaris-aba-listener-v2.1-multigroup",
        uptimeSeconds: Math.round(process.uptime()),
        watchedChatIdsCount: allowedChatIdsSet.size,
        watchedChatIds: Array.from(allowedChatIdsSet),
        cachedPayments: recentPaymentsBuffer.length,
      })
    );
  })
  .listen(port, () => {
    console.log(`🌐 Listener HTTP & SSE server active on port ${port}`);
  });

// 5. Minute-Granularity Timestamp Alignment Helper
function parseAbaPaidAtToMs(paidAtStr, referenceMs = Date.now()) {
  if (!paidAtStr || typeof paidAtStr !== "string") return referenceMs;
  const clean = paidAtStr.trim();
  const refDate = new Date(referenceMs);
  const currentYear = refDate.getFullYear();

  const match = clean.match(
    /^([A-Za-z]{3,9})\s+(\d{1,2})(?:,\s*(\d{4}))?,?\s+(\d{1,2}):(\d{2})(?::(\d{2}))?\s*(AM|PM)?$/i
  );
  if (match) {
    const months = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"];
    const mIdx = months.indexOf(match[1].slice(0, 3).toLowerCase());
    if (mIdx !== -1) {
      const day = parseInt(match[2], 10);
      const year = match[3] ? parseInt(match[3], 10) : currentYear;
      let hours = parseInt(match[4], 10);
      const mins = parseInt(match[5], 10);
      const hasSecs = !!match[6];
      const secs = hasSecs ? parseInt(match[6], 10) : 0;
      const ampm = (match[7] || "").toUpperCase();

      if (ampm === "PM" && hours < 12) hours += 12;
      if (ampm === "AM" && hours === 12) hours = 0;

      const constructed = new Date(year, mIdx, day, hours, mins, secs, 0);
      if (!isNaN(constructed.getTime())) {
        if (!hasSecs) {
          const diffMs = referenceMs - constructed.getTime();
          if (diffMs >= 0 && diffMs < 60000) return referenceMs;
          return constructed.getTime() + 59000;
        }
        return constructed.getTime();
      }
    }
  }
  return referenceMs;
}

// 6. Unified Indoor ABA PAY + Outdoor Bank KHQR Parser
function parseABAPayWay(raw, msgDateMs = Date.now()) {
  if (!raw || typeof raw !== "string") return null;
  const t = raw.replace(/[\r\n\t]+/g, " ").replace(/\s+/g, " ").trim();

  const strictRegex =
    /(?:(៛|\$|KHR|USD)\s*([0-9,]+(?:\.[0-9]{1,2})?))\s+paid by\s+(.+?)(?:\s*\(\s*\*\s*([0-9A-Za-z]+)\s*\))?\s+on\s+([A-Za-z]{3,9}\s+\d{1,2}(?:,\s*\d{4})?,?\s+\d{1,2}:\d{2}(?::\d{2})?\s*(?:AM|PM)?|\d{1,2}:\d{2}(?::\d{2})?\s*(?:AM|PM)?)\s+via\s+(.+?)\s+at\s+(.+?)(?:\.\s*|\s+)Trx\.?\s*ID\s*[:#]?\s*([0-9A-Za-z]+)[,\s]+APV\s*[:#]?\s*([0-9A-Za-z]+)/i;

  const m = t.match(strictRegex);
  if (!m) return null;

  const cur = m[1] === "៛" || /^khr$/i.test(m[1]) ? "KHR" : "USD";
  const amount = parseFloat(m[2].replace(/,/g, ""));
  if (isNaN(amount) || amount <= 0) return null;

  const payerName = m[3].replace(/\(\s*\*\s*[0-9A-Za-z]+\s*\)\s*$/, "").trim();
  const maskedAccount = m[4] ? `*${m[4].trim()}` : "*ABA";
  const paidAtStr = m[5].trim();
  const paymentChannel = m[6].trim();
  const bankParenMatch = paymentChannel.match(/ABA\s*KHQR\s*\(\s*([^)]+)\s*\)/i);
  const isOutdoor = /ABA\s*KHQR/i.test(paymentChannel) || !!bankParenMatch;
  const sourceBank = bankParenMatch
    ? bankParenMatch[1].trim()
    : isOutdoor
    ? paymentChannel.replace(/^ABA\s*KHQR\s*/i, "").replace(/[()]/g, "").trim() || "External Bank"
    : "ABA Bank";

  const merchantName = m[7].replace(/\.$/, "").trim();
  const trxId = m[8].trim();
  const apv = m[9].replace(/\.$/, "").trim();

  return {
    currency: cur,
    amount,
    payerName,
    maskedAccount,
    paidAtStr,
    paidTimestamp: parseAbaPaidAtToMs(paidAtStr, msgDateMs),
    paymentChannel,
    paymentCategory: isOutdoor ? "OUTDOOR_KHQR" : "INDOOR_ABA",
    sourceBank,
    merchantName,
    trxId,
    apv,
    rawText: t,
  };
}

const apiId = Number(process.env.TG_API_ID);
const apiHash = String(process.env.TG_API_HASH || "").trim();
const sessionStr = String(process.env.TG_SESSION || "").trim();

if (!apiId || !apiHash || !sessionStr) {
  console.error("Missing TG_API_ID, TG_API_HASH, or TG_SESSION in environment variables.");
  process.exit(1);
}

const savedTrxMemory = new Set();

// 7. Lock-First Parallel Dual-Write to Firestore (`aba_payments` + `system_settings/aba_live_feed`)
async function saveParsedPaymentToFirestore(parsed, chatIdStr) {
  if (!parsed || !parsed.trxId) return false;
  const trxId = String(parsed.trxId).trim();

  if (savedTrxMemory.has(trxId)) return false;
  savedTrxMemory.add(trxId);

  const writeStartMs = performance.now();
  const nowMs = Date.now();
  const cleanChatId = String(chatIdStr || "").trim();
  const docPayload = {
    ...parsed,
    chatId: cleanChatId,
    receivedAt: nowMs,
    claimedByOrderId: null,
  };

  recentPaymentsBuffer.unshift(docPayload);
  if (recentPaymentsBuffer.length > 50) recentPaymentsBuffer.pop();
  broadcastToSseClients(docPayload);

  try {
    await Promise.all([
      db.collection("aba_payments").doc(trxId).set(docPayload, { merge: true }),
      db
        .collection("system_settings")
        .doc("aba_live_feed")
        .set(
          {
            lastUpdated: nowMs,
            latestPayment: docPayload,
            [`payments.${trxId}`]: docPayload,
          },
          { merge: true }
        ),
    ]);

    const elapsedMs = Math.round(performance.now() - writeStartMs);
    console.log(
      `⚡ [ABA VERIFIED -> CLOUD in ${elapsedMs}ms] Chat: ${cleanChatId} | Trx: ${trxId} | ${parsed.currency} ${parsed.amount} | ${parsed.payerName} (${parsed.maskedAccount}) via ${parsed.paymentChannel} at ${parsed.merchantName} | APV: ${parsed.apv}`
    );
    return true;
  } catch (e) {
    savedTrxMemory.delete(trxId);
    console.error("Firestore write error:", e.message || e);
    return false;
  }
}

// 8. Use Native TCP Transport in Node.js (useWSS: false) for lowest latency
const client = new TelegramClient(new StringSession(sessionStr), apiId, apiHash, {
  connectionRetries: 10,
  retryDelay: 1000,
  autoReconnect: true,
  useWSS: false,
});

client.setLogLevel("error");

await client.connect();
console.log("==================================================================");
console.log("⚡ Connected to Telegram MTProto (Ultra-Fast Multi-Group Mode)!");
console.log(
  `   Watching ${allowedChatIdsSet.size} Normalized PayWay Chat ID(s):`,
  Array.from(allowedChatIdsSet).join(", ")
);
console.log("==================================================================");

// 9A. REAL-TIME EVENT LISTENER
client.addEventHandler(async (ev) => {
  try {
    const rawMessage = ev.message?.message || ev.message?.text || "";
    if (!rawMessage) return;

    const peer = ev.message?.peerId;
    const rawPeerId =
      ev.chatId ||
      ev.message?.chatId ||
      (peer?.channelId ? `-100${peer.channelId}` : peer?.chatId ? `-${peer.chatId}` : "");

    const incomingChatId = String(rawPeerId || "");

    const parsed = parseABAPayWay(rawMessage, Date.now());
    if (!parsed) return;

    if (!isChatAllowed(incomingChatId)) {
      console.log(
        `ℹ️ [UNLISTED GROUP ABA DETECTED - AUTO-ADDING] Chat ID: ${incomingChatId} | Trx: ${parsed.trxId}`
      );
      registerAllowedChatIdsFromString(incomingChatId);
    }

    await saveParsedPaymentToFirestore(parsed, incomingChatId);
  } catch (err) {}
}, new NewMessage({}));

// 9B. RAW MTPROTO UPDATE FALLBACK
client.addEventHandler(async (update) => {
  try {
    const msgObj = update?.message || update?.newMessage?.message;
    const rawText = typeof msgObj === "string" ? msgObj : msgObj?.message || msgObj?.text || "";
    if (!rawText) return;

    const peer = msgObj?.peerId || update?.peerId;
    const rawPeerId = peer?.channelId
      ? `-100${peer.channelId}`
      : peer?.chatId
      ? `-${peer.chatId}`
      : "";

    const parsed = parseABAPayWay(rawText, Date.now());
    if (!parsed) return;

    if (!isChatAllowed(rawPeerId)) {
      registerAllowedChatIdsFromString(rawPeerId);
    }

    await saveParsedPaymentToFirestore(parsed, rawPeerId);
  } catch (err) {}
});

// 10. FAST PARALLEL MULTI-GROUP SWEEP (Scans up to 100 dialogs so all 8+ groups are always included!)
let cachedTargetDialogs = [];
let lastDialogRefreshMs = 0;
let isSweeping = false;

async function syncRecentGroupHistory(isInitialBoot = false) {
  if (isSweeping) return;
  isSweeping = true;
  try {
    const now = Date.now();
    // Refresh dialog list every 3 minutes or on boot (increased limit from 20 -> 100 so no PayWay group is missed!)
    if (isInitialBoot || cachedTargetDialogs.length === 0 || now - lastDialogRefreshMs > 180000) {
      const dialogs = await client.getDialogs({ limit: 100 });
      cachedTargetDialogs = dialogs.filter((d) => {
        const dialogIdStr = String(d.id || "");
        const titleLower = String(d.title || d.name || "").toLowerCase();
        const isKeywordMatch =
          titleLower.includes("khqr") ||
          titleLower.includes("payway") ||
          titleLower.includes("aba") ||
          titleLower.includes("slr") ||
          titleLower.includes("solaris");
        return (d.isGroup || d.isChannel) && (isChatAllowed(dialogIdStr) || isKeywordMatch);
      });
      lastDialogRefreshMs = now;
      if (isInitialBoot) {
        console.log(
          `📡 Active Dialog Sweep locked onto ${cachedTargetDialogs.length} Telegram Group(s):`,
          cachedTargetDialogs.map((d) => `${d.title} (${d.id})`).join(" | ")
        );
      }
    }

    await Promise.all(
      cachedTargetDialogs.map(async (d) => {
        try {
          const dialogIdStr = String(d.id || "");
          const messages = await client.getMessages(d.entity, { limit: isInitialBoot ? 12 : 4 });
          for (const msg of messages) {
            const text = msg?.message || msg?.text || "";
            if (!text) continue;
            const msgServerMs = msg.date ? Number(msg.date) * 1000 : Date.now();
            const effectiveMs = Math.abs(Date.now() - msgServerMs) < 120000 ? Date.now() : msgServerMs;
            const parsed = parseABAPayWay(text, effectiveMs);
            if (parsed) {
              await saveParsedPaymentToFirestore(parsed, dialogIdStr);
            }
          }
        } catch (innerErr) {}
      })
    );
  } catch (err) {
    if (!/RPCError|INTERNAL|FLOOD/i.test(err.message || "")) {
      console.warn("History sweep warning:", err.message || err);
    }
  } finally {
    isSweeping = false;
  }
}

await syncRecentGroupHistory(true);
setInterval(() => syncRecentGroupHistory(false), 3500);
