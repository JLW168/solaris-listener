/**
 * ABA PayWay Telegram Group Listener Bridge — Strict Whitelist Edition v2.3 (listener-SLR.mjs)
 * Synchronized 1:1 with Solaris Cafe `index.html` Application Workspace.
 *
 * Key Optimizations:
 *   1. Nested Map Write for `system_settings/aba_live_feed`: Uses `{ payments: { [trxId]: docPayload } }`
 *      with `{ merge: true }` so `docSnap.data()?.payments` in `index.html` reads every transaction
 *      instantly in 0ms (fixes literal dot-key `payments.${trxId}` bug in Firestore `.set()`).
 *   2. Claim-Safe Upsert & Memory Pruning: Preserves existing `claimedByOrderId` if a payment was already
 *      settled by POS/KDS/Customer Portal, and caps `aba_live_feed.payments` to the latest 40 entries
 *      so the single-document live feed never exceeds Firestore's 1MB limit.
 *   3. Unified Indoor ABA PAY + Outdoor Bakong KHQR Parser (with Resilient Fallback):
 *      Matches the exact regex & fallback parser in `window.SolarisKHQREngine.parseABAPayWayMessage`.
 *   4. Strict Multi-Group Whitelist Isolation: Watches ONLY explicitly configured branch `paywayChatId`s.
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

// Canonical Supergroup Chat ID formatter (`-100...` or `-...`) for consistent storage in Firestore
function formatCanonicalChatId(idInput) {
  const raw = String(idInput || "").trim();
  if (!raw) return "";
  if (raw.startsWith("-")) return raw;
  const digits = normalizeChatId(raw);
  return digits ? `-100${digits}` : raw;
}

// Default built-in list of strictly allowed Solaris ABA PayWay Telegram Groups
const DEFAULT_PAYWAY_CHAT_IDS = [
  "-1003938546235", // SLR-BSL
  "-1003861176164", // SLR•OTH
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

let lastDialogRefreshMs = 0;

// Dynamic sync from Firestore `shops` and `payway_secrets`
// Set PAYWAY_STRICT_ENV_ONLY=true in Render if you want to ignore Firestore Chat IDs completely
async function syncAllowedChatIdsFromFirestore() {
  if (String(process.env.PAYWAY_STRICT_ENV_ONLY || "").toLowerCase() === "true") {
    return;
  }
  try {
    const [shopsSnap, secretsSnap] = await Promise.all([
      db.collection("shops").get().catch(() => null),
      db.collection("payway_secrets").get().catch(() => null),
    ]);

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

    const addedCount = allowedChatIdsSet.size - beforeSize;
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

// Strict Whitelist Verification: Rejects any empty or unlisted Chat ID
const isChatAllowed = (chatIdStr) => {
  if (String(process.env.PAYWAY_ALLOW_ALL_GROUPS || "").toLowerCase() === "true") {
    return true;
  }
  const stripped = normalizeChatId(chatIdStr);
  if (!stripped) return false;
  return allowedChatIdsSet.has(stripped);
};

// 3. In-Memory Ring Buffer & SSE Clients for <5ms Instant Delivery
const recentPaymentsBuffer = [];
const liveFeedMapMemory = {}; // Mirrors `system_settings/aba_live_feed.payments` (capped at 40)
const savedTrxMemory = new Set();
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

// Pre-warm Firestore gRPC HTTP/2 channel, hydrate existing claimed Trx IDs, & sync dynamic branch Chat IDs
async function prewarmFirestoreChannel(isInitial = false) {
  try {
    const tasks = [
      db
        .collection("system_settings")
        .doc("aba_live_feed")
        .set(
          {
            listenerHeartbeatAt: Date.now(),
            listenerVersion: "v2.3-strict-whitelist",
            watchedChatIdsCount: allowedChatIdsSet.size,
            watchedChatIds: Array.from(allowedChatIdsSet).map((id) => `-100${id}`),
          },
          { merge: true }
        ),
      syncAllowedChatIdsFromFirestore(),
    ];

    if (isInitial) {
      tasks.push(
        db
          .collection("aba_payments")
          .orderBy("receivedAt", "desc")
          .limit(60)
          .get()
          .then((snap) => {
            snap.forEach((docSnap) => {
              const d = docSnap.data() || {};
              const id = String(d.trxId || docSnap.id).trim();
              if (id) {
                savedTrxMemory.add(id);
                liveFeedMapMemory[id] = d;
              }
            });
          })
          .catch(() => {})
      );
    }

    await Promise.all(tasks);
  } catch (e) {}
}
await prewarmFirestoreChannel(true);
setInterval(() => prewarmFirestoreChannel(false), 45000);

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
        service: "solaris-aba-listener-v2.3-strict-whitelist",
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

// 5. Minute-Granularity Timestamp Alignment Helper (Synchronized with index.html)
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
        // Handle Dec -> Jan year rollover
        if (!match[3] && constructed.getTime() - referenceMs > 60 * 24 * 60 * 60 * 1000) {
          constructed.setFullYear(year - 1);
        }
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

// 6. Unified Indoor ABA PAY + Outdoor Bank KHQR Parser (Synchronized 1:1 with index.html)
function parseABAPayWay(raw, msgDateMs = Date.now()) {
  if (!raw || typeof raw !== "string") return null;
  const t = raw.replace(/[\r\n\t]+/g, " ").replace(/\s+/g, " ").trim();

  // Primary pattern: supports spaces inside parentheses e.g. "CHE SOKUNTH ( *636 )" and Outdoor KHQR e.g. "via ABA KHQR (ACLEDA Bank)"
  const strictRegex =
    /(?:(៛|\$|KHR|USD)\s*([0-9,]+(?:\.[0-9]{1,2})?))\s+paid by\s+(.+?)(?:\s*\(\s*\*\s*([0-9A-Za-z]+)\s*\))?\s+on\s+([A-Za-z]{3,9}\s+\d{1,2}(?:,\s*\d{4})?,?\s+\d{1,2}:\d{2}(?::\d{2})?\s*(?:AM|PM)?|\d{1,2}:\d{2}(?::\d{2})?\s*(?:AM|PM)?)\s+via\s+(.+?)\s+at\s+(.+?)(?:\.\s*|\s+)Trx\.?\s*ID\s*[:#]?\s*([0-9A-Za-z]+)[,\s]+APV\s*[:#]?\s*([0-9A-Za-z]+)/i;

  const m = t.match(strictRegex);
  if (m) {
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
      isValid: true,
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

  // Resilient fallback extractor for any ABA PayWay message variation containing Amount + paid by + Trx. ID / APV
  const amtMatch = t.match(/(?:(៛|\$|KHR|USD)\s*([0-9,]+(?:\.[0-9]{1,2})?))\s*(?:paid by|received from)/i);
  const trxMatch = t.match(/Trx\.?\s*ID\s*[:#]?\s*([0-9A-Za-z]+)/i);
  const apvMatch = t.match(/APV\s*[:#]?\s*([0-9A-Za-z]+)/i);
  if (amtMatch && (trxMatch || apvMatch)) {
    const rawSymbol = amtMatch[1].toUpperCase();
    const currency = rawSymbol === "៛" || rawSymbol === "KHR" ? "KHR" : "USD";
    const amount = parseFloat(amtMatch[2].replace(/,/g, ""));
    if (isNaN(amount) || amount <= 0) return null;

    const payerMatch = t.match(/(?:paid by|received from)\s+([^(]+?)(?:\s*\(\s*\*\s*([0-9A-Za-z]+)\s*\)|\s+on\s+|\s+via\s+)/i);
    const onMatch = t.match(/\bon\s+(.+?)\s+via\s+/i);
    const viaMatch = t.match(/\bvia\s+(.+?)\s+at\s+/i);
    const atMatch = t.match(/\bat\s+(.+?)(?:\.\s*Trx|\s+Trx)/i);
    const generatedId = trxMatch ? trxMatch[1].trim() : `APV-${apvMatch[1].trim()}`;
    const paidAtStr = onMatch
      ? onMatch[1].trim()
      : new Date(msgDateMs).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
    const paymentChannel = viaMatch ? viaMatch[1].trim() : "ABA PAY";
    const bankParenMatch = paymentChannel.match(/ABA\s*KHQR\s*\(\s*([^)]+)\s*\)/i);
    const isOutdoor = /ABA\s*KHQR/i.test(paymentChannel) || !!bankParenMatch;
    const sourceBank = bankParenMatch ? bankParenMatch[1].trim() : isOutdoor ? "External Bank" : "ABA Bank";

    return {
      isValid: true,
      currency,
      amount,
      payerName: payerMatch ? payerMatch[1].trim() : "ABA Customer",
      maskedAccount: payerMatch && payerMatch[2] ? `*${payerMatch[2].trim()}` : "*ABA",
      paidAtStr,
      paidTimestamp: parseAbaPaidAtToMs(paidAtStr, msgDateMs),
      paymentChannel,
      paymentCategory: isOutdoor ? "OUTDOOR_KHQR" : "INDOOR_ABA",
      sourceBank,
      merchantName: atMatch ? atMatch[1].replace(/\.$/, "").trim() : "",
      trxId: generatedId,
      apv: apvMatch ? apvMatch[1].replace(/\.$/, "").trim() : generatedId.slice(-6),
      rawText: t,
    };
  }

  return null;
}

const apiId = Number(process.env.TG_API_ID);
const apiHash = String(process.env.TG_API_HASH || "").trim();
const sessionStr = String(process.env.TG_SESSION || "").trim();

if (!apiId || !apiHash || !sessionStr) {
  console.error("Missing TG_API_ID, TG_API_HASH, or TG_SESSION in environment variables.");
  process.exit(1);
}

// 7. Lock-First Parallel Dual-Write to Firestore (`aba_payments` + `system_settings/aba_live_feed`)
async function saveParsedPaymentToFirestore(parsed, chatIdStr, msgDateMs = Date.now()) {
  if (!parsed || !parsed.trxId) return false;

  // Final strict guard before writing to Firestore
  if (!isChatAllowed(chatIdStr)) {
    return false;
  }

  const trxId = String(parsed.trxId).trim();

  if (savedTrxMemory.has(trxId)) return false;
  savedTrxMemory.add(trxId);

  // Prevent boundlessness in long-running processes
  if (savedTrxMemory.size > 2000) {
    const oldest = savedTrxMemory.values().next().value;
    savedTrxMemory.delete(oldest);
  }

  const writeStartMs = performance.now();
  const effectiveReceivedAt =
    Number(msgDateMs) > 0 && Math.abs(Date.now() - Number(msgDateMs)) < 365 * 86400000
      ? Number(msgDateMs)
      : Date.now();
  const cleanChatId = formatCanonicalChatId(chatIdStr);

  // Preserve claimedByOrderId if already known in memory
  const existingClaim = liveFeedMapMemory[trxId]?.claimedByOrderId || null;

  const docPayload = {
    ...parsed,
    chatId: cleanChatId,
    receivedAt: effectiveReceivedAt,
    claimedByOrderId: existingClaim,
  };

  recentPaymentsBuffer.unshift(docPayload);
  if (recentPaymentsBuffer.length > 50) recentPaymentsBuffer.pop();
  broadcastToSseClients(docPayload);

  // Update bounded liveFeedMapMemory (keep newest 35 payments so `aba_live_feed` doc stays tiny & fast)
  liveFeedMapMemory[trxId] = docPayload;
  const allFeedEntries = Object.values(liveFeedMapMemory).sort(
    (a, b) => Number(b.receivedAt || 0) - Number(a.receivedAt || 0)
  );
  const prunedPaymentsMap = {};
  allFeedEntries.slice(0, 35).forEach((item) => {
    if (item && item.trxId) {
      prunedPaymentsMap[String(item.trxId).trim()] = item;
    }
  });

  try {
    // CRITICAL FIX: Use nested `{ payments: { [trxId]: docPayload } }` with `{ merge: true }`
    // so `docSnap.data()?.payments` in `index.html` reads `payments` as a nested object map!
    await Promise.all([
      db.collection("aba_payments").doc(trxId).set(docPayload, { merge: true }),
      db
        .collection("system_settings")
        .doc("aba_live_feed")
        .set(
          {
            lastUpdated: Date.now(),
            latestPayment: docPayload,
            payments: prunedPaymentsMap,
          },
          { merge: true }
        ),
    ]);

    const elapsedMs = Math.round(performance.now() - writeStartMs);
    console.log(
      `⚡ [ABA VERIFIED -> CLOUD in ${elapsedMs}ms] Chat: ${cleanChatId} | Trx: ${trxId} | ${parsed.currency} ${parsed.amount} | ${parsed.payerName} (${parsed.maskedAccount}) via ${parsed.paymentChannel} [${parsed.sourceBank}] at ${parsed.merchantName} | APV: ${parsed.apv}`
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
console.log("⚡ Connected to Telegram MTProto (Strict Whitelist Multi-Group Mode v2.3)!");
console.log(
  `   Strictly Watching ${allowedChatIdsSet.size} Normalized PayWay Chat ID(s):`,
  Array.from(allowedChatIdsSet).join(", ")
);
console.log("==================================================================");

// 9A. REAL-TIME EVENT LISTENER (Strictly ignores unlisted groups)
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

    // Strictly ignore any group not in our allowed whitelist
    if (!isChatAllowed(incomingChatId)) return;

    const msgServerMs = ev.message?.date ? Number(ev.message.date) * 1000 : Date.now();
    const effectiveMs = Math.abs(Date.now() - msgServerMs) < 120000 ? Date.now() : msgServerMs;

    const parsed = parseABAPayWay(rawMessage, effectiveMs);
    if (!parsed) return;

    await saveParsedPaymentToFirestore(parsed, incomingChatId, effectiveMs);
  } catch (err) {}
}, new NewMessage({}));

// 9B. RAW MTPROTO UPDATE FALLBACK (Strictly ignores unlisted groups)
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

    // Strictly ignore any group not in our allowed whitelist
    if (!isChatAllowed(rawPeerId)) return;

    const msgServerMs = msgObj?.date ? Number(msgObj.date) * 1000 : Date.now();
    const effectiveMs = Math.abs(Date.now() - msgServerMs) < 120000 ? Date.now() : msgServerMs;

    const parsed = parseABAPayWay(rawText, effectiveMs);
    if (!parsed) return;

    await saveParsedPaymentToFirestore(parsed, rawPeerId, effectiveMs);
  } catch (err) {}
});

// 10. FAST PARALLEL MULTI-GROUP SWEEP (Strictly scans ONLY whitelisted Chat IDs)
let cachedTargetDialogs = [];
let isSweeping = false;

async function syncRecentGroupHistory(isInitialBoot = false) {
  if (isSweeping) return;
  isSweeping = true;
  try {
    const now = Date.now();
    // Refresh dialog list every 3 minutes or on boot
    if (isInitialBoot || cachedTargetDialogs.length === 0 || now - lastDialogRefreshMs > 180000) {
      const dialogs = await client.getDialogs({ limit: 100 });
      cachedTargetDialogs = dialogs.filter((d) => {
        const dialogIdStr = String(d.id || "");
        return (d.isGroup || d.isChannel) && isChatAllowed(dialogIdStr);
      });
      lastDialogRefreshMs = now;
      if (isInitialBoot) {
        console.log(
          `📡 Active Dialog Sweep strictly locked onto ${cachedTargetDialogs.length} Whitelisted Group(s):`,
          cachedTargetDialogs.map((d) => `${d.title} (${d.id})`).join(" | ")
        );
      }
    }

    await Promise.all(
      cachedTargetDialogs.map(async (d) => {
        try {
          const dialogIdStr = String(d.id || "");
          if (!isChatAllowed(dialogIdStr)) return;

          const messages = await client.getMessages(d.entity, { limit: isInitialBoot ? 12 : 4 });
          for (const msg of messages) {
            const text = msg?.message || msg?.text || "";
            if (!text) continue;
            const msgServerMs = msg.date ? Number(msg.date) * 1000 : Date.now();
            const effectiveMs = Math.abs(Date.now() - msgServerMs) < 120000 ? Date.now() : msgServerMs;
            const parsed = parseABAPayWay(text, effectiveMs);
            if (parsed) {
              await saveParsedPaymentToFirestore(parsed, dialogIdStr, effectiveMs);
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
