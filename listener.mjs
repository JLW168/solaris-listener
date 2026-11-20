/**
 * ABA PayWay Telegram Group Listener Bridge — Strict Whitelist & Zero-Overlap Edition v2.4 (listener.mjs)
 * Synchronized 1:1 with Solaris Cafe `index.html` Application Workspace.
 *
 * Root-Cause Fixes for Render `RPCError: 406: AUTH_KEY_DUPLICATED`:
 *   1. Firestore Lease Handoff (`system_settings/aba_listener_lease`):
 *      During Render zero-downtime deploys, the new container signals the old container via Firestore
 *      to disconnect from Telegram FIRST before the new container calls `client.connect()`.
 *   2. Graceful `SIGTERM` / `SIGINT` Shutdown:
 *      Cleanly disconnects the MTProto socket when Render stops a container.
 *   3. Session Revocation Guard (No Crash Loops):
 *      Detects `AUTH_KEY_DUPLICATED` / `AUTH_KEY_UNREGISTERED` / `SESSION_REVOKED`, reports the exact
 *      status to Firestore `system_settings/aba_live_feed` and `/`, and avoids hammering Telegram.
 *   4. Reduced Background Sweep Pressure:
 *      Relies on instant `NewMessage` push events with a safe 15s fallback sweep to avoid FLOOD_WAIT.
 */
import http from "node:http";
import crypto from "node:crypto";
import { TelegramClient } from "telegram";
import { StringSession } from "telegram/sessions/index.js";
import { NewMessage } from "telegram/events/index.js";
import { initializeApp, applicationDefault, cert } from "firebase-admin/app";
import { getFirestore } from "firebase-admin/firestore";

const INSTANCE_ID = `${process.env.RENDER_INSTANCE_ID || "local"}-${Date.now()}-${crypto.randomBytes(3).toString("hex")}`;
const LISTENER_VERSION = "v2.4-zero-overlap";

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

// Track runtime state for health checks and graceful shutdown
let listenerStatus = "BOOTING"; // BOOTING | HANDOFF_WAIT | CONNECTED | SESSION_REVOKED | DISCONNECTED_HANDOFF | ERROR
let lastErrorMsg = null;
let isShuttingDown = false;
let client = null;
let heartbeatInterval = null;
let sweepInterval = null;
let leaseUnsub = null;

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

registerAllowedChatIdsFromString(DEFAULT_PAYWAY_CHAT_IDS.join(","));
registerAllowedChatIdsFromString(rawEnvTargetChats);

let lastDialogRefreshMs = 0;

// Dynamic sync from Firestore `shops` and `payway_secrets`
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
      lastDialogRefreshMs = 0;
    }
  } catch (e) {
    console.warn("Firestore Chat ID sync warning:", e.message);
  }
}

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
const liveFeedMapMemory = {};
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

// 4. Start HTTP Health-Check & SSE Server IMMEDIATELY (so Render port-binding succeeds before handoff wait)
const port = Number(process.env.PORT || 3000);
const httpServer = http
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
      res.write(`data: ${JSON.stringify({ type: "CONNECTED", status: listenerStatus, ts: Date.now() })}\n\n`);
      sseClients.add(res);
      req.on("close", () => sseClients.delete(res));
      return;
    }

    if (req.url === "/recent") {
      res.writeHead(200, { "Content-Type": "application/json", "Cache-Control": "no-store" });
      res.end(
        JSON.stringify({
          ok: listenerStatus === "CONNECTED",
          status: listenerStatus,
          lastError: lastErrorMsg,
          watchedChatIds: Array.from(allowedChatIdsSet),
          payments: recentPaymentsBuffer.slice(0, 30),
        })
      );
      return;
    }

    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(
      JSON.stringify({
        status: listenerStatus === "CONNECTED" ? "ok" : listenerStatus.toLowerCase(),
        listenerStatus,
        lastError: lastErrorMsg,
        instanceId: INSTANCE_ID,
        service: `solaris-aba-listener-${LISTENER_VERSION}`,
        uptimeSeconds: Math.round(process.uptime()),
        watchedChatIdsCount: allowedChatIdsSet.size,
        watchedChatIds: Array.from(allowedChatIdsSet),
        cachedPayments: recentPaymentsBuffer.length,
      })
    );
  })
  .listen(port, () => {
    console.log(`🌐 Listener HTTP & SSE server active on port ${port} (Instance: ${INSTANCE_ID})`);
  });

// 5. Status & Heartbeat Publisher to Firestore
async function publishListenerStatus(statusOverride = null, errorOverride = null) {
  if (statusOverride) listenerStatus = statusOverride;
  if (errorOverride !== null) lastErrorMsg = errorOverride;
  try {
    await db
      .collection("system_settings")
      .doc("aba_live_feed")
      .set(
        {
          listenerHeartbeatAt: Date.now(),
          listenerVersion: LISTENER_VERSION,
          listenerInstanceId: INSTANCE_ID,
          listenerStatus,
          listenerLastError: lastErrorMsg || null,
          watchedChatIdsCount: allowedChatIdsSet.size,
          watchedChatIds: Array.from(allowedChatIdsSet).map((id) => `-100${id}`),
        },
        { merge: true }
      );
  } catch (e) {}
}

async function prewarmFirestoreChannel(isInitial = false) {
  try {
    const tasks = [publishListenerStatus(), syncAllowedChatIdsFromFirestore()];

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

// 6. Minute-Granularity Timestamp Alignment Helper (Synchronized with index.html)
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

// 7. Unified Indoor ABA PAY + Outdoor Bank KHQR Parser (Synchronized 1:1 with index.html)
function parseABAPayWay(raw, msgDateMs = Date.now()) {
  if (!raw || typeof raw !== "string") return null;
  const t = raw.replace(/[\r\n\t]+/g, " ").replace(/\s+/g, " ").trim();

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

// 8. Lock-First Parallel Dual-Write to Firestore (`aba_payments` + `system_settings/aba_live_feed`)
async function saveParsedPaymentToFirestore(parsed, chatIdStr, msgDateMs = Date.now()) {
  if (!parsed || !parsed.trxId) return false;
  if (!isChatAllowed(chatIdStr)) return false;

  const trxId = String(parsed.trxId).trim();
  if (savedTrxMemory.has(trxId)) return false;
  savedTrxMemory.add(trxId);

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

// 9. Graceful Disconnect & Shutdown Routine (Prevents AUTH_KEY_DUPLICATED on Render rolling deploys)
async function disconnectTelegramCleanly(reason = "SHUTDOWN") {
  if (isShuttingDown) return;
  isShuttingDown = true;

  if (heartbeatInterval) clearInterval(heartbeatInterval);
  if (sweepInterval) clearInterval(sweepInterval);
  if (typeof leaseUnsub === "function") {
    leaseUnsub();
    leaseUnsub = null;
  }

  console.log(`🛑 Disconnecting Telegram MTProto cleanly (${reason})...`);
  try {
    if (client) {
      await client.disconnect();
      client = null;
    }
  } catch (e) {}

  try {
    await db
      .collection("system_settings")
      .doc("aba_listener_lease")
      .set(
        {
          lastDisconnectedInstanceId: INSTANCE_ID,
          lastDisconnectedAt: Date.now(),
        },
        { merge: true }
      );
  } catch (e) {}
}

process.on("SIGTERM", async () => {
  await disconnectTelegramCleanly("RENDER_SIGTERM");
  httpServer.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 2000);
});

process.on("SIGINT", async () => {
  await disconnectTelegramCleanly("SIGINT");
  httpServer.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 2000);
});

// Catch fatal GramJS auth errors cleanly without infinite crash loops
async function handleFatalAuthError(err) {
  const msg = String(err?.errorMessage || err?.message || err || "");
  if (/AUTH_KEY_DUPLICATED|AUTH_KEY_UNREGISTERED|SESSION_REVOKED|USER_DEACTIVATED/i.test(msg)) {
    console.error("==================================================================");
    console.error(`❌ TELEGRAM SESSION ERROR: ${msg}`);
    console.error("   ACTION REQUIRED:");
    console.error("   1. Generate a brand-new TG_SESSION by running `node login.mjs` ONCE.");
    console.error("   2. Update TG_SESSION in Render Environment Variables and Redeploy.");
    console.error("   3. NEVER use the same TG_SESSION in a browser or second server at the same time.");
    console.error("==================================================================");
    await publishListenerStatus("SESSION_REVOKED", `${msg}: Generate a fresh TG_SESSION with login.mjs and update Render env.`);
    if (heartbeatInterval) clearInterval(heartbeatInterval);
    if (sweepInterval) clearInterval(sweepInterval);
    try {
      if (client) await client.disconnect();
    } catch (e) {}
    return true;
  }
  return false;
}

process.on("unhandledRejection", async (reason) => {
  const handled = await handleFatalAuthError(reason);
  if (!handled) console.error("Unhandled Promise Rejection:", reason);
});

process.on("uncaughtException", async (err) => {
  const handled = await handleFatalAuthError(err);
  if (!handled) console.error("Uncaught Exception:", err);
});

// 10. Zero-Overlap Render Deploy Handoff via Firestore Lease
async function acquireSingleActiveLease() {
  const leaseRef = db.collection("system_settings").doc("aba_listener_lease");
  listenerStatus = "HANDOFF_WAIT";

  let previousInstanceId = null;
  let previousWasRecent = false;
  try {
    const snap = await leaseRef.get();
    if (snap.exists) {
      const d = snap.data() || {};
      if (d.activeInstanceId && d.activeInstanceId !== INSTANCE_ID) {
        previousInstanceId = d.activeInstanceId;
        previousWasRecent = Date.now() - Number(d.updatedAt || 0) < 120000;
      }
    }
  } catch (e) {}

  // Claim the active lease so any older Render container disconnects immediately
  await leaseRef.set(
    {
      activeInstanceId: INSTANCE_ID,
      updatedAt: Date.now(),
    },
    { merge: true }
  );

  // If a previous container was recently active, wait up to 4.5s for it to confirm disconnect
  if (previousInstanceId && previousWasRecent) {
    console.log(`⏳ Waiting for previous instance (${previousInstanceId}) to release Telegram session...`);
    const waitStart = Date.now();
    while (Date.now() - waitStart < 4500) {
      await new Promise((r) => setTimeout(r, 500));
      try {
        const checkSnap = await leaseRef.get();
        if (checkSnap.exists && checkSnap.data()?.lastDisconnectedInstanceId === previousInstanceId) {
          console.log(`✅ Previous instance (${previousInstanceId}) disconnected cleanly in ${Date.now() - waitStart}ms!`);
          break;
        }
      } catch (e) {}
    }
  }

  // Listen for future newer deployments taking over the lease
  leaseUnsub = leaseRef.onSnapshot(
    async (snap) => {
      if (!snap.exists || isShuttingDown) return;
      const d = snap.data() || {};
      if (d.activeInstanceId && d.activeInstanceId !== INSTANCE_ID) {
        console.log(`🔁 Newer instance (${d.activeInstanceId}) took over lease. Yielding Telegram connection...`);
        listenerStatus = "DISCONNECTED_HANDOFF";
        await disconnectTelegramCleanly("LEASE_YIELDED_TO_NEW_DEPLOY");
      }
    },
    () => {}
  );
}

// 11. Main Boot Sequence
const apiId = Number(process.env.TG_API_ID);
const apiHash = String(process.env.TG_API_HASH || "").trim();
const sessionStr = String(process.env.TG_SESSION || "").trim();

if (!apiId || !apiHash || !sessionStr) {
  console.error("Missing TG_API_ID, TG_API_HASH, or TG_SESSION in environment variables.");
  await publishListenerStatus("ERROR", "Missing TG_API_ID, TG_API_HASH, or TG_SESSION.");
} else {
  await prewarmFirestoreChannel(true);
  await acquireSingleActiveLease();

  if (!isShuttingDown) {
    client = new TelegramClient(new StringSession(sessionStr), apiId, apiHash, {
      connectionRetries: 5,
      retryDelay: 2000,
      autoReconnect: true,
      useWSS: false,
    });

    client.setLogLevel("error");

    try {
      await client.connect();
      const isAuth = await client.checkAuthorization();
      if (!isAuth) {
        throw new Error("AUTH_KEY_UNREGISTERED: TG_SESSION is not authorized.");
      }

      await publishListenerStatus("CONNECTED", "");
      console.log("==================================================================");
      console.log(`⚡ Connected to Telegram MTProto (${LISTENER_VERSION} | Instance: ${INSTANCE_ID})!`);
      console.log(
        `   Strictly Watching ${allowedChatIdsSet.size} Normalized PayWay Chat ID(s):`,
        Array.from(allowedChatIdsSet).join(", ")
      );
      console.log("==================================================================");

      // REAL-TIME EVENT LISTENER (Instant Push)
      client.addEventHandler(async (ev) => {
        try {
          if (isShuttingDown) return;
          const rawMessage = ev.message?.message || ev.message?.text || "";
          if (!rawMessage) return;

          const peer = ev.message?.peerId;
          const rawPeerId =
            ev.chatId ||
            ev.message?.chatId ||
            (peer?.channelId ? `-100${peer.channelId}` : peer?.chatId ? `-${peer.chatId}` : "");

          const incomingChatId = String(rawPeerId || "");
          if (!isChatAllowed(incomingChatId)) return;

          const msgServerMs = ev.message?.date ? Number(ev.message.date) * 1000 : Date.now();
          const effectiveMs = Math.abs(Date.now() - msgServerMs) < 120000 ? Date.now() : msgServerMs;

          const parsed = parseABAPayWay(rawMessage, effectiveMs);
          if (!parsed) return;

          await saveParsedPaymentToFirestore(parsed, incomingChatId, effectiveMs);
        } catch (err) {}
      }, new NewMessage({}));

      // RAW MTPROTO UPDATE FALLBACK
      client.addEventHandler(async (update) => {
        try {
          if (isShuttingDown) return;
          const msgObj = update?.message || update?.newMessage?.message;
          const rawText = typeof msgObj === "string" ? msgObj : msgObj?.message || msgObj?.text || "";
          if (!rawText) return;

          const peer = msgObj?.peerId || update?.peerId;
          const rawPeerId = peer?.channelId
            ? `-100${peer.channelId}`
            : peer?.chatId
            ? `-${peer.chatId}`
            : "";

          if (!isChatAllowed(rawPeerId)) return;

          const msgServerMs = msgObj?.date ? Number(msgObj.date) * 1000 : Date.now();
          const effectiveMs = Math.abs(Date.now() - msgServerMs) < 120000 ? Date.now() : msgServerMs;

          const parsed = parseABAPayWay(rawText, effectiveMs);
          if (!parsed) return;

          await saveParsedPaymentToFirestore(parsed, rawPeerId, effectiveMs);
        } catch (err) {}
      });

      // SAFETY-NET HISTORY SWEEP (Every 15s to prevent FLOOD_WAIT while catching any missed messages)
      let cachedTargetDialogs = [];
      let isSweeping = false;

      async function syncRecentGroupHistory(isInitialBoot = false) {
        if (isSweeping || isShuttingDown || !client) return;
        isSweeping = true;
        try {
          const now = Date.now();
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
                if (isShuttingDown || !client) return;
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
          const wasFatal = await handleFatalAuthError(err);
          if (!wasFatal && !/RPCError|INTERNAL|FLOOD/i.test(err.message || "")) {
            console.warn("History sweep warning:", err.message || err);
          }
        } finally {
          isSweeping = false;
        }
      }

      await syncRecentGroupHistory(true);
      sweepInterval = setInterval(() => syncRecentGroupHistory(false), 15000);
      heartbeatInterval = setInterval(() => prewarmFirestoreChannel(false), 45000);
    } catch (connectErr) {
      const handled = await handleFatalAuthError(connectErr);
      if (!handled) {
        console.error("Telegram connection error:", connectErr.message || connectErr);
        await publishListenerStatus("ERROR", connectErr.message || String(connectErr));
      }
    }
  }
}
