// ============================================================
// Binance WebSocket Listener V7
// Autenticación Ed25519 — WebSocket API sin listenKey
//
// Cambios desde V6:
//   - Firma Ed25519 para session.logon (requerida por Binance WS API)
//   - HMAC-SHA256 se mantiene solo para REST API (polling fallback)
//   - Variable BINANCE_PRIVATE_KEY en lugar de BINANCE_API_SECRET para WS
//   - Validación de BINANCE_PRIVATE_KEY al iniciar
// ============================================================

require("dotenv").config();

const WebSocket = require("ws");
const http = require("http");
const crypto = require("crypto");

// ============================================================
// CONFIGURACIÓN
// ============================================================

const WEBHOOK_URL = process.env.WEBHOOK_URL;
const IS_TESTNET = process.env.IS_TESTNET === "true";
const BINANCE_API_KEY = process.env.BINANCE_API_KEY;
const BINANCE_API_SECRET = process.env.BINANCE_API_SECRET; // Para REST/HMAC (polling)
const BINANCE_PRIVATE_KEY_RAW = process.env.BINANCE_PRIVATE_KEY; // Para WS/Ed25519

const CONFIG = {
  testnet: {
    REST_URL: "https://testnet.binance.vision",
    WS_API_URL: "wss://ws-api.testnet.binance.vision/ws-api/v3",
  },
  mainnet: {
    REST_URL: "https://api.binance.com",
    WS_API_URL: "wss://ws-api.binance.com:443/ws-api/v3",
  },
};

const CURRENT_CONFIG = IS_TESTNET ? CONFIG.testnet : CONFIG.mainnet;

// ============================================================
// VARIABLES GLOBALES
// ============================================================

let ws = null;
let pingInterval = null;
let pollingInterval = null;
let reconnectAttempts = 0;
let isAuthenticated = false;
let isSubscribed = false;
let lastKnownOrders = new Map();
let privateKey = null; // Objeto CryptoKey cargado al iniciar

const MAX_RECONNECT_ATTEMPTS = 10;
const PING_INTERVAL_MS = 2 * 60 * 1000; // 2 minutos

// ============================================================
// FIRMA Ed25519 — para WebSocket API (session.logon)
// ============================================================

/**
 * Carga la llave privada Ed25519 desde la variable de entorno.
 * BINANCE_PRIVATE_KEY puede tener \n literales (formato Railway/Render)
 * o saltos de línea reales. Ambos se normalizan aquí.
 */
function loadPrivateKey() {
  if (!BINANCE_PRIVATE_KEY_RAW) {
    throw new Error(
      "BINANCE_PRIVATE_KEY no está definida en las variables de entorno",
    );
  }

  // Normalizar: reemplazar \n literales por saltos de línea reales
  const pem = BINANCE_PRIVATE_KEY_RAW.replace(/\\n/g, "\n");

  // Verificar que tiene la estructura PEM correcta
  if (!pem.includes("-----BEGIN PRIVATE KEY-----")) {
    throw new Error(
      "BINANCE_PRIVATE_KEY no tiene formato PEM válido. " +
        "Debe comenzar con -----BEGIN PRIVATE KEY-----",
    );
  }

  try {
    return crypto.createPrivateKey({ key: pem, format: "pem" });
  } catch (err) {
    throw new Error(`Error cargando llave privada Ed25519: ${err.message}`);
  }
}

/**
 * Genera firma Ed25519 para el WebSocket API.
 * Los params se ordenan alfabéticamente antes de firmar.
 * @param {Object} params — parámetros del request (sin signature)
 * @returns {{ params: Object }} — params originales + signature en base64
 */
function buildWsSignedParams(params) {
  const sortedPayload = Object.keys(params)
    .sort()
    .map((k) => `${k}=${params[k]}`)
    .join("&");

  const signature = crypto
    .sign(null, Buffer.from(sortedPayload), privateKey)
    .toString("base64");

  return { params: { ...params, signature } };
}

// ============================================================
// FIRMA HMAC-SHA256 — para REST API (polling fallback)
// ============================================================

/**
 * Construye query string con percent-encoding correcto para Binance REST.
 * Cada clave y valor se encodea individualmente antes de firmar.
 */
function buildSignedQuery(params) {
  const queryString = Object.entries(params)
    .map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(v)}`)
    .join("&");

  const signature = crypto
    .createHmac("sha256", BINANCE_API_SECRET)
    .update(queryString)
    .digest("hex");

  return { queryString, signature };
}

// ============================================================
// FETCH CON TIMEOUT
// ============================================================

async function fetchWithTimeout(url, options = {}, ms = 8000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), ms);
  try {
    const res = await fetch(url, { ...options, signal: controller.signal });
    clearTimeout(timer);
    return res;
  } catch (err) {
    clearTimeout(timer);
    if (err.name === "AbortError") throw new Error(`Timeout (${ms}ms)`);
    throw err;
  }
}

// ============================================================
// WEBSOCKET — AUTENTICACIÓN Ed25519
// ============================================================

function connectWebSocket() {
  if (reconnectAttempts >= MAX_RECONNECT_ATTEMPTS) {
    console.error(
      `❌ Máximo de reintentos alcanzado (${MAX_RECONNECT_ATTEMPTS}). Deteniendo.`,
    );
    setTimeout(() => process.exit(1), 5000);
    return;
  }

  reconnectAttempts++;
  isAuthenticated = false;
  isSubscribed = false;

  console.log(
    `\n🔌 Conectando WebSocket API (intento ${reconnectAttempts})...`,
  );
  console.log(`   URL: ${CURRENT_CONFIG.WS_API_URL}`);

  ws = new WebSocket(CURRENT_CONFIG.WS_API_URL, {
    handshakeTimeout: 10000,
    perMessageDeflate: false,
  });

  // ── OPEN: autenticar con Ed25519 ─────────────────────────────────────────
  ws.on("open", () => {
    console.log("✅ WebSocket conectado — autenticando con Ed25519...");

    const timestamp = Date.now();
    const { params } = buildWsSignedParams({
      apiKey: BINANCE_API_KEY,
      timestamp,
    });

    ws.send(
      JSON.stringify({
        id: "session-login",
        method: "session.logon",
        params,
      }),
    );

    // Ping proactivo para mantener la conexión viva
    pingInterval = setInterval(() => {
      if (ws && ws.readyState === WebSocket.OPEN) {
        ws.ping();
        console.log("🏓 Ping enviado");
      }
    }, PING_INTERVAL_MS);
  });

  // ── MESSAGE ───────────────────────────────────────────────────────────────
  ws.on("message", async (data) => {
    let msg;
    try {
      msg = JSON.parse(data.toString());
    } catch {
      console.error(
        "❌ Mensaje no parseable:",
        data.toString().substring(0, 100),
      );
      return;
    }

    // Respuesta al session.logon
    if (msg.id === "session-login") {
      if (msg.status === 200) {
        console.log("✅ Sesión autenticada con Ed25519");
        isAuthenticated = true;
        reconnectAttempts = 0;
        subscribeUserDataStream();
      } else {
        console.error("❌ Error en autenticación:", JSON.stringify(msg.error));
        console.error("   Código:", msg.error?.code);
        console.error("   Mensaje:", msg.error?.msg);
        ws.close(1008, "Auth failed");
      }
      return;
    }

    // Respuesta a userDataStream.subscribe
    if (msg.id === "subscribe-user-data") {
      if (msg.status === 200) {
        console.log("✅ Suscrito al User Data Stream");
        console.log("   Escuchando eventos de órdenes...\n");
        isSubscribed = true;
        deactivateFallbackPolling();
      } else {
        console.error("❌ Error suscribiendo:", JSON.stringify(msg.error));
      }
      return;
    }

    // Respuesta a session.status
    if (msg.id === "session-status") {
      console.log("📊 Estado de sesión:", msg.result?.status || "desconocido");
      return;
    }

    // Eventos de usuario
    if (msg.e) {
      await handleUserDataEvent(msg);
      return;
    }

    console.log(
      "📨 Mensaje sin handler:",
      JSON.stringify(msg).substring(0, 150),
    );
  });

  // ── ERROR ─────────────────────────────────────────────────────────────────
  ws.on("error", (error) => {
    console.error("❌ Error WebSocket:", error.message);
  });

  // ── CLOSE ─────────────────────────────────────────────────────────────────
  ws.on("close", (code, reason) => {
    const reasonStr = reason?.toString() || "sin motivo";
    console.log(
      `\n🔌 WebSocket cerrado — código: ${code}, razón: ${reasonStr}`,
    );
    clearInterval(pingInterval);
    isAuthenticated = false;
    isSubscribed = false;

    if (code === 1008) {
      console.error("\n❌ Error de autenticación. Posibles causas:");
      console.error(
        "   • BINANCE_API_KEY no corresponde a la llave Ed25519 registrada",
      );
      console.error(
        "   • BINANCE_PRIVATE_KEY tiene formato incorrecto (revisar \\n)",
      );
      console.error(
        "   • La API key fue eliminada o desactivada en Binance Testnet",
      );
      console.error(
        "\n   Corrige las variables de entorno y reinicia el servicio.",
      );
      process.exit(1);
    }

    const delay = Math.min(1000 * Math.pow(1.5, reconnectAttempts), 60000);
    console.log(`🔄 Reconectando en ${Math.round(delay / 1000)}s...`);
    setTimeout(connectWebSocket, delay);
  });

  ws.on("pong", () => {
    console.log("🏓 Pong recibido");
  });
}

// ============================================================
// SUSCRIPCIÓN AL USER DATA STREAM
// ============================================================

function subscribeUserDataStream() {
  if (!ws || ws.readyState !== WebSocket.OPEN) {
    console.error("❌ WebSocket no está abierto para suscribir");
    return;
  }

  console.log("📡 Suscribiendo al User Data Stream...");

  ws.send(
    JSON.stringify({
      id: "subscribe-user-data",
      method: "userDataStream.subscribe",
    }),
  );
}

// ============================================================
// MANEJO DE EVENTOS DE USUARIO
// ============================================================

async function handleUserDataEvent(event) {
  switch (event.e) {
    case "executionReport":
      console.log(`\n📊 executionReport — Orden #${event.i}`);
      console.log(
        `   ${event.s} | ${event.S} | Estado: ${event.X} | Ejecutado: ${event.z}`,
      );
      await trackAndForwardOrder(event);
      break;

    case "balanceUpdate":
      console.log(`\n💰 balanceUpdate — Asset: ${event.a}, Delta: ${event.d}`);
      break;

    case "outboundAccountPosition":
      console.log(`\n📋 outboundAccountPosition — Balances actualizados`);
      break;

    default:
      console.log(`\n📨 Evento no manejado: ${event.e}`);
  }
}

async function trackAndForwardOrder(event) {
  const orderId = event.i?.toString();
  const currentStatus = event.X;
  const previousStatus = lastKnownOrders.get(orderId);

  if (previousStatus === currentStatus) {
    console.log("   (Sin cambio de estado, ignorando)");
    return;
  }

  console.log(`   Estado: ${previousStatus || "NUEVO"} → ${currentStatus}`);
  lastKnownOrders.set(orderId, currentStatus);

  await sendToWebhook(event);

  // Limpiar tracking cuando supera 200 entradas
  if (lastKnownOrders.size > 200) {
    const oldest = [...lastKnownOrders.keys()].slice(0, 100);
    oldest.forEach((k) => lastKnownOrders.delete(k));
    console.log("🧹 Tracking limpiado");
  }
}

// ============================================================
// POLLING FALLBACK — solo si WebSocket falla
// Usa HMAC porque los endpoints REST siguen aceptándolo
// ============================================================

async function checkOrdersViaPolling() {
  if (!BINANCE_API_SECRET) {
    console.warn("⚠️  BINANCE_API_SECRET no definida — polling desactivado");
    return [];
  }

  try {
    const timestamp = Date.now();
    const { queryString, signature } = buildSignedQuery({
      symbol: "BTCUSDT",
      timestamp,
    });

    const response = await fetchWithTimeout(
      `${CURRENT_CONFIG.REST_URL}/api/v3/openOrders?${queryString}&signature=${signature}`,
      { headers: { "X-MBX-APIKEY": BINANCE_API_KEY } },
      10000,
    );

    if (!response.ok) {
      const err = await response.json().catch(() => ({}));
      throw new Error(
        `API Error ${response.status}: ${err.msg || "desconocido"}`,
      );
    }

    const openOrders = await response.json();

    // Órdenes recientes para detectar FILLED
    const ts2 = Date.now();
    const { queryString: qs2, signature: sig2 } = buildSignedQuery({
      symbol: "BTCUSDT",
      limit: 20,
      timestamp: ts2,
    });

    const allOrdersRes = await fetchWithTimeout(
      `${CURRENT_CONFIG.REST_URL}/api/v3/allOrders?${qs2}&signature=${sig2}`,
      { headers: { "X-MBX-APIKEY": BINANCE_API_KEY } },
      10000,
    );

    const allOrders = await allOrdersRes.json();
    await processPollingOrders(allOrders);

    return openOrders;
  } catch (error) {
    console.error("❌ Error en polling:", error.message);
    return [];
  }
}

async function processPollingOrders(orders) {
  for (const order of orders) {
    const orderId = order.orderId.toString();
    const previousStatus = lastKnownOrders.get(orderId);

    if (!previousStatus || previousStatus !== order.status) {
      console.log(
        `\n📊 Polling — Orden ${orderId}: ${previousStatus || "NUEVO"} → ${order.status}`,
      );
      lastKnownOrders.set(orderId, order.status);

      if (order.status === "FILLED" || order.status === "CANCELED") {
        const event = convertRestOrderToWsEvent(order);
        await sendToWebhook(event);
      }
    }
  }
}

function convertRestOrderToWsEvent(order) {
  return {
    e: "executionReport",
    E: Date.now(),
    s: order.symbol,
    c: order.clientOrderId,
    S: order.side,
    o: order.type,
    f: order.timeInForce,
    q: order.origQty,
    p: order.price,
    P: "0.00000000",
    F: "0.00000000",
    g: -1,
    C: "",
    x: order.status === "FILLED" ? "TRADE" : order.status,
    X: order.status,
    r: "NONE",
    i: order.orderId,
    l: order.executedQty,
    z: order.executedQty,
    L: order.price,
    n: "0",
    N: null,
    T: order.updateTime,
    t: -1,
    I: order.orderId,
    w: false,
    m: false,
    M: false,
    O: order.time,
    Z: order.cummulativeQuoteQty,
    Y: order.cummulativeQuoteQty,
    Q: "0.00000000",
    _source: "polling-fallback",
  };
}

function activateFallbackPolling() {
  if (pollingInterval) return;
  console.log("\n⚠️  Activando polling de emergencia (fallback)...");
  checkOrdersViaPolling();
  pollingInterval = setInterval(checkOrdersViaPolling, 15000);
}

function deactivateFallbackPolling() {
  if (!pollingInterval) return;
  clearInterval(pollingInterval);
  pollingInterval = null;
  console.log("✅ Polling de emergencia desactivado — WebSocket activo");
}

// ============================================================
// ENVIAR AL WEBHOOK
// ============================================================

async function sendToWebhook(event) {
  const {
    i: orderId,
    s: symbol,
    X: status,
    z: executedQty,
    S: side,
    p: price,
    L: lastPrice,
  } = event;

  console.log("\n📤 Enviando a webhook:");
  console.log(`   Orden:    ${orderId}`);
  console.log(`   Símbolo:  ${symbol}`);
  console.log(`   Lado:     ${side}`);
  console.log(`   Estado:   ${status}`);
  console.log(`   Ejecutado: ${executedQty}`);
  console.log(`   Precio:   ${lastPrice || price}`);
  console.log(`   Fuente:   ${event._source || "websocket"}`);

  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      const response = await fetchWithTimeout(
        WEBHOOK_URL,
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "User-Agent": "Binance-Listener/7.0",
            "X-Binance-Event": "executionReport",
            "X-Event-Source": event._source || "websocket",
          },
          body: JSON.stringify(event),
        },
        10000,
      );

      if (response.ok) {
        console.log("✅ Webhook enviado exitosamente");
        return true;
      }
      console.error(`❌ Webhook respondió: ${response.status}`);
    } catch (error) {
      console.error(`❌ Intento ${attempt}/3 falló:`, error.message);
      if (attempt < 3) await new Promise((r) => setTimeout(r, 2000 * attempt));
    }
  }

  console.error("❌ Webhook falló después de 3 intentos");
  return false;
}

// ============================================================
// HEALTH SERVER
// ============================================================

function startHealthServer() {
  const PORT = process.env.PORT || 3000;

  const server = http.createServer((req, res) => {
    const wsStatus = ws
      ? ["CONNECTING", "OPEN", "CLOSING", "CLOSED"][ws.readyState]
      : "NOT_CREATED";
    const isHealthy = ws && ws.readyState === WebSocket.OPEN && isSubscribed;

    if (req.url === "/health" || req.url === "/") {
      res.writeHead(isHealthy ? 200 : 503, {
        "Content-Type": "application/json",
      });
      res.end(
        JSON.stringify({
          status: isHealthy ? "healthy" : "unhealthy",
          mode: "websocket-ed25519",
          environment: IS_TESTNET ? "testnet" : "mainnet",
          ws_status: wsStatus,
          authenticated: isAuthenticated,
          subscribed: isSubscribed,
          uptime_seconds: Math.floor(process.uptime()),
          tracked_orders: lastKnownOrders.size,
          fallback_polling_active: !!pollingInterval,
        }),
      );
    } else if (req.url === "/status") {
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(
        JSON.stringify(
          {
            service: "binance-listener",
            version: "7.0",
            auth_method: "Ed25519",
            mode: "websocket-api-authenticated",
            environment: IS_TESTNET ? "testnet" : "mainnet",
            ws_api_url: CURRENT_CONFIG.WS_API_URL,
            ws_status: wsStatus,
            authenticated: isAuthenticated,
            subscribed: isSubscribed,
            reconnect_attempts: reconnectAttempts,
            tracked_orders: lastKnownOrders.size,
            fallback_polling_active: !!pollingInterval,
            memory: process.memoryUsage(),
            timestamp: new Date().toISOString(),
          },
          null,
          2,
        ),
      );
    } else if (req.url === "/test-poll") {
      checkOrdersViaPolling().then((orders) => {
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(
          JSON.stringify({
            orders_checked: orders.length,
            tracked: lastKnownOrders.size,
          }),
        );
      });
    } else if (req.url === "/ws-status") {
      if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(
          JSON.stringify({ id: "session-status", method: "session.status" }),
        );
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(
          JSON.stringify({ message: "Status solicitado — revisa los logs" }),
        );
      } else {
        res.writeHead(503, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: "WebSocket no disponible" }));
      }
    } else {
      res.writeHead(404);
      res.end(JSON.stringify({ error: "Not Found" }));
    }
  });

  server.listen(PORT, () => {
    console.log(`\n🏥 Health server en puerto ${PORT}`);
    console.log(`   Health:    http://localhost:${PORT}/health`);
    console.log(`   Status:    http://localhost:${PORT}/status`);
    console.log(`   WS Status: http://localhost:${PORT}/ws-status`);
    console.log(`   Test Poll: http://localhost:${PORT}/test-poll`);
  });

  return server;
}

// ============================================================
// GRACEFUL SHUTDOWN
// ============================================================

function setupGracefulShutdown(server) {
  async function shutdown(signal) {
    console.log(`\n👋 ${signal} recibido. Cerrando...`);

    clearInterval(pingInterval);
    clearInterval(pollingInterval);

    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(
        JSON.stringify({ id: "session-logout", method: "session.logout" }),
      );
      await new Promise((r) => setTimeout(r, 500));
      ws.close(1000, "Shutdown");
    }

    server.close(() => console.log("✅ HTTP server cerrado"));
    setTimeout(() => {
      console.log("✅ Shutdown completo");
      process.exit(0);
    }, 3000);
  }

  process.on("SIGINT", () => shutdown("SIGINT"));
  process.on("SIGTERM", () => shutdown("SIGTERM"));
}

// ============================================================
// INICIALIZACIÓN
// ============================================================

async function initialize() {
  console.log("\n");
  console.log("═══════════════════════════════════════════════════════");
  console.log("🚀 Binance Listener V7 — Ed25519 WebSocket API");
  console.log("═══════════════════════════════════════════════════════");
  console.log("Ambiente:", IS_TESTNET ? "TESTNET 🧪" : "MAINNET 🚀");
  console.log("Auth:     Ed25519 (WebSocket) + HMAC (REST fallback)");
  console.log("WS URL:  ", CURRENT_CONFIG.WS_API_URL);
  console.log("Webhook:", WEBHOOK_URL || "⚠️  NO CONFIGURADO");
  console.log("═══════════════════════════════════════════════════════\n");

  // Validaciones de entorno
  if (!WEBHOOK_URL) {
    console.error("❌ WEBHOOK_URL no configurada");
    process.exit(1);
  }

  if (!BINANCE_API_KEY) {
    console.error("❌ BINANCE_API_KEY no configurada");
    process.exit(1);
  }

  // Cargar llave privada Ed25519
  try {
    console.log("🔑 Cargando llave privada Ed25519...");
    privateKey = loadPrivateKey();
    console.log("✅ Llave privada Ed25519 cargada correctamente\n");
  } catch (err) {
    console.error("❌", err.message);
    console.error(
      "\n   Verifica que BINANCE_PRIVATE_KEY en Render contiene el PEM completo",
    );
    console.error("   con \\n entre cada línea (formato de una sola línea).");
    process.exit(1);
  }

  // Verificar conectividad REST
  try {
    console.log("🔍 Verificando conectividad con Binance...");
    const pingRes = await fetchWithTimeout(
      `${CURRENT_CONFIG.REST_URL}/api/v3/ping`,
      {},
      5000,
    );
    if (pingRes.ok) {
      console.log("✅ Binance REST API accesible\n");
    } else {
      console.warn(
        `⚠️  Binance REST respondió ${pingRes.status} — continuando de todas formas\n`,
      );
    }
  } catch (err) {
    console.warn(`⚠️  Ping fallido: ${err.message} — puede ser geoblocking\n`);
  }

  const server = startHealthServer();
  setupGracefulShutdown(server);

  // Conectar WebSocket
  connectWebSocket();

  // Activar polling de emergencia si el WebSocket no se suscribe en 30s
  const fallbackTimer = setTimeout(() => {
    if (!isSubscribed) {
      console.warn(
        "\n⚠️  WebSocket no suscrito en 30s — activando polling de emergencia",
      );
      activateFallbackPolling();
    }
  }, 30000);

  // Desactivar polling cuando WebSocket se recupere
  const checkSubscription = setInterval(() => {
    if (isSubscribed && pollingInterval) deactivateFallbackPolling();
  }, 5000);

  process.on("exit", () => {
    clearTimeout(fallbackTimer);
    clearInterval(checkSubscription);
  });

  console.log("✅ Inicialización completada\n");
}

// ============================================================
// ERROR HANDLING GLOBAL
// ============================================================

process.on("uncaughtException", (error) => {
  console.error("❌ UNCAUGHT EXCEPTION:", error.message);
  console.error(error.stack);
});

process.on("unhandledRejection", (reason) => {
  console.error("❌ UNHANDLED REJECTION:", reason);
});

// ============================================================
// START
// ============================================================

initialize().catch((error) => {
  console.error("❌ Error fatal:", error.message);
  process.exit(1);
});
