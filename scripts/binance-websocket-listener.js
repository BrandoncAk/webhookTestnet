// ========================================
// Binance WebSocket Listener V5
// Con soporte para Polling en Testnet
// ========================================

require("dotenv").config();

const WebSocket = require("ws");
const http = require("http");
const crypto = require("crypto");

// ========================================
// CONFIGURACIÓN
// ========================================

const WEBHOOK_URL = process.env.WEBHOOK_URL;
const IS_TESTNET = process.env.IS_TESTNET === "true";
const BINANCE_API_KEY = process.env.BINANCE_API_KEY;
const BINANCE_API_SECRET = process.env.BINANCE_API_SECRET;

// URLs según ambiente
const CONFIG = {
  testnet: {
    REST_URL: "https://testnet.binance.vision",
    WS_URL: null, // No soportado en Testnet Spot
    USE_POLLING: true,
    POLLING_INTERVAL: 15000, // 15 segundos
  },
  mainnet: {
    REST_URL: "https://api.binance.com",
    WS_URL: "wss://stream.binance.com:9443/ws",
    USE_POLLING: false,
    POLLING_INTERVAL: null,
  },
};

const CURRENT_CONFIG = IS_TESTNET ? CONFIG.testnet : CONFIG.mainnet;

// ========================================
// VARIABLES GLOBALES
// ========================================

let listenKey = null;
let ws = null;
let pingInterval = null;
let renewInterval = null;
let pollingInterval = null;
let reconnectAttempts = 0;
let lastKnownOrders = new Map(); // Para tracking de cambios
const MAX_RECONNECT_ATTEMPTS = 10;

// ========================================
// UTILIDADES
// ========================================

function generateSignature(queryString) {
  return crypto
    .createHmac("sha256", BINANCE_API_SECRET)
    .update(queryString)
    .digest("hex");
}

async function fetchWithTimeout(url, options = {}, timeoutMs = 10000) {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const response = await fetch(url, {
      ...options,
      signal: controller.signal,
    });
    clearTimeout(timeoutId);
    return response;
  } catch (error) {
    clearTimeout(timeoutId);
    if (error.name === "AbortError") {
      throw new Error(`Timeout después de ${timeoutMs}ms`);
    }
    throw error;
  }
}

// ========================================
// 1. OBTENER LISTENKEY
// ========================================

async function getListenKey() {
  try {
    console.log("🔄 Obteniendo ListenKey de Binance...");

    const response = await fetch(
      "https://slay-seven.vercel.app/api/webhooks/binance/userDataStream",
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
      },
    );

    const data = await response.json();
    console.log("📡 Respuesta ListenKey:", {
      success: data.success,
      hasKey: !!data.listenKey,
    });

    if (!response.ok || !data.listenKey) {
      throw new Error(data.error || "No se recibió listenKey");
    }

    console.log(`✅ ListenKey obtenida: ${data.listenKey.substring(0, 20)}...`);
    return data.listenKey;
  } catch (error) {
    console.error("❌ Error obteniendo ListenKey:", error.message);
    throw error;
  }
}

// ========================================
// 2. POLLING PARA TESTNET
// ========================================

async function checkOrdersViaAPI() {
  console.log("🔑 API Key presente:", !!BINANCE_API_KEY);
  console.log(
    "🔑 API Key (primeros 10 chars):",
    BINANCE_API_KEY?.substring(0, 10),
  );
  console.log("🔑 API Secret presente:", !!BINANCE_API_SECRET);

  try {
    const timestamp = Date.now();
    const queryString = `timestamp=${timestamp}`;
    const signature = generateSignature(queryString);

    // Obtener todas las órdenes abiertas
    const response = await fetchWithTimeout(
      `${CURRENT_CONFIG.REST_URL}/api/v3/openOrders?symbol=BTCUSDT&${queryString}&signature=${signature}`,
      {
        headers: { "X-MBX-APIKEY": BINANCE_API_KEY },
      },
      10000,
    );

    if (!response.ok) {
      const errorData = await response.json();
      throw new Error(`API Error: ${errorData.msg || response.status}`);
    }

    const openOrders = await response.json();
    console.log("📦 Respuesta de Binance openOrders:", openOrders);

    // También verificar órdenes recientes (últimas 24h) para detectar FILLED
    const allOrdersResponse = await fetchWithTimeout(
      `${CURRENT_CONFIG.REST_URL}/api/v3/allOrders?symbol=BTCUSDT&limit=20&timestamp=${Date.now()}&signature=${generateSignature(`symbol=BTCUSDT&limit=20&timestamp=${Date.now()}`)}`,
      {
        headers: { "X-MBX-APIKEY": BINANCE_API_KEY },
      },
      10000,
    );

    const allOrders = await allOrdersResponse.json();

    console.log("📦 Respuesta de Binance allOrders:", allOrders);

    // Procesar cambios
    await processOrderChanges(allOrders);

    return openOrders;
  } catch (error) {
    console.error("❌ Error en polling:", error.message);
    return [];
  }
}

async function processOrderChanges(orders) {
  for (const order of orders) {
    const orderId = order.orderId.toString();
    const previousStatus = lastKnownOrders.get(orderId);

    // Si es una orden nueva o cambió de estado
    if (!previousStatus || previousStatus !== order.status) {
      console.log(
        `\n📊 Orden ${orderId}: ${previousStatus || "NEW"} → ${order.status}`,
      );

      // Si pasó a FILLED o CANCELED, enviar al webhook
      if (order.status === "FILLED" || order.status === "CANCELED") {
        const event = convertOrderToEvent(order);
        await sendToWebhook(event);
      }

      // Actualizar tracking
      lastKnownOrders.set(orderId, order.status);
    }
  }

  // Limpiar órdenes antiguas del tracking (más de 100 órdenes)
  if (lastKnownOrders.size > 100) {
    const entries = [...lastKnownOrders.entries()];
    entries.slice(0, entries.length - 50).forEach(([key]) => {
      lastKnownOrders.delete(key);
    });
  }
}

function convertOrderToEvent(order) {
  // Convertir formato de API REST a formato de WebSocket event
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
    // Campo personalizado para identificar que viene de polling
    _source: "polling",
  };
}

function startPolling() {
  console.log(
    `\n🔄 Iniciando polling cada ${CURRENT_CONFIG.POLLING_INTERVAL / 1000} segundos...`,
  );
  console.log("   (Testnet no soporta WebSocket User Data Stream)");

  // Polling inicial
  checkOrdersViaAPI();

  // Polling periódico
  pollingInterval = setInterval(async () => {
    console.log("🔍 Verificando órdenes...");
    await checkOrdersViaAPI();
  }, CURRENT_CONFIG.POLLING_INTERVAL);
}

// ========================================
// 3. WEBSOCKET PARA MAINNET
// ========================================

function connectWebSocket() {
  if (!CURRENT_CONFIG.WS_URL) {
    console.log("⚠️ WebSocket no disponible en este ambiente");
    return;
  }

  if (reconnectAttempts >= MAX_RECONNECT_ATTEMPTS) {
    console.error(
      `❌ Máximo de intentos alcanzado (${MAX_RECONNECT_ATTEMPTS})`,
    );
    setTimeout(() => process.exit(1), 30000);
    return;
  }

  if (!listenKey) {
    console.error("❌ No hay ListenKey");
    return;
  }

  const WS_URL = `${CURRENT_CONFIG.WS_URL}/${listenKey}`;

  reconnectAttempts++;
  console.log(`🔌 Conectando WebSocket (Intento ${reconnectAttempts})...`);
  console.log("   URL:", WS_URL);

  ws = new WebSocket(WS_URL, {
    handshakeTimeout: 10000,
    perMessageDeflate: false,
  });

  ws.on("open", () => {
    console.log("✅ WebSocket conectado");
    console.log("⏰", new Date().toISOString());
    reconnectAttempts = 0;

    pingInterval = setInterval(
      () => {
        if (ws.readyState === WebSocket.OPEN) {
          ws.ping();
          console.log("🏓 Ping enviado");
        }
      },
      2 * 60 * 1000,
    );
  });

  ws.on("message", async (data) => {
    try {
      const event = JSON.parse(data.toString());

      if (event.e === "executionReport") {
        console.log("\n📊 Evento executionReport recibido via WebSocket");
        await sendToWebhook(event);
      } else {
        console.log(`📨 Evento: ${event.e || "desconocido"}`);
      }
    } catch (error) {
      console.error("❌ Error procesando mensaje:", error.message);
    }
  });

  ws.on("error", (error) => {
    console.error("❌ Error WebSocket:", error.message);
  });

  ws.on("close", (code, reason) => {
    console.log(`🔌 WebSocket cerrado: ${code}`);
    clearInterval(pingInterval);

    const delay = Math.min(1000 * Math.pow(1.5, reconnectAttempts), 45000);
    console.log(`🔄 Reconectando en ${Math.round(delay / 1000)}s...`);

    setTimeout(async () => {
      if (reconnectAttempts > 1) {
        listenKey = await getListenKey();
      }
      connectWebSocket();
    }, delay);
  });

  ws.on("pong", () => {
    console.log("🏓 Pong recibido");
  });
}

// ========================================
// 4. ENVIAR A WEBHOOK
// ========================================

async function sendToWebhook(event) {
  const {
    i: orderId,
    s: symbol,
    X: status,
    x: eventType,
    z: executedQty,
    S: side,
    p: price,
    L: lastPrice,
  } = event;

  console.log("\n📤 Enviando a webhook:");
  console.log("   Orden:", orderId);
  console.log("   Símbolo:", symbol);
  console.log("   Lado:", side);
  console.log("   Estado:", status);
  console.log("   Ejecutado:", executedQty);
  console.log("   Precio:", lastPrice || price);
  console.log("   Fuente:", event._source || "websocket");

  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      const response = await fetchWithTimeout(
        WEBHOOK_URL,
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "User-Agent": "Binance-Listener/5.0",
            "X-Binance-Event": "executionReport",
            "X-Event-Source": event._source || "websocket",
          },
          body: JSON.stringify(event),
        },
        10000,
      );

      if (response.ok) {
        console.log(`✅ Webhook enviado exitosamente`);
        return true;
      } else {
        console.error(`❌ Webhook respondió: ${response.status}`);
      }
    } catch (error) {
      console.error(`❌ Intento ${attempt} falló:`, error.message);
      if (attempt < 3) {
        await new Promise((r) => setTimeout(r, 2000 * attempt));
      }
    }
  }

  console.error("❌ Falló después de 3 intentos");
  return false;
}

// ========================================
// 5. RENOVAR LISTENKEY
// ========================================

async function renewListenKey() {
  if (!listenKey) return;

  try {
    console.log("🔄 Renovando ListenKey...");

    const response = await fetchWithTimeout(
      "https://slay-seven.vercel.app/api/webhooks/binance/userDataStream",
      {
        method: "PUT",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ listenKey }),
      },
      10000,
    );

    const data = await response.json();

    if (data.success) {
      console.log("✅ ListenKey renovada");
    } else {
      throw new Error(data.error || "Error renovando");
    }
  } catch (error) {
    console.error("❌ Error renovando:", error.message);

    // Si expiró, obtener nueva
    try {
      listenKey = await getListenKey();
      if (ws && !CURRENT_CONFIG.USE_POLLING) {
        ws.close();
        setTimeout(() => connectWebSocket(), 1000);
      }
    } catch (e) {
      console.error("❌ Error obteniendo nueva key:", e.message);
    }
  }
}

// ========================================
// 6. HEALTH SERVER
// ========================================

function startHealthServer() {
  const PORT = process.env.PORT || 3000;

  const server = http.createServer((req, res) => {
    if (req.url === "/health" || req.url === "/") {
      const isHealthy = CURRENT_CONFIG.USE_POLLING
        ? !!pollingInterval
        : ws && ws.readyState === WebSocket.OPEN;

      res.writeHead(isHealthy ? 200 : 503, {
        "Content-Type": "application/json",
      });
      res.end(
        JSON.stringify({
          status: isHealthy ? "healthy" : "unhealthy",
          mode: CURRENT_CONFIG.USE_POLLING ? "polling" : "websocket",
          environment: IS_TESTNET ? "testnet" : "mainnet",
          uptime_seconds: Math.floor(process.uptime()),
          listen_key_active: !!listenKey,
          tracked_orders: lastKnownOrders.size,
        }),
      );
    } else if (req.url === "/status") {
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(
        JSON.stringify(
          {
            service: "binance-listener",
            version: "5.0",
            mode: CURRENT_CONFIG.USE_POLLING ? "polling" : "websocket",
            environment: IS_TESTNET ? "testnet" : "mainnet",
            polling_interval: CURRENT_CONFIG.POLLING_INTERVAL,
            ws_status: ws
              ? ["CONNECTING", "OPEN", "CLOSING", "CLOSED"][ws.readyState]
              : "N/A",
            tracked_orders: lastKnownOrders.size,
            memory: process.memoryUsage(),
            timestamp: new Date().toISOString(),
          },
          null,
          2,
        ),
      );
    } else if (req.url === "/test-poll") {
      // Endpoint para forzar un polling manual
      checkOrdersViaAPI().then((orders) => {
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(
          JSON.stringify({
            orders_checked: orders.length,
            tracked: lastKnownOrders.size,
          }),
        );
      });
    } else {
      res.writeHead(404);
      res.end(JSON.stringify({ error: "Not Found" }));
    }
  });

  server.listen(PORT, () => {
    console.log(`\n🏥 Health server en puerto ${PORT}`);
    console.log(`   Health: http://localhost:${PORT}/health`);
    console.log(`   Status: http://localhost:${PORT}/status`);
    console.log(`   Test Poll: http://localhost:${PORT}/test-poll`);
  });

  return server;
}

// ========================================
// 7. GRACEFUL SHUTDOWN
// ========================================

function setupGracefulShutdown(server) {
  async function shutdown(signal) {
    console.log(`\n👋 ${signal} recibido. Cerrando...`);

    clearInterval(pingInterval);
    clearInterval(renewInterval);
    clearInterval(pollingInterval);

    if (ws) ws.close(1000, "Shutdown");

    server.close(() => console.log("✅ HTTP server cerrado"));

    setTimeout(() => {
      console.log("✅ Shutdown completo");
      process.exit(0);
    }, 2000);
  }

  process.on("SIGINT", () => shutdown("SIGINT"));
  process.on("SIGTERM", () => shutdown("SIGTERM"));
}

// ========================================
// 8. INICIALIZACIÓN
// ========================================

async function initialize() {
  console.log("\n");
  console.log("═══════════════════════════════════════════════════");
  console.log("🚀 Binance Listener V5");
  console.log("═══════════════════════════════════════════════════");
  console.log("Ambiente:", IS_TESTNET ? "TESTNET 🧪" : "MAINNET 🚀");
  console.log(
    "Modo:",
    CURRENT_CONFIG.USE_POLLING ? "POLLING 🔄" : "WEBSOCKET 🔌",
  );
  console.log("Webhook:", WEBHOOK_URL || "NO CONFIGURADO");
  console.log("═══════════════════════════════════════════════════\n");

  if (!WEBHOOK_URL) {
    console.error("❌ WEBHOOK_URL no configurado");
    process.exit(1);
  }

  if (!BINANCE_API_KEY || !BINANCE_API_SECRET) {
    console.error("❌ API Keys no configuradas");
    process.exit(1);
  }

  try {
    // 1. Obtener ListenKey (útil para renovación aunque usemos polling)
    listenKey = await getListenKey();

    // 2. Iniciar health server
    const server = startHealthServer();
    setupGracefulShutdown(server);

    // 3. Iniciar según modo
    if (CURRENT_CONFIG.USE_POLLING) {
      // TESTNET: Usar polling
      console.log("\n⚠️ Testnet Spot no soporta WebSocket User Data Stream");
      console.log("   Usando polling como alternativa\n");
      startPolling();
    } else {
      // MAINNET: Usar WebSocket
      setTimeout(() => connectWebSocket(), 500);
    }

    // 4. Renovación de ListenKey (cada 25 min)
    renewInterval = setInterval(renewListenKey, 25 * 60 * 1000);

    console.log("✅ Servicio inicializado\n");
  } catch (error) {
    console.error("❌ Error inicializando:", error.message);
    setTimeout(() => process.exit(1), 10000);
  }
}

// ========================================
// ERROR HANDLING
// ========================================

process.on("uncaughtException", (error) => {
  console.error("❌ UNCAUGHT:", error.message);
});

process.on("unhandledRejection", (reason) => {
  console.error("❌ UNHANDLED:", reason);
});

// ========================================
// START
// ========================================

initialize().catch((error) => {
  console.error("❌ Fatal:", error);
  process.exit(1);
});
