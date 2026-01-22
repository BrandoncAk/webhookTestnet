// ========================================
// Binance WebSocket Listener V4
// Optimizado para Render.com con FETCH
// ========================================

require("dotenv").config();

const WebSocket = require("ws");
const http = require("http");

// ========================================
// CONFIGURACIÓN
// ========================================

const WEBHOOK_URL = process.env.WEBHOOK_URL;
const IS_TESTNET = process.env.IS_TESTNET;

// URLs según ambiente
const BINANCE_REST_URL = IS_TESTNET
  ? "https://testnet.binance.vision"
  : "https://api.binance.com";
const BINANCE_WS_BASE = IS_TESTNET
  ? "wss://testnet.binance.vision/ws"
  : "wss://stream.binance.com:9443/ws";

// ========================================
// CLASE ABORTCONTROLLER PARA TIMEOUTS
// ========================================

class TimeoutController {
  constructor(timeoutMs) {
    this.controller = new AbortController();
    this.timeoutId = setTimeout(() => this.controller.abort(), timeoutMs);
  }

  get signal() {
    return this.controller.signal;
  }

  clear() {
    clearTimeout(this.timeoutId);
  }
}

// ========================================
// FUNCIÓN FETCH CON TIMEOUT
// ========================================

async function fetchWithTimeout(url, options = {}, timeoutMs = 10000) {
  const timeoutController = new TimeoutController(timeoutMs);

  try {
    const response = await fetch(url, {
      ...options,
      signal: timeoutController.signal,
    });

    timeoutController.clear();
    return response;
  } catch (error) {
    timeoutController.clear();

    if (error.name === "AbortError") {
      throw new Error(`Timeout después de ${timeoutMs}ms`);
    }
    throw error;
  }
}

// ========================================
// VARIABLES GLOBALES
// ========================================

let listenKey = null;
let ws = null;
let pingInterval = null;
let renewInterval = null;
let reconnectAttempts = 0;
const MAX_RECONNECT_ATTEMPTS = 15; // Más intentos para Render

// ========================================
// 1. OBTENER LISTENKEY CON FETCH
// ========================================

async function getListenKey() {
  try {
    console.log("🔄 Obteniendo ListenKey de Binance...");

    // const response = await fetchWithTimeout(
    //   "https://slay-seven.vercel.app/api/webhooks/binance/userDataStream",
    //   {
    //     method: "POST",
    //     headers: {
    //       "Content-Type": "application/json",
    //     },
    //   },
    //   10000,
    // );

    const response = await fetch(
      "https://slay-seven.vercel.app/api/webhooks/binance/userDataStream",
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
      },
    );

    const data = await response.json();
    console.log("RESPONSE ALV", data);

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    }

    if (!data.listenKey) {
      throw new Error("No se recibió listenKey en la respuesta");
    }

    console.log(`✅ ListenKey obtenida: ${data.listenKey.substring(0, 20)}...`);
    return data.listenKey;
  } catch (error) {
    console.error("❌ Error obteniendo ListenKey:", error.message);
    throw error;
  }
}

// ========================================
// 2. CONECTAR WEBSOCKET
// ========================================

function connectWebSocket() {
  if (reconnectAttempts >= MAX_RECONNECT_ATTEMPTS) {
    console.error(
      `❌ Máximo de intentos de reconexión alcanzado (${MAX_RECONNECT_ATTEMPTS})`,
    );
    console.error(
      "   Render.com está funcionando, pero Binance rechaza la conexión",
    );
    console.error("   Posibles causas:");
    console.error("   1. API Key inválida o expirada");
    console.error("   2. ListenKey expirada");
    console.error("   3. Problemas temporales de Binance");

    // En Render, mejor reiniciar el servicio después de un tiempo
    setTimeout(() => {
      console.log("🔄 Reiniciando servicio en 30 segundos...");
      process.exit(1); // Render reiniciará automáticamente
    }, 30000);

    return;
  }

  if (!listenKey) {
    console.error("❌ No hay ListenKey para conectar");
    return;
  }

  // URL CORREGIDA para Render (añadir slash)
  const WS_URL = `${BINANCE_WS_BASE}/${listenKey}`;

  reconnectAttempts++;
  console.log(`🔌 Conectando WebSocket (Intento ${reconnectAttempts})...`);
  console.log("   URL:", WS_URL);

  // Configuración especial para Render
  ws = new WebSocket(WS_URL, {
    handshakeTimeout: 10000,
    perMessageDeflate: false,
  });

  // ========== EVENTOS WEBSOCKET ==========

  ws.on("open", () => {
    console.log("✅ WebSocket conectado exitosamente desde Render.com");
    console.log("⏰", new Date().toISOString());
    reconnectAttempts = 0;

    // Ping cada 2 minutos (más frecuente para mantener conexión)
    pingInterval = setInterval(
      () => {
        if (ws.readyState === WebSocket.OPEN) {
          ws.ping();
          console.log("🏓 Ping enviado para mantener conexión viva");
        }
      },
      2 * 60 * 1000,
    );
  });

  ws.on("message", async (data) => {
    try {
      const event = JSON.parse(data.toString());

      if (event.e === "executionReport") {
        await handleExecutionReport(event);
      } else if (
        event.e === "outboundAccountPosition" ||
        event.e === "balanceUpdate"
      ) {
        console.log(`💰 Evento de cuenta: ${event.e}`);
      } else {
        console.log(`📨 Evento ${event.e || "desconocido"} recibido`);
      }
    } catch (error) {
      console.error("❌ Error procesando mensaje:", error.message);
    }
  });

  ws.on("error", (error) => {
    console.error("❌ Error WebSocket:", error.message);
    if (error.code) {
      console.error("   Código:", error.code);
    }
  });

  ws.on("close", (code, reason) => {
    console.log(`🔌 WebSocket cerrado: ${code} - ${reason || "Sin razón"}`);
    console.log("⏰", new Date().toISOString());

    clearInterval(pingInterval);

    // Backoff exponencial con límite
    const delay = Math.min(1000 * Math.pow(1.5, reconnectAttempts), 45000); // Max 45s
    console.log(`🔄 Reconectando en ${Math.round(delay / 1000)}s...`);

    setTimeout(async () => {
      try {
        // Para reconexiones después del primer fallo, obtener nueva listenKey
        if (reconnectAttempts > 1) {
          console.log("🔄 Obteniendo nueva ListenKey para reconexión...");
          listenKey = await getListenKey();
        }
        connectWebSocket();
      } catch (error) {
        console.error("❌ Error preparando reconexión:", error.message);
      }
    }, delay);
  });

  ws.on("pong", () => {
    console.log("🏓 Pong recibido - Conexión saludable");
  });
}

// ========================================
// 3. MANEJAR EVENTOS Y ENVIAR A WEBHOOK
// ========================================

async function handleExecutionReport(event) {
  const {
    i: orderId,
    s: symbol,
    X: status,
    x: eventType,
    z: executedQty,
  } = event;

  console.log("\n📊 Evento executionReport");
  console.log("   Orden:", orderId);
  console.log("   Símbolo:", symbol);
  console.log("   Estado:", status);
  console.log("   Evento:", eventType);
  console.log("   Ejecutado:", executedQty);
  console.log("   ⏰", new Date().toISOString());

  // Enviar a webhook
  await sendToWebhookWithRetry(event, 3);
}

async function sendToWebhookWithRetry(event, maxRetries) {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      console.log(`📤 Enviando a webhook (${attempt}/${maxRetries})...`);

      const response = await fetchWithTimeout(
        WEBHOOK_URL,
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "User-Agent": "Binance-Listener/4.0",
            "X-Binance-Event": "executionReport",
          },
          body: JSON.stringify(event),
        },
        10000,
      );

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }

      const responseText = await response.text();
      console.log("✅ Webhook respondió:", response.status);

      if (responseText) {
        try {
          const data = JSON.parse(responseText);
          console.log(
            "   Respuesta:",
            data.success ? "Éxito" : "Error",
            data.message || "",
          );
        } catch {
          console.log("   Respuesta:", responseText.substring(0, 100));
        }
      }

      return; // Éxito
    } catch (error) {
      console.error(`   ❌ Intento ${attempt} falló:`, error.message);

      if (attempt < maxRetries) {
        const delay = 1500 * attempt; // 1.5s, 3s, 4.5s...
        console.log(`   ⏳ Esperando ${delay / 1000}s...`);
        await sleep(delay);
      } else {
        console.error(`❌ Falló después de ${maxRetries} intentos`);
        // En Render, podemos loguear para debugging pero continuar
      }
    }
  }
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ========================================
// 4. RENOVAR LISTENKEY CADA 30 MIN
// ========================================

async function renewListenKey() {
  if (!listenKey) return;

  try {
    console.log("🔄 Renovando ListenKey...");

    const response = await fetchWithTimeout(
      "http://slay-seven.vercel.app/api/webhooks/binance/userDataStream",
      {
        method: "PUT",
        headers: {
          "Content-Type": "application/json",
          "User-Agent": "Binance-Listener/Render",
        },
        body: JSON.stringify({ listenKey }),
      },
      10000,
    );

    if (!response.success) {
      if (response.status === 404) {
        throw new Error("ListenKey no encontrada (expirada)");
      }
      throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    }

    console.log("✅ ListenKey renovada");
    console.log("   ⏰", new Date().toISOString());
  } catch (error) {
    console.error("❌ Error renovando ListenKey:", error.message);

    // Si expiró, obtener nueva
    if (
      error.message.includes("expirada") ||
      error.message.includes("no encontrada")
    ) {
      console.log("   🔄 ListenKey expirada, obteniendo nueva...");
      try {
        listenKey = await getListenKey();

        // Reconectar con nueva key
        if (ws) {
          ws.close();
          setTimeout(() => connectWebSocket(), 1000);
        }
      } catch (getError) {
        console.error(
          "   ❌ Error obteniendo nueva ListenKey:",
          getError.message,
        );
      }
    }
  }
}

function startListenKeyRenewal() {
  // Renovar cada 25 minutos (menos de 30 para ser seguros)
  renewInterval = setInterval(renewListenKey, 25 * 60 * 1000);
  console.log("⏰ Renovación automática cada 25 minutos\n");
}

// ========================================
// 5. HEALTH CHECK PARA RENDER
// ========================================

function startHealthServer() {
  const PORT = process.env.PORT || 3000;

  const server = http.createServer((req, res) => {
    // Render requiere que respondamos rápido (<10s)
    if (req.url === "/health" || req.url === "/") {
      const isConnected = ws && ws.readyState === WebSocket.OPEN;
      const status = isConnected ? "healthy" : "unhealthy";
      const statusCode = isConnected ? 200 : 503;

      // Respuesta MINIMALISTA para Render
      const healthData = {
        status: status,
        service: "binance-websocket-listener",
        version: "4.0",
        ws_connected: isConnected,
        listen_key_active: !!listenKey,
        uptime_seconds: Math.floor(process.uptime()),
        reconnect_attempts: reconnectAttempts,
      };

      res.writeHead(statusCode, {
        "Content-Type": "application/json",
        "Cache-Control": "no-cache",
      });
      res.end(JSON.stringify(healthData));
    } else if (req.url === "/status") {
      // Endpoint más detallado para debugging
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(
        JSON.stringify(
          {
            service: "binance-listener",
            environment: IS_TESTNET ? "testnet" : "production",
            node_version: process.version,
            memory_usage: process.memoryUsage(),
            ws_status: getWsStatus(ws),
            listen_key_preview: listenKey
              ? `${listenKey.substring(0, 10)}...`
              : null,
            webhook_url: WEBHOOK_URL ? "configured" : "missing",
            timestamp: new Date().toISOString(),
          },
          null,
          2,
        ),
      );
    } else {
      res.writeHead(404, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: "Not Found" }));
    }
  });

  server.listen(PORT, () => {
    console.log(`🏥 Health server en puerto ${PORT}`);
    console.log(`   Health check: http://localhost:${PORT}/health`);
    console.log(`   Status detallado: http://localhost:${PORT}/status`);
    console.log("");
  });

  return server;
}

function getWsStatus(ws) {
  if (!ws) return "not_initialized";
  const states = ["CONNECTING", "OPEN", "CLOSING", "CLOSED"];
  return states[ws.readyState] || "UNKNOWN";
}

// ========================================
// 6. GRACEFUL SHUTDOWN PARA RENDER
// ========================================

function setupGracefulShutdown(server) {
  async function shutdown(signal) {
    console.log(`\n👋 ${signal} recibido. Cerrando servicio...`);

    // 1. Detener intervalos
    clearInterval(pingInterval);
    clearInterval(renewInterval);

    // 2. Cerrar WebSocket
    if (ws) {
      ws.close(1000, "Service shutdown");
    }

    // 3. Intentar eliminar ListenKey de Binance (opcional)
    if (listenKey && BINANCE_API_KEY) {
      setTimeout(async () => {
        try {
          await fetchWithTimeout(
            "http://slay-seven.vercel.app/api/webhooks/binance/userDataStream",
            {
              method: "DELETE",
              headers: {
                "Content-Type": "application/json",
                "User-Agent": "Binance-Listener/Render",
              },
              body: JSON.stringify({ listenKey }),
            },
            5000,
          );
          console.log("🗑️ ListenKey eliminada de Binance");
        } catch (error) {
          // Ignorar en shutdown
        }
      }, 100);
    }

    // 4. Cerrar servidor HTTP
    server.close(() => {
      console.log("✅ Servidor HTTP cerrado");
    });

    // 5. Dar tiempo y salir
    setTimeout(() => {
      console.log("✅ Shutdown completo");
      process.exit(0);
    }, 2500);
  }

  process.on("SIGINT", () => shutdown("SIGINT"));
  process.on("SIGTERM", () => shutdown("SIGTERM"));
}

// ========================================
// 7. MANEJO DE ERRORES
// ========================================

function setupGlobalErrorHandling() {
  process.on("uncaughtException", (error) => {
    console.error("❌ UNCAUGHT EXCEPTION:", error.message);
    console.error("Stack:", error.stack);
    // En Render, mejor continuar a menos que sea crítico
  });

  process.on("unhandledRejection", (reason, promise) => {
    console.error("❌ UNHANDLED REJECTION:", reason);
    // Log pero continuar
  });
}

// ========================================
// 8. INICIALIZACIÓN PRINCIPAL
// ========================================

async function initialize() {
  console.log("🚀 Binance WebSocket Listener V4");
  console.log("Deploy: Render.com");
  console.log("Modo:", IS_TESTNET ? "TESTNET 🧪" : "PRODUCTION 🚀");
  console.log("Webhook:", WEBHOOK_URL || "NO CONFIGURADO");
  console.log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

  if (!WEBHOOK_URL) {
    console.error("❌ ERROR: WEBHOOK_URL no configurado");
    console.error("   Necesitas una URL para enviar los eventos");
    process.exit(1);
  }

  try {
    // 1. Obtener ListenKey inicial
    listenKey = await getListenKey();

    // 2. Iniciar servidor health check (Render lo requiere rápido)
    const server = startHealthServer();

    // 3. Configurar shutdown graceful
    setupGracefulShutdown(server);

    // 4. Conectar WebSocket
    setTimeout(() => connectWebSocket(), 500);

    // 5. Iniciar renovación de ListenKey
    setTimeout(() => startListenKeyRenewal(), 60000); // Esperar 1 minuto

    console.log("✅ Servicio inicializado para Render.com");
    console.log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");
  } catch (error) {
    console.error("❌ Error inicializando:", error.message);

    // En Render, si falla al inicio, salir para que reinicie
    setTimeout(() => {
      console.log("🔄 Reiniciando en 10 segundos...");
      process.exit(1);
    }, 10000);
  }
}

// ========================================
// INICIAR APLICACIÓN
// ========================================

// Configurar manejo de errores primero
setupGlobalErrorHandling();

// Inicializar
initialize().catch((error) => {
  console.error("❌ Error fatal:", error);
  process.exit(1);
});
