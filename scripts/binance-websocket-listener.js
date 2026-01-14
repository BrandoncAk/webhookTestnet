// ========================================
// Binance WebSocket Listener
// Optimizado para Railway/Render/Fly.io
// Versión: 2.0
// ========================================

require("dotenv").config();

const WebSocket = require("ws");

// ========================================
// CONFIGURACIÓN DESDE VARIABLES DE ENTORNO
// ========================================

const LISTEN_KEY = process.env.BINANCE_LISTEN_KEY;
const WEBHOOK_URL =
  process.env.WEBHOOK_URL || process.env.VERCEL_URL + "/api/webhooks/binance";
const BINANCE_WS_BASE =
  process.env.BINANCE_WS_URL || "wss://stream.binance.com/ws/";
const VERCEL_URL = process.env.VERCEL_URL;

// ========================================
// VALIDACIÓN DE CONFIGURACIÓN
// ========================================

if (!LISTEN_KEY) {
  console.error("❌ ERROR: BINANCE_LISTEN_KEY no está configurado");
  console.error("Configura la variable de entorno BINANCE_LISTEN_KEY");
  process.exit(1);
}

if (!WEBHOOK_URL) {
  console.error("❌ ERROR: WEBHOOK_URL no está configurado");
  console.error("Configura la variable de entorno WEBHOOK_URL o VERCEL_URL");
  process.exit(1);
}

if (!VERCEL_URL) {
  console.error("⚠️ WARNING: VERCEL_URL no está configurado");
  console.error("La renovación automática del Listen Key podría fallar");
}

console.log("🚀 Iniciando Binance WebSocket Listener");
console.log("Environment:", process.env.NODE_ENV || "development");
console.log("Webhook URL:", WEBHOOK_URL);
console.log("Binance WebSocket Base:", BINANCE_WS_BASE);

// ========================================
// VARIABLES GLOBALES
// ========================================

const WS_URL = `${BINANCE_WS_BASE}${LISTEN_KEY}`;

console.log("Binance WS_URL:", WS_URL);

let ws;
let pingInterval;
let reconnectAttempts = 0;
const MAX_RECONNECT_ATTEMPTS = 10;

// ========================================
// FUNCIÓN: CONECTAR WEBSOCKET
// ========================================

function connect() {
  if (reconnectAttempts >= MAX_RECONNECT_ATTEMPTS) {
    console.error(
      `❌ Máximo de intentos de reconexión alcanzado (${MAX_RECONNECT_ATTEMPTS})`
    );
    console.error("Revisa el Listen Key o la configuración de red");
    process.exit(1);
  }

  console.log(
    `🔌 Conectando a Binance WebSocket... (intento ${
      reconnectAttempts + 1
    }/${MAX_RECONNECT_ATTEMPTS})`
  );

  ws = new WebSocket(WS_URL);

  // ========================================
  // EVENTO: CONEXIÓN EXITOSA
  // ========================================

  ws.on("open", () => {
    console.log("✅ WebSocket conectado exitosamente");
    console.log("⏰ Timestamp:", new Date().toISOString());
    reconnectAttempts = 0; // Reset counter

    // Ping cada 3 minutos para mantener conexión viva
    pingInterval = setInterval(() => {
      if (ws.readyState === WebSocket.OPEN) {
        ws.ping();
        console.log("🏓 Ping enviado");
      }
    }, 3 * 60 * 1000);
  });

  // ========================================
  // EVENTO: MENSAJE RECIBIDO
  // ========================================

  ws.on("message", async (data) => {
    try {
      const event = JSON.parse(data.toString());

      console.log("\n📨 Evento recibido:", event.e);
      console.log("Timestamp:", new Date().toISOString());

      // Solo procesar executionReport
      if (event.e === "executionReport") {
        console.log(`📊 Order ${event.i}: ${event.X} (${event.z} ejecutados)`);
        console.log(
          `   Symbol: ${event.s}, Side: ${event.S}, Type: ${event.o}`
        );

        // Reenviar al webhook con retry
        let retries = 3;
        let success = false;

        while (retries > 0 && !success) {
          try {
            console.log(`📤 Reenviando a webhook: ${WEBHOOK_URL}`);

            const response = await fetch(WEBHOOK_URL, {
              method: "POST",
              headers: {
                "Content-Type": "application/json",
                "User-Agent": "Binance-WebSocket-Listener/2.0",
              },
              body: JSON.stringify(event),
              timeout: 10000, // 10 segundos timeout
            });

            if (!response.ok) {
              throw new Error(
                `HTTP ${response.status}: ${response.statusText}`
              );
            }

            const result = await response.json();

            if (result.success) {
              console.log("✅ Webhook procesado exitosamente");
              console.log(`   Trade ID: ${result.tradeId || "N/A"}`);
              console.log(
                `   Status: ${result.oldStatus} → ${result.newStatus}`
              );
              success = true;
            } else {
              console.log(
                "⚠️ Webhook retornó error:",
                result.error || result.message
              );
              retries--;
              if (retries > 0) {
                console.log(
                  `🔄 Reintentando... (${retries} intentos restantes)`
                );
                await new Promise((resolve) => setTimeout(resolve, 1000));
              }
            }
          } catch (fetchError) {
            console.error("❌ Error enviando a webhook:", fetchError.message);
            retries--;
            if (retries > 0) {
              console.log(`🔄 Reintentando... (${retries} intentos restantes)`);
              await new Promise((resolve) => setTimeout(resolve, 1000));
            } else {
              console.error(
                "❌ Falló después de 3 intentos. Evento perdido:",
                event.i
              );
            }
          }
        }
      } else {
        console.log("⏭️ Evento ignorado (tipo:", event.e, ")");
      }
    } catch (error) {
      console.error("❌ Error procesando mensaje:", error.message);
      console.error("Stack:", error.stack);
    }
  });

  // ========================================
  // EVENTO: ERROR DE WEBSOCKET
  // ========================================

  ws.on("error", (error) => {
    console.error("❌ WebSocket error:", error.message);
    if (error.code) {
      console.error("Error code:", error.code);
    }
  });

  // ========================================
  // EVENTO: CONEXIÓN CERRADA
  // ========================================

  ws.on("close", (code, reason) => {
    console.log("🔌 WebSocket cerrado");
    console.log("Código:", code);
    console.log("Razón:", reason.toString() || "Sin razón específica");
    console.log("Timestamp:", new Date().toISOString());

    clearInterval(pingInterval);

    reconnectAttempts++;
    const delay = Math.min(1000 * Math.pow(2, reconnectAttempts), 30000); // Exponential backoff, max 30s
    console.log(`🔄 Reconectando en ${delay / 1000} segundos...`);
    setTimeout(connect, delay);
  });

  // ========================================
  // EVENTO: PONG (RESPUESTA A PING)
  // ========================================

  ws.on("pong", () => {
    console.log("🏓 Pong recibido - conexión activa");
  });
}

// ========================================
// FUNCIÓN: RENOVAR LISTEN KEY
// ========================================

async function renewListenKey() {
  if (!VERCEL_URL) {
    console.log(
      "⏭️ Renovación de Listen Key omitida (VERCEL_URL no configurado)"
    );
    return;
  }

  try {
    console.log("\n🔄 Renovando Listen Key...");
    console.log("Timestamp:", new Date().toISOString());

    const response = await fetch(`${VERCEL_URL}/api/binance/userDataStream`, {
      method: "PUT",
      headers: {
        "Content-Type": "application/json",
        "User-Agent": "Binance-WebSocket-Listener/2.0",
      },
      body: JSON.stringify({ listenKey: LISTEN_KEY }),
      timeout: 10000,
    });

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    }

    const result = await response.json();

    if (result.success) {
      console.log("✅ Listen Key renovado exitosamente");
    } else {
      console.error(
        "❌ Error renovando Listen Key:",
        result.error || result.message
      );
    }
  } catch (error) {
    console.error("❌ Error en renovación de Listen Key:", error.message);
    console.error("Stack:", error.stack);
  }
}

// ========================================
// HEALTH CHECK SERVER (Para Railway/Render)
// ========================================

const http = require("http");
const PORT = process.env.PORT || 3000;

const server = http.createServer((req, res) => {
  if (req.url === "/health" || req.url === "/") {
    const isConnected = ws && ws.readyState === WebSocket.OPEN;
    const statusCode = isConnected ? 200 : 503;

    res.writeHead(statusCode, { "Content-Type": "application/json" });
    res.end(
      JSON.stringify(
        {
          status: isConnected ? "healthy" : "unhealthy",
          service: "binance-websocket-listener",
          version: "2.0",
          wsStatus: ws ? ws.readyState : "not initialized",
          wsStatusText: ws
            ? ws.readyState === WebSocket.OPEN
              ? "OPEN"
              : ws.readyState === WebSocket.CONNECTING
              ? "CONNECTING"
              : ws.readyState === WebSocket.CLOSING
              ? "CLOSING"
              : "CLOSED"
            : "N/A",
          uptime: process.uptime(),
          uptimeFormatted: formatUptime(process.uptime()),
          reconnectAttempts: reconnectAttempts,
          timestamp: new Date().toISOString(),
        },
        null,
        2
      )
    );
  } else {
    res.writeHead(404, { "Content-Type": "application/json" });
    res.end(
      JSON.stringify({
        error: "Not Found",
        availableEndpoints: ["/health"],
      })
    );
  }
});

server.listen(PORT, () => {
  console.log(`🏥 Health check server listening on port ${PORT}`);
  console.log(`   Health endpoint: http://localhost:${PORT}/health`);
});

// ========================================
// FUNCIÓN: FORMATEAR UPTIME
// ========================================

function formatUptime(seconds) {
  const days = Math.floor(seconds / 86400);
  const hours = Math.floor((seconds % 86400) / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  const secs = Math.floor(seconds % 60);

  const parts = [];
  if (days > 0) parts.push(`${days}d`);
  if (hours > 0) parts.push(`${hours}h`);
  if (minutes > 0) parts.push(`${minutes}m`);
  parts.push(`${secs}s`);

  return parts.join(" ");
}

// ========================================
// INICIAR CONEXIÓN
// ========================================

console.log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
console.log("Iniciando conexión...");
console.log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

connect();

// ========================================
// RENOVAR LISTEN KEY CADA 30 MINUTOS
// ========================================

const RENEWAL_INTERVAL = 30 * 60 * 1000; // 30 minutos
console.log(
  `⏰ Listen Key se renovará automáticamente cada ${
    RENEWAL_INTERVAL / 60000
  } minutos\n`
);

setInterval(renewListenKey, RENEWAL_INTERVAL);

// ========================================
// GRACEFUL SHUTDOWN
// ========================================

function shutdown(signal) {
  console.log(`\n👋 Señal ${signal} recibida. Cerrando gracefully...`);

  // Detener renovación de Listen Key
  clearInterval(pingInterval);

  // Cerrar WebSocket
  if (ws) {
    ws.close(1000, "Normal shutdown");
  }

  // Cerrar servidor HTTP
  server.close(() => {
    console.log("✅ Health check server cerrado");
  });

  // Dar tiempo para cerrar conexiones
  setTimeout(() => {
    console.log("✅ Shutdown completo");
    process.exit(0);
  }, 2000);
}

process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));

// ========================================
// MANEJO DE ERRORES NO CAPTURADOS
// ========================================

process.on("uncaughtException", (error) => {
  console.error("❌ Uncaught Exception:", error.message);
  console.error("Stack:", error.stack);
  console.error("El proceso continuará ejecutándose...");
});

process.on("unhandledRejection", (reason, promise) => {
  console.error("❌ Unhandled Rejection en:", promise);
  console.error("Razón:", reason);
  console.error("El proceso continuará ejecutándose...");
});

// ========================================
// MENSAJE FINAL
// ========================================

console.log("✅ Listener iniciado exitosamente");
console.log("Presiona Ctrl+C para detener\n");
console.log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");
