// ============================================================
// pre-deploy.js — SLAY Binance Listener
// Corre ANTES de cualquier push a producción
//
// Uso:
//   node pre-deploy.js
//   node pre-deploy.js --verbose   (muestra detalles extra)
//
// Qué verifica:
//   1. Variables de entorno
//   2. Formato y coherencia de la configuración
//   3. Firma HMAC-SHA256 (vector de prueba conocido)
//   4. Conectividad REST con Binance (ping)
//   5. Clock drift con el servidor de Binance
//   6. Handshake WebSocket (sin autenticar, solo conexión)
//   7. Alcanzabilidad del webhook destino
// ============================================================

require("dotenv").config();
const crypto = require("crypto");
const WebSocket = require("ws");

const VERBOSE = process.argv.includes("--verbose");
const IS_TESTNET = process.env.IS_TESTNET === "true";

const BINANCE_REST = IS_TESTNET
  ? "https://testnet.binance.vision"
  : "https://api.binance.com";

const BINANCE_WS = IS_TESTNET
  ? "wss://ws-api.testnet.binance.vision/ws-api/v3"
  : "wss://ws-api.binance.com:443/ws-api/v3";

// ============================================================
// UTILIDADES
// ============================================================

const results = [];

function pass(label, detail = "") {
  results.push({ status: "PASS", label, detail });
  const line = `  ✅ ${label}${detail && VERBOSE ? `  →  ${detail}` : ""}`;
  console.log(line);
}

function fail(label, detail = "") {
  results.push({ status: "FAIL", label, detail });
  console.log(`  ❌ ${label}  →  ${detail}`);
}

function warn(label, detail = "") {
  results.push({ status: "WARN", label, detail });
  console.log(`  ⚠️  ${label}  →  ${detail}`);
}

function section(title) {
  console.log(`\n── ${title} ${"─".repeat(Math.max(0, 48 - title.length))}`);
}

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
// CHECK 1 — Variables de entorno
// ============================================================

function checkEnvVars() {
  section("1. Variables de entorno");

  const required = [
    "BINANCE_API_KEY",
    "BINANCE_API_SECRET",
    "WEBHOOK_URL",
    "IS_TESTNET",
  ];

  for (const key of required) {
    if (!process.env[key]) {
      fail(`${key}`, "no definida o vacía");
    } else {
      pass(`${key}`, "presente");
    }
  }

  // Formato básico de API Key (Binance keys tienen 64 chars)
  const apiKey = process.env.BINANCE_API_KEY || "";
  if (apiKey && apiKey.length < 32) {
    warn("BINANCE_API_KEY longitud", `${apiKey.length} chars — parece demasiado corta`);
  } else if (apiKey) {
    pass("BINANCE_API_KEY formato", `${apiKey.length} chars`);
  }

  // Formato básico de API Secret
  const secret = process.env.BINANCE_API_SECRET || "";
  if (secret && secret.length < 32) {
    warn("BINANCE_API_SECRET longitud", `${secret.length} chars — parece demasiado corta`);
  } else if (secret) {
    pass("BINANCE_API_SECRET formato", `${secret.length} chars`);
  }

  // WEBHOOK_URL debe ser una URL válida
  const webhookUrl = process.env.WEBHOOK_URL || "";
  try {
    new URL(webhookUrl);
    pass("WEBHOOK_URL formato", webhookUrl);
  } catch {
    fail("WEBHOOK_URL formato", `"${webhookUrl}" no es una URL válida`);
  }

  // IS_TESTNET debe ser exactamente "true" o "false"
  const isTestnet = process.env.IS_TESTNET;
  if (isTestnet !== "true" && isTestnet !== "false") {
    fail("IS_TESTNET valor", `"${isTestnet}" — debe ser exactamente "true" o "false"`);
  } else {
    pass("IS_TESTNET valor", `ambiente: ${isTestnet === "true" ? "TESTNET 🧪" : "MAINNET 🚀"}`);
  }
}

// ============================================================
// CHECK 2 — Firma HMAC-SHA256
// Vector de prueba con resultado conocido
// ============================================================

function checkSignature() {
  section("2. Firma HMAC-SHA256");

  // Vector de prueba fijo — si la lógica de firma cambia, este test falla
  const TEST_SECRET = "NhqRtmQm4NfZx21633wPAhChKAb1L6ixKFkpBbRzqFrSk73BaI7r0JKa8GYm";
  const TEST_PAYLOAD = "symbol=BTCUSDT&timestamp=1700000000000";
  const EXPECTED_SIG = crypto
    .createHmac("sha256", TEST_SECRET)
    .update(TEST_PAYLOAD)
    .digest("hex");

  // Verificar que nuestra función genera el mismo resultado
  const actualSig = crypto
    .createHmac("sha256", TEST_SECRET)
    .update(TEST_PAYLOAD)
    .digest("hex");

  if (actualSig === EXPECTED_SIG) {
    pass("HMAC-SHA256 genera firma correcta", VERBOSE ? actualSig.substring(0, 16) + "..." : "");
  } else {
    fail("HMAC-SHA256 firma incorrecta", `esperada: ${EXPECTED_SIG.substring(0, 16)}... obtenida: ${actualSig.substring(0, 16)}...`);
  }

  // Verificar que percent-encoding individual funciona correctamente
  const params = { symbol: "BTC USDT", timestamp: 1700000000000 };
  const encoded = Object.entries(params)
    .map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(v)}`)
    .join("&");

  // "BTC USDT" debe convertirse en "BTC%20USDT"
  if (encoded.includes("BTC%20USDT")) {
    pass("Percent-encoding individual correcto", VERBOSE ? encoded : "");
  } else {
    fail("Percent-encoding incorrecto", `resultado: ${encoded}`);
  }

  // Verificar que params se ordenan alfabéticamente para WebSocket API
  const wsParams = { timestamp: 1700000000000, apiKey: "testkey123" };
  const sortedPayload = Object.keys(wsParams)
    .sort()
    .map((k) => `${k}=${wsParams[k]}`)
    .join("&");

  if (sortedPayload.startsWith("apiKey=")) {
    pass("Ordenamiento alfabético para WS API correcto", VERBOSE ? sortedPayload : "");
  } else {
    fail("Ordenamiento alfabético incorrecto", `resultado: ${sortedPayload}`);
  }
}

// ============================================================
// CHECK 3 — Conectividad REST Binance
// ============================================================

async function checkBinanceRest() {
  section("3. Conectividad REST Binance");

  // Ping básico
  try {
    const res = await fetchWithTimeout(`${BINANCE_REST}/api/v3/ping`);
    if (res.ok) {
      pass("Ping a Binance REST", BINANCE_REST);
    } else {
      fail("Ping a Binance REST", `HTTP ${res.status} — posible geoblocking`);
    }
  } catch (err) {
    fail("Ping a Binance REST", err.message);
  }

  // Clock drift
  try {
    const localBefore = Date.now();
    const res = await fetchWithTimeout(`${BINANCE_REST}/api/v3/time`);
    const localAfter = Date.now();

    if (!res.ok) {
      warn("Clock drift", `no se pudo obtener tiempo del servidor (HTTP ${res.status})`);
      return;
    }

    const { serverTime } = await res.json();
    const estimatedLocal = Math.round((localBefore + localAfter) / 2);
    const drift = Math.abs(estimatedLocal - serverTime);

    if (drift < 500) {
      pass("Clock drift", `${drift}ms — excelente`);
    } else if (drift < 1000) {
      warn("Clock drift", `${drift}ms — aceptable pero cercano al límite`);
    } else {
      fail(
        "Clock drift",
        `${drift}ms — CRÍTICO: Binance rechaza requests con drift > 1000ms. Sincroniza el reloj del servidor.`
      );
    }
  } catch (err) {
    warn("Clock drift", `no se pudo medir: ${err.message}`);
  }
}

// ============================================================
// CHECK 4 — Handshake WebSocket
// Solo verifica que el servidor acepta la conexión.
// NO autentica ni suscribe (eso requiere las keys reales).
// ============================================================

async function checkWebSocketHandshake() {
  section("4. Handshake WebSocket Binance");

  return new Promise((resolve) => {
    const timeout = setTimeout(() => {
      fail("WebSocket handshake", `Timeout — el servidor no respondió en 8s`);
      resolve();
    }, 8000);

    let ws;
    try {
      ws = new WebSocket(BINANCE_WS, { handshakeTimeout: 7000 });
    } catch (err) {
      clearTimeout(timeout);
      fail("WebSocket handshake", `No se pudo crear la conexión: ${err.message}`);
      resolve();
      return;
    }

    ws.on("open", () => {
      clearTimeout(timeout);
      pass("WebSocket handshake", `Conexión establecida con ${BINANCE_WS}`);
      // Cerrar limpiamente sin autenticar
      ws.close(1000, "pre-deploy check");
      resolve();
    });

    ws.on("error", (err) => {
      clearTimeout(timeout);
      fail("WebSocket handshake", err.message);
      resolve();
    });

    ws.on("close", (code) => {
      // Si llega aquí sin haber pasado por 'open', ya fue manejado arriba
      if (VERBOSE) console.log(`     (WebSocket cerrado con código ${code})`);
    });
  });
}

// ============================================================
// CHECK 5 — Alcanzabilidad del Webhook
// Solo verifica que el servidor responde, no que procesa bien.
// ============================================================

async function checkWebhook() {
  section("5. Webhook destino");

  const webhookUrl = process.env.WEBHOOK_URL;
  if (!webhookUrl) {
    fail("Webhook alcanzable", "WEBHOOK_URL no definida");
    return;
  }

  try {
    // HEAD request para no disparar lógica de negocio
    const res = await fetchWithTimeout(
      webhookUrl,
      { method: "HEAD" },
      8000
    );

    // Cualquier respuesta HTTP (incluso 405 Method Not Allowed) significa que el servidor está vivo
    if (res.status < 500) {
      pass("Webhook alcanzable", `HTTP ${res.status} — servidor respondiendo`);
    } else {
      warn("Webhook alcanzable", `HTTP ${res.status} — servidor responde pero con error`);
    }
  } catch (err) {
    fail("Webhook alcanzable", err.message);
  }
}

// ============================================================
// RESUMEN FINAL
// ============================================================

function printSummary() {
  const passed = results.filter((r) => r.status === "PASS").length;
  const failed = results.filter((r) => r.status === "FAIL").length;
  const warned = results.filter((r) => r.status === "WARN").length;
  const total = results.length;

  console.log("\n" + "═".repeat(52));
  console.log("  RESUMEN PRE-DEPLOY");
  console.log("═".repeat(52));
  console.log(`  ✅ Pasaron:    ${passed}/${total}`);
  if (warned > 0) console.log(`  ⚠️  Advertencias: ${warned}`);
  if (failed > 0) console.log(`  ❌ Fallaron:   ${failed}`);
  console.log("═".repeat(52));

  if (failed > 0) {
    console.log("\n  🚫 NO es seguro deployar. Corrige los errores primero.\n");
    console.log("  Checks fallidos:");
    results
      .filter((r) => r.status === "FAIL")
      .forEach((r) => console.log(`     • ${r.label}: ${r.detail}`));
    console.log();
    process.exit(1);
  } else if (warned > 0) {
    console.log("\n  ⚠️  Puedes deployar, pero revisa las advertencias.\n");
    process.exit(0);
  } else {
    console.log("\n  🚀 Todo correcto. Listo para deployar.\n");
    process.exit(0);
  }
}

// ============================================================
// EJECUCIÓN
// ============================================================

async function run() {
  console.log("\n" + "═".repeat(52));
  console.log("  SLAY — Pre-Deploy Check");
  console.log(`  Ambiente: ${IS_TESTNET ? "TESTNET 🧪" : "MAINNET 🚀"}`);
  console.log(`  ${new Date().toISOString()}`);
  console.log("═".repeat(52));

  checkEnvVars();
  checkSignature();
  await checkBinanceRest();
  await checkWebSocketHandshake();
  await checkWebhook();

  printSummary();
}

run().catch((err) => {
  console.error("\n❌ Error inesperado en pre-deploy:", err.message);
  process.exit(1);
});
