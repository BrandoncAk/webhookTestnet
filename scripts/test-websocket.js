// test-testnet.js
const WebSocket = require("ws");

// Tu API Key de TESTNET (diferente a la de producción)
const TESTNET_API_KEY =
  "SkVds6nzskEXpk9v9m2H6Eawt1Xr1nd5ccd4TLMXsFWbv7NFTsOgGX3EcwVk3gkV";

async function getTestnetListenKey() {
  try {
    console.log("🔄 Obteniendo ListenKey de TESTNET...");

    const response = await fetch(
      "https://testnet.binance.vision/api/v3/userDataStream",
      {
        method: "POST",
        headers: {
          "X-MBX-APIKEY": TESTNET_API_KEY,
          "Content-Type": "application/json",
        },
      },
    );

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${await response.text()}`);
    }

    const data = await response.json();

    if (!data.listenKey) {
      throw new Error("No se recibió listenKey");
    }

    console.log(
      "✅ ListenKey de TESTNET obtenida:",
      data.listenKey.substring(0, 20) + "...",
    );
    return data.listenKey;
  } catch (error) {
    console.error("❌ Error obteniendo ListenKey de TESTNET:", error.message);
    return null;
  }
}

async function testTestnetConnection() {
  console.log("🧪 Probando conexión a TESTNET Binance...");

  // 1. Obtener ListenKey específica de testnet
  const listenKey = await getTestnetListenKey();

  if (!listenKey) {
    console.error("❌ No se pudo obtener ListenKey de testnet");
    return;
  }

  // 2. Conectar con esa ListenKey
  const wsUrl = `wss://testnet.binance.vision/ws/${listenKey}`;
  console.log("🔌 Conectando a:", wsUrl);

  const ws = new WebSocket(wsUrl);

  ws.on("open", () => {
    console.log("✅ Conexión exitosa a TESTNET!");
    console.log("   ListenKey válida para testnet");

    // Mantener abierta por 5 segundos para demostrar
    setTimeout(() => {
      ws.close(1000, "Test completado");
    }, 5000);
  });

  ws.on("message", (data) => {
    console.log("📨 Evento recibido:", JSON.parse(data).e || "unknown");
  });

  ws.on("error", (error) => {
    console.error("❌ Error WebSocket:", error.message);
  });

  ws.on("close", (code, reason) => {
    console.log(`🔌 Conexión cerrada: ${code} - ${reason}`);
  });
}

// Ejecutar
testTestnetConnection();
