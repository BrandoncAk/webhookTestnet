const WebSocket = require("ws");

const LISTEN_KEY =
  "Qq22nOGeT0KmISVRPDnNzRKtIYFRwxMFQPLpcTN13PPs0zp49JGqDb33bzkG";
const ws = new WebSocket(`wss://stream.binance.com/ws/${LISTEN_KEY}`);

ws.on("open", () => {
  console.log("✅ Conexión exitosa! El Listen Key es válido");
  ws.close();
});

ws.on("error", (error) => {
  console.error("❌ Error:", error.message);
});

ws.on("close", (code, reason) => {
  console.log("Código:", code);
  console.log("Razón:", reason.toString());
});
