// ══════════════════════════════════════════════════════════════
// CENTRAL BACKEND — Conecta WhatsApp Business API (Meta) con CENTRAL
// v3 — soporte de archivos (imágenes/docs/audio) + envío de tickets
//      de presupuesto/pedido como imagen real por WhatsApp
// ══════════════════════════════════════════════════════════════
require('dotenv').config();
const express = require('express');
const cors = require('cors');
const axios = require('axios');
const http = require('http');
const { WebSocketServer } = require('ws');
const multer = require('multer');
const FormData = require('form-data');
const puppeteer = require('puppeteer');

const app = express();
app.use(cors());
app.use(express.json({ limit: '20mb' }));

const PORT = process.env.PORT || 3000;
const PHONE_NUMBER_ID = process.env.WHATSAPP_PHONE_NUMBER_ID;
const WHATSAPP_TOKEN = process.env.WHATSAPP_TOKEN;
const VERIFY_TOKEN = process.env.WEBHOOK_VERIFY_TOKEN || 'central_webhook_secreto_123';
const GRAPH_API_VERSION = 'v21.0';
const GRAPH_BASE = `https://graph.facebook.com/${GRAPH_API_VERSION}`;

const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 20 * 1024 * 1024 } });

// ── In-memory store (pendiente migrar a DB persistente) ───────
const conversations = {};
const mediaCache = {}; // { mediaId: { buffer, mimeType, size } }

function getOrCreateConvo(waId, name) {
  if (!conversations[waId]) {
    conversations[waId] = { waId, name: name || waId, messages: [], lastMessageAt: null };
  }
  return conversations[waId];
}

// ── WebSocket ───────────────────────────────────────────────
const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws' });

function broadcast(data) {
  const payload = JSON.stringify(data);
  wss.clients.forEach(client => { if (client.readyState === 1) client.send(payload); });
}

wss.on('connection', (ws) => {
  console.log('🔌 Cliente CENTRAL conectado via WebSocket');
  ws.send(JSON.stringify({ type: 'init', conversations }));
  ws.on('close', () => console.log('🔌 Cliente CENTRAL desconectado'));
});

// ══════════════════════════════════════════════════════════════
// PUPPETEER — navegador headless reutilizable para renderizar HTML → imagen
// ══════════════════════════════════════════════════════════════
let browserInstance = null;

async function getBrowser() {
  if (browserInstance && browserInstance.isConnected()) return browserInstance;

  const launchOptions = {
    headless: 'new',
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--disable-gpu',
      '--single-process', // ayuda en entornos con memoria limitada como Railway free tier
    ],
  };

  // En Railway/Docker a veces Puppeteer necesita la ruta explícita del binario.
  // Prioridad: 1) variable de entorno explícita, 2) chromium instalado por nixpacks
  if (process.env.PUPPETEER_EXECUTABLE_PATH) {
    launchOptions.executablePath = process.env.PUPPETEER_EXECUTABLE_PATH;
  } else {
    const fs = require('fs');
    const commonPaths = [
      '/usr/bin/chromium',
      '/usr/bin/chromium-browser',
      '/nix/var/nix/profiles/default/bin/chromium',
    ];
    const found = commonPaths.find(p => { try { return fs.existsSync(p); } catch { return false; } });
    if (found) launchOptions.executablePath = found;
  }

  browserInstance = await puppeteer.launch(launchOptions);
  return browserInstance;
}

// Convierte un fragmento de HTML (el ticket) en un PNG real
async function renderHtmlToImage(htmlContent, width = 380) {
  const browser = await getBrowser();
  const page = await browser.newPage();
  try {
    await page.setViewport({ width, height: 100, deviceScaleFactor: 2 }); // 2x para nitidez

    const fullHtml = `
      <!DOCTYPE html>
      <html>
      <head>
        <meta charset="UTF-8">
        <style>
          * { box-sizing: border-box; margin: 0; padding: 0; font-family: -apple-system, 'Segoe UI', Arial, sans-serif; }
          body { background: #ffffff; padding: 20px; width: ${width - 40}px; }
        </style>
      </head>
      <body>${htmlContent}</body>
      </html>
    `;

    await page.setContent(fullHtml, { waitUntil: 'networkidle0' });
    const bodyHandle = await page.$('body');
    const box = await bodyHandle.boundingBox();

    const imageBuffer = await page.screenshot({
      type: 'png',
      clip: { x: 0, y: 0, width, height: Math.ceil(box.height) },
    });

    return imageBuffer;
  } finally {
    await page.close();
  }
}

// ══════════════════════════════════════════════════════════════
// HELPERS — descarga/subida de media con Meta
// ══════════════════════════════════════════════════════════════

async function downloadMediaFromMeta(mediaId) {
  const urlRes = await axios.get(`${GRAPH_BASE}/${mediaId}`, {
    headers: { Authorization: `Bearer ${WHATSAPP_TOKEN}` },
  });
  const { url, mime_type, file_size } = urlRes.data;

  const fileRes = await axios.get(url, {
    headers: { Authorization: `Bearer ${WHATSAPP_TOKEN}` },
    responseType: 'arraybuffer',
  });

  const buffer = Buffer.from(fileRes.data);
  mediaCache[mediaId] = { buffer, mimeType: mime_type, size: file_size };
  return mediaCache[mediaId];
}

async function uploadMediaToMeta(buffer, mimeType, filename) {
  const form = new FormData();
  form.append('file', buffer, { filename, contentType: mimeType });
  form.append('messaging_product', 'whatsapp');

  const res = await axios.post(`${GRAPH_BASE}/${PHONE_NUMBER_ID}/media`, form, {
    headers: { Authorization: `Bearer ${WHATSAPP_TOKEN}`, ...form.getHeaders() },
  });
  return res.data.id;
}

function whatsappTypeFromMime(mimeType) {
  if (mimeType.startsWith('image/')) return 'image';
  if (mimeType.startsWith('audio/')) return 'audio';
  if (mimeType.startsWith('video/')) return 'video';
  return 'document';
}

// Envía un mensaje de tipo media (imagen/documento/etc.) ya subido a Meta
async function sendMediaMessage(waId, mediaId, waType, caption, filename) {
  const payload = {
    messaging_product: 'whatsapp',
    to: waId,
    type: waType,
    [waType]: { id: mediaId },
  };
  if (caption && waType !== 'audio') payload[waType].caption = caption;
  if (waType === 'document' && filename) payload[waType].filename = filename;

  const response = await axios.post(`${GRAPH_BASE}/${PHONE_NUMBER_ID}/messages`, payload, {
    headers: { Authorization: `Bearer ${WHATSAPP_TOKEN}`, 'Content-Type': 'application/json' },
  });
  return response.data;
}

// ══════════════════════════════════════════════════════════════
// WEBHOOK — recepción de mensajes entrantes (texto y archivos)
// ══════════════════════════════════════════════════════════════

app.get('/webhook', (req, res) => {
  const mode = req.query['hub.mode'];
  const token = req.query['hub.verify_token'];
  const challenge = req.query['hub.challenge'];
  if (mode === 'subscribe' && token === VERIFY_TOKEN) {
    console.log('✅ Webhook verificado correctamente por Meta');
    return res.status(200).send(challenge);
  }
  console.warn('❌ Verificación de webhook fallida');
  return res.sendStatus(403);
});

app.post('/webhook', async (req, res) => {
  res.sendStatus(200); // responder rápido, procesar después

  try {
    const body = req.body;
    if (body.object !== 'whatsapp_business_account') return;

    const entry = body.entry?.[0];
    const change = entry?.changes?.[0];
    const value = change?.value;
    const messages = value?.messages;

    if (!messages || messages.length === 0) {
      const statuses = value?.statuses;
      if (statuses && statuses.length > 0) {
        statuses.forEach(st => {
          broadcast({ type: 'message_status', messageId: st.id, status: st.status, waId: st.recipient_id });
        });
      }
      return;
    }

    for (const msg of messages) {
      const waId = msg.from;
      const contactName = value.contacts?.[0]?.profile?.name || waId;
      const convo = getOrCreateConvo(waId, contactName);

      let newMessage = { id: msg.id, role: 'in', time: new Date(parseInt(msg.timestamp) * 1000).toISOString() };

      if (msg.type === 'text') {
        newMessage.type = 'text';
        newMessage.text = msg.text.body;
      } else if (['image', 'audio', 'video', 'document', 'sticker'].includes(msg.type)) {
        const mediaInfo = msg[msg.type];
        newMessage.type = msg.type === 'sticker' ? 'image' : msg.type;
        newMessage.mediaId = mediaInfo.id;
        newMessage.mimeType = mediaInfo.mime_type;
        newMessage.caption = mediaInfo.caption || '';
        newMessage.filename = mediaInfo.filename || null;
        newMessage.mediaUrl = `/api/media/${mediaInfo.id}`;
        try { await downloadMediaFromMeta(mediaInfo.id); }
        catch (err) { console.error('Error descargando media:', err.response?.data || err.message); }
      } else {
        newMessage.type = 'text';
        newMessage.text = `[Mensaje tipo no soportado: ${msg.type}]`;
      }

      convo.messages.push(newMessage);
      convo.lastMessageAt = newMessage.time;
      console.log(`📩 Mensaje de ${contactName} (${waId}): ${newMessage.type}`);
      broadcast({ type: 'new_message', waId, name: contactName, message: newMessage });
    }
  } catch (err) {
    console.error('Error procesando webhook:', err);
  }
});

// ══════════════════════════════════════════════════════════════
// API — endpoints generales
// ══════════════════════════════════════════════════════════════

app.get('/api/conversations', (req, res) => res.json(Object.values(conversations)));

app.get('/api/conversations/:waId', (req, res) => {
  const convo = conversations[req.params.waId];
  if (!convo) return res.status(404).json({ error: 'Conversación no encontrada' });
  res.json(convo);
});

app.get('/api/media/:mediaId', async (req, res) => {
  const { mediaId } = req.params;
  try {
    let cached = mediaCache[mediaId];
    if (!cached) cached = await downloadMediaFromMeta(mediaId);
    res.set('Content-Type', cached.mimeType);
    res.send(cached.buffer);
  } catch (err) {
    console.error('Error sirviendo media:', err.response?.data || err.message);
    res.status(404).json({ error: 'Archivo no encontrado o expirado' });
  }
});

// ── Enviar mensaje de texto ────────────────────────────────────
app.post('/api/send', async (req, res) => {
  const { waId, text } = req.body;
  if (!waId || !text) return res.status(400).json({ error: 'Faltan waId o text' });
  if (!PHONE_NUMBER_ID || !WHATSAPP_TOKEN) return res.status(500).json({ error: 'Backend no configurado' });

  try {
    const response = await axios.post(
      `${GRAPH_BASE}/${PHONE_NUMBER_ID}/messages`,
      { messaging_product: 'whatsapp', to: waId, type: 'text', text: { body: text } },
      { headers: { Authorization: `Bearer ${WHATSAPP_TOKEN}`, 'Content-Type': 'application/json' } }
    );
    const convo = getOrCreateConvo(waId);
    const sentMessage = {
      id: response.data.messages?.[0]?.id || 'local_' + Date.now(),
      role: 'out', type: 'text', text, time: new Date().toISOString(),
    };
    convo.messages.push(sentMessage);
    convo.lastMessageAt = sentMessage.time;
    broadcast({ type: 'message_sent', waId, message: sentMessage });
    res.json({ success: true, message: sentMessage });
  } catch (err) {
    console.error('Error enviando mensaje:', err.response?.data || err.message);
    res.status(500).json({ error: 'Error al enviar el mensaje', details: err.response?.data || err.message });
  }
});

// ── Enviar un archivo subido por el usuario (adjuntos manuales) ──
app.post('/api/send-media', upload.single('file'), async (req, res) => {
  const { waId, caption } = req.body;
  const file = req.file;
  if (!waId || !file) return res.status(400).json({ error: 'Faltan waId o file' });
  if (!PHONE_NUMBER_ID || !WHATSAPP_TOKEN) return res.status(500).json({ error: 'Backend no configurado' });

  try {
    const mediaId = await uploadMediaToMeta(file.buffer, file.mimetype, file.originalname);
    const waType = whatsappTypeFromMime(file.mimetype);
    const result = await sendMediaMessage(waId, mediaId, waType, caption, file.originalname);

    mediaCache[mediaId] = { buffer: file.buffer, mimeType: file.mimetype, size: file.size };

    const convo = getOrCreateConvo(waId);
    const sentMessage = {
      id: result.messages?.[0]?.id || 'local_' + Date.now(),
      role: 'out', type: waType, mediaId, mimeType: file.mimetype,
      filename: file.originalname, caption: caption || '',
      mediaUrl: `/api/media/${mediaId}`, time: new Date().toISOString(),
    };
    convo.messages.push(sentMessage);
    convo.lastMessageAt = sentMessage.time;
    broadcast({ type: 'message_sent', waId, message: sentMessage });
    res.json({ success: true, message: sentMessage });
  } catch (err) {
    console.error('Error enviando archivo:', err.response?.data || err.message);
    res.status(500).json({ error: 'Error al enviar el archivo', details: err.response?.data || err.message });
  }
});

// ══════════════════════════════════════════════════════════════
// ── Enviar TICKET (presupuesto / confirmación de pedido) ───────
// CENTRAL manda el HTML interno del ticket, el backend lo convierte
// en imagen real y la envía por WhatsApp como una imagen normal.
// ══════════════════════════════════════════════════════════════
app.post('/api/send-ticket', async (req, res) => {
  const { waId, ticketHtml, caption, orderType } = req.body;
  // orderType: 'presupuesto' | 'confirmacion' — solo informativo para logs

  if (!waId || !ticketHtml) return res.status(400).json({ error: 'Faltan waId o ticketHtml' });
  if (!PHONE_NUMBER_ID || !WHATSAPP_TOKEN) return res.status(500).json({ error: 'Backend no configurado' });

  try {
    // 1) Renderizar el HTML del ticket como imagen PNG
    const imageBuffer = await renderHtmlToImage(ticketHtml);

    // 2) Subir esa imagen a Meta
    const mediaId = await uploadMediaToMeta(imageBuffer, 'image/png', `ticket_${Date.now()}.png`);

    // 3) Enviarla como mensaje de imagen al cliente
    const result = await sendMediaMessage(waId, mediaId, 'image', caption || '');

    // 4) Guardar en el historial del backend
    mediaCache[mediaId] = { buffer: imageBuffer, mimeType: 'image/png', size: imageBuffer.length };

    const convo = getOrCreateConvo(waId);
    const sentMessage = {
      id: result.messages?.[0]?.id || 'local_' + Date.now(),
      role: 'out', type: 'image', mediaId, mimeType: 'image/png',
      caption: caption || '', mediaUrl: `/api/media/${mediaId}`,
      orderType: orderType || null,
      time: new Date().toISOString(),
    };
    convo.messages.push(sentMessage);
    convo.lastMessageAt = sentMessage.time;

    broadcast({ type: 'message_sent', waId, message: sentMessage });

    console.log(`🧾 Ticket (${orderType || 'sin tipo'}) enviado a ${waId} como imagen`);
    res.json({ success: true, message: sentMessage, mediaId });
  } catch (err) {
    console.error('Error enviando ticket:', err.response?.data || err.message);
    res.status(500).json({ error: 'Error al generar/enviar el ticket', details: err.response?.data || err.message });
  }
});

// ── Health check ──────────────────────────────────────────────
app.get('/', (req, res) => {
  res.json({
    status: 'ok',
    service: 'CENTRAL Backend',
    version: '3.0 — archivos + tickets de pedido',
    whatsapp_configured: !!(PHONE_NUMBER_ID && WHATSAPP_TOKEN),
    conversations_count: Object.keys(conversations).length,
    media_cached: Object.keys(mediaCache).length,
  });
});

app.get('/health', (req, res) => res.json({ status: 'ok', timestamp: new Date().toISOString() }));

// ── Cierre limpio del navegador headless al apagar el server ───
process.on('SIGTERM', async () => {
  if (browserInstance) await browserInstance.close();
  process.exit(0);
});

// ══════════════════════════════════════════════════════════════
server.listen(PORT, () => {
  console.log(`\n🚀 CENTRAL Backend v3 corriendo en puerto ${PORT}`);
  console.log(`📡 Webhook URL para Meta: https://<tu-dominio>/webhook`);
  console.log(`🔑 Verify Token configurado: ${VERIFY_TOKEN}`);
  console.log(`📱 Phone Number ID: ${PHONE_NUMBER_ID || '⚠️  NO CONFIGURADO'}`);
  console.log(`📎 Soporte de archivos: imágenes, documentos, audio, video`);
  console.log(`🧾 Soporte de tickets de pedido (HTML → imagen → WhatsApp)`);
  console.log(`🔌 WebSocket disponible en /ws\n`);
});
