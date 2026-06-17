// ══════════════════════════════════════════════════════════════
// CENTRAL BACKEND — Conecta WhatsApp Business API (Meta) con CENTRAL
// ══════════════════════════════════════════════════════════════
require('dotenv').config();
const express = require('express');
const cors = require('cors');
const axios = require('axios');
const http = require('http');
const { WebSocketServer } = require('ws');

const app = express();
app.use(cors());
app.use(express.json());

const PORT = process.env.PORT || 3000;
const PHONE_NUMBER_ID = process.env.WHATSAPP_PHONE_NUMBER_ID;
const WHATSAPP_TOKEN = process.env.WHATSAPP_TOKEN;
const VERIFY_TOKEN = process.env.WEBHOOK_VERIFY_TOKEN || 'central_webhook_secreto_123';
const GRAPH_API_VERSION = 'v21.0';

// ── In-memory store ──────────────────────────────────────────
const conversations = {};   // { [waId]: { name, messages: [...] } }

function getOrCreateConvo(waId, name) {
  if (!conversations[waId]) {
    conversations[waId] = {
      waId,
      name: name || waId,
      messages: [],
      lastMessageAt: null,
    };
  }
  return conversations[waId];
}

// ── WebSocket setup — para avisar a CENTRAL en tiempo real ─────
const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws' });

function broadcast(data) {
  const payload = JSON.stringify(data);
  wss.clients.forEach(client => {
    if (client.readyState === 1) client.send(payload);
  });
}

wss.on('connection', (ws) => {
  console.log('🔌 Cliente CENTRAL conectado via WebSocket');
  ws.send(JSON.stringify({ type: 'init', conversations }));
  ws.on('close', () => console.log('🔌 Cliente CENTRAL desconectado'));
});

// ══════════════════════════════════════════════════════════════
// WEBHOOK — Meta envía aquí los mensajes entrantes de WhatsApp
// ══════════════════════════════════════════════════════════════

app.get('/webhook', (req, res) => {
  const mode = req.query['hub.mode'];
  const token = req.query['hub.verify_token'];
  const challenge = req.query['hub.challenge'];

  if (mode === 'subscribe' && token === VERIFY_TOKEN) {
    console.log('✅ Webhook verificado correctamente por Meta');
    return res.status(200).send(challenge);
  }
  console.warn('❌ Verificación de webhook fallida — token no coincide');
  return res.sendStatus(403);
});

app.post('/webhook', (req, res) => {
  try {
    const body = req.body;

    if (body.object !== 'whatsapp_business_account') {
      return res.sendStatus(404);
    }

    const entry = body.entry?.[0];
    const change = entry?.changes?.[0];
    const value = change?.value;

    const messages = value?.messages;
    if (messages && messages.length > 0) {
      messages.forEach(msg => {
        const waId = msg.from;
        const contactName = value.contacts?.[0]?.profile?.name || waId;
        const convo = getOrCreateConvo(waId, contactName);

        let text = '';
        let type = msg.type;

        if (msg.type === 'text') {
          text = msg.text.body;
        } else if (msg.type === 'image') {
          text = '📷 Imagen recibida';
        } else if (msg.type === 'audio') {
          text = '🎤 Audio recibido';
        } else if (msg.type === 'document') {
          text = '📄 Documento recibido';
        } else {
          text = `[Mensaje tipo: ${msg.type}]`;
        }

        const newMessage = {
          id: msg.id,
          role: 'in',
          type: 'text',
          text,
          time: new Date(parseInt(msg.timestamp) * 1000).toISOString(),
          rawType: type,
        };

        convo.messages.push(newMessage);
        convo.lastMessageAt = newMessage.time;

        console.log(`📩 Mensaje de ${contactName} (${waId}): ${text}`);

        broadcast({
          type: 'new_message',
          waId,
          name: contactName,
          message: newMessage,
        });
      });
    }

    const statuses = value?.statuses;
    if (statuses && statuses.length > 0) {
      statuses.forEach(st => {
        broadcast({
          type: 'message_status',
          messageId: st.id,
          status: st.status,
          waId: st.recipient_id,
        });
      });
    }

    return res.sendStatus(200);
  } catch (err) {
    console.error('Error procesando webhook:', err);
    return res.sendStatus(500);
  }
});

// ══════════════════════════════════════════════════════════════
// API — Endpoints que CENTRAL usa para leer/enviar mensajes
// ══════════════════════════════════════════════════════════════

app.get('/api/conversations', (req, res) => {
  res.json(Object.values(conversations));
});

app.get('/api/conversations/:waId', (req, res) => {
  const convo = conversations[req.params.waId];
  if (!convo) return res.status(404).json({ error: 'Conversación no encontrada' });
  res.json(convo);
});

app.post('/api/send', async (req, res) => {
  const { waId, text } = req.body;

  if (!waId || !text) {
    return res.status(400).json({ error: 'Faltan waId o text' });
  }
  if (!PHONE_NUMBER_ID || !WHATSAPP_TOKEN) {
    return res.status(500).json({ error: 'Backend no configurado: faltan credenciales de Meta' });
  }

  try {
    const response = await axios.post(
      `https://graph.facebook.com/${GRAPH_API_VERSION}/${PHONE_NUMBER_ID}/messages`,
      {
        messaging_product: 'whatsapp',
        to: waId,
        type: 'text',
        text: { body: text },
      },
      {
        headers: {
          Authorization: `Bearer ${WHATSAPP_TOKEN}`,
          'Content-Type': 'application/json',
        },
      }
    );

    const convo = getOrCreateConvo(waId);
    const sentMessage = {
      id: response.data.messages?.[0]?.id || 'local_' + Date.now(),
      role: 'out',
      type: 'text',
      text,
      time: new Date().toISOString(),
    };
    convo.messages.push(sentMessage);
    convo.lastMessageAt = sentMessage.time;

    broadcast({ type: 'message_sent', waId, message: sentMessage });

    res.json({ success: true, message: sentMessage });
  } catch (err) {
    console.error('Error enviando mensaje:', err.response?.data || err.message);
    res.status(500).json({
      error: 'Error al enviar el mensaje',
      details: err.response?.data || err.message,
    });
  }
});

app.get('/', (req, res) => {
  res.json({
    status: 'ok',
    service: 'CENTRAL Backend',
    whatsapp_configured: !!(PHONE_NUMBER_ID && WHATSAPP_TOKEN),
    conversations_count: Object.keys(conversations).length,
  });
});

app.get('/health', (req, res) => res.json({ status: 'ok', timestamp: new Date().toISOString() }));

// ══════════════════════════════════════════════════════════════
server.listen(PORT, () => {
  console.log(`\n🚀 CENTRAL Backend corriendo en puerto ${PORT}`);
  console.log(`📡 Webhook URL para Meta: https://<tu-dominio>/webhook`);
  console.log(`🔑 Verify Token configurado: ${VERIFY_TOKEN}`);
  console.log(`📱 Phone Number ID: ${PHONE_NUMBER_ID || '⚠️  NO CONFIGURADO — revisa tu .env'}`);
  console.log(`🔌 WebSocket disponible en /ws\n`);
});
