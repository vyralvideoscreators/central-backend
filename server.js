// ══════════════════════════════════════════════════════════════
// CENTRAL BACKEND v5.0
// Multi-tenant + PostgreSQL + WhatsApp Business API + Instagram DMs
// ══════════════════════════════════════════════════════════════
require('dotenv').config();
const express    = require('express');
const cors       = require('cors');
const axios      = require('axios');
const http       = require('http');
const { WebSocketServer } = require('ws');
const multer     = require('multer');
const FormData   = require('form-data');
const puppeteer  = require('puppeteer');
const { Pool }   = require('pg');
const bcrypt     = require('bcrypt');
const jwt        = require('jsonwebtoken');

const app = express();
app.use(cors());
app.use(express.json({ limit: '20mb' }));

const PORT             = process.env.PORT || 3000;
const GRAPH_API_VERSION = 'v21.0';
const GRAPH_BASE       = `https://graph.facebook.com/${GRAPH_API_VERSION}`;
const JWT_SECRET       = process.env.JWT_SECRET || 'central_jwt_dev_secret_cambiar_en_prod';

// Variables de entorno de compatibilidad (para el tenant por defecto / legacy)
const DEFAULT_PHONE_ID  = process.env.WHATSAPP_PHONE_NUMBER_ID;
const DEFAULT_WA_TOKEN  = process.env.WHATSAPP_TOKEN;
const DEFAULT_IG_TOKEN  = process.env.INSTAGRAM_ACCESS_TOKEN;
const DEFAULT_IG_ACCT   = process.env.INSTAGRAM_ACCOUNT_ID || '17841426682340000';
const VERIFY_TOKEN      = process.env.WEBHOOK_VERIFY_TOKEN || 'central_webhook_secreto_123';

const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 20 * 1024 * 1024 } });

// ── In-memory media cache (binarios no van a DB) ──────────────
const mediaCache = {};

// ══════════════════════════════════════════════════════════════
// POSTGRESQL
// ══════════════════════════════════════════════════════════════
let pool = null;
let dbReady = false;

if (process.env.DATABASE_URL) {
  pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false },
  });
}

async function initDB() {
  if (!pool) {
    console.warn('⚠️  DATABASE_URL no configurado — datos solo en memoria');
    return;
  }
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS tenants (
        id                        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        business_name             TEXT NOT NULL,
        email                     TEXT UNIQUE NOT NULL,
        password_hash             TEXT NOT NULL,
        plan                      TEXT DEFAULT 'basic',
        whatsapp_phone_number_id  TEXT,
        whatsapp_token            TEXT,
        instagram_access_token    TEXT,
        instagram_account_id      TEXT,
        webhook_verify_token      TEXT DEFAULT 'central_webhook_secreto_123',
        created_at                TIMESTAMP DEFAULT NOW(),
        active                    BOOLEAN DEFAULT true
      );

      CREATE TABLE IF NOT EXISTS conversations (
        wa_id          TEXT NOT NULL,
        tenant_id      UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
        name           TEXT,
        platform       TEXT DEFAULT 'wa',
        ig_sender_id   TEXT,
        last_message_at TIMESTAMP,
        PRIMARY KEY (wa_id, tenant_id)
      );

      CREATE TABLE IF NOT EXISTS messages (
        id         TEXT    NOT NULL,
        wa_id      TEXT    NOT NULL,
        tenant_id  UUID    NOT NULL,
        role       TEXT    NOT NULL,
        type       TEXT    NOT NULL,
        text       TEXT,
        media_id   TEXT,
        mime_type  TEXT,
        media_url  TEXT,
        caption    TEXT,
        filename   TEXT,
        order_type TEXT,
        time       TIMESTAMP NOT NULL,
        created_at TIMESTAMP DEFAULT NOW(),
        PRIMARY KEY (id, tenant_id),
        FOREIGN KEY (wa_id, tenant_id) REFERENCES conversations(wa_id, tenant_id) ON DELETE CASCADE
      );
    `);

    await seedDefaultTenant();
    dbReady = true;
    console.log('✅ Base de datos PostgreSQL lista');
  } catch (err) {
    console.error('⚠️  Error inicializando DB:', err.message);
    console.warn('⚠️  El servidor continuará sin persistencia en DB');
  }
}

async function seedDefaultTenant() {
  const email    = process.env.DEFAULT_TENANT_EMAIL;
  const password = process.env.DEFAULT_TENANT_PASSWORD;
  if (!email || !password) return;

  const { rows } = await pool.query('SELECT id FROM tenants WHERE email = $1', [email]);
  if (rows.length > 0) {
    // Actualizar credenciales de WhatsApp/Instagram si cambiaron
    await pool.query(`
      UPDATE tenants SET
        whatsapp_phone_number_id = COALESCE($1, whatsapp_phone_number_id),
        whatsapp_token           = COALESCE($2, whatsapp_token),
        instagram_access_token   = COALESCE($3, instagram_access_token),
        instagram_account_id     = COALESCE($4, instagram_account_id)
      WHERE email = $5
    `, [DEFAULT_PHONE_ID || null, DEFAULT_WA_TOKEN || null, DEFAULT_IG_TOKEN || null, DEFAULT_IG_ACCT || null, email]);
    return;
  }

  const hash = await bcrypt.hash(password, 10);
  await pool.query(`
    INSERT INTO tenants (business_name, email, password_hash, plan, whatsapp_phone_number_id, whatsapp_token, instagram_access_token, instagram_account_id)
    VALUES ($1, $2, $3, 'basic', $4, $5, $6, $7)
  `, ['Mi Negocio', email, hash, DEFAULT_PHONE_ID || null, DEFAULT_WA_TOKEN || null, DEFAULT_IG_TOKEN || null, DEFAULT_IG_ACCT || null]);
  console.log(`✅ Tenant por defecto creado: ${email}`);
}

// ── DB helpers ────────────────────────────────────────────────

async function dbGetOrCreateConvo(waId, name, tenantId, platform = 'wa', igSenderId = null) {
  if (!dbReady) return;
  await pool.query(`
    INSERT INTO conversations (wa_id, tenant_id, name, platform, ig_sender_id, last_message_at)
    VALUES ($1, $2, $3, $4, $5, NOW())
    ON CONFLICT (wa_id, tenant_id) DO UPDATE SET
      name            = COALESCE(EXCLUDED.name, conversations.name),
      last_message_at = NOW()
  `, [waId, tenantId, name || waId, platform, igSenderId]);
}

async function dbSaveMessage(msg, waId, tenantId) {
  if (!dbReady) return;
  await pool.query(`
    INSERT INTO messages (id, wa_id, tenant_id, role, type, text, media_id, mime_type, media_url, caption, filename, order_type, time)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
    ON CONFLICT (id, tenant_id) DO NOTHING
  `, [
    msg.id, waId, tenantId, msg.role, msg.type,
    msg.text || null, msg.mediaId || null, msg.mimeType || null,
    msg.mediaUrl || null, msg.caption || null, msg.filename || null,
    msg.orderType || null, new Date(msg.time),
  ]);
}

async function dbGetConversations(tenantId) {
  if (!dbReady) return [];
  const { rows } = await pool.query(`
    SELECT
      c.wa_id, c.name, c.platform, c.ig_sender_id, c.last_message_at,
      COALESCE(
        json_agg(
          json_build_object(
            'id', m.id, 'role', m.role, 'type', m.type, 'text', m.text,
            'mediaId', m.media_id, 'mimeType', m.mime_type, 'mediaUrl', m.media_url,
            'caption', m.caption, 'filename', m.filename, 'orderType', m.order_type,
            'time', to_char(m.time AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
          ) ORDER BY m.time
        ) FILTER (WHERE m.id IS NOT NULL),
        '[]'::json
      ) AS messages
    FROM conversations c
    LEFT JOIN messages m ON m.wa_id = c.wa_id AND m.tenant_id = c.tenant_id
    WHERE c.tenant_id = $1
    GROUP BY c.wa_id, c.tenant_id
    ORDER BY c.last_message_at DESC NULLS LAST
  `, [tenantId]);

  return rows.map(r => ({
    waId:          r.wa_id,
    name:          r.name,
    platform:      r.platform,
    igSenderId:    r.ig_sender_id,
    lastMessageAt: r.last_message_at,
    messages:      r.messages || [],
  }));
}

async function dbGetTenantByPhoneId(phoneNumberId) {
  if (!dbReady) return null;
  const { rows } = await pool.query(
    'SELECT * FROM tenants WHERE whatsapp_phone_number_id = $1 AND active = true LIMIT 1',
    [phoneNumberId]
  );
  return rows[0] || null;
}

async function dbGetTenantByIgAccountId(igAccountId) {
  if (!dbReady) return null;
  const { rows } = await pool.query(
    'SELECT * FROM tenants WHERE instagram_account_id = $1 AND active = true LIMIT 1',
    [igAccountId]
  );
  return rows[0] || null;
}

// ══════════════════════════════════════════════════════════════
// WEBSOCKET — por tenant
// ══════════════════════════════════════════════════════════════
const server = http.createServer(app);
const wss    = new WebSocketServer({ server, path: '/ws' });

// tenantId → Set<WebSocket>
const tenantClients = new Map();

function broadcastToTenant(tenantId, data) {
  const clients = tenantClients.get(String(tenantId));
  if (!clients) return;
  const payload = JSON.stringify(data);
  clients.forEach(ws => { if (ws.readyState === 1) ws.send(payload); });
}

// Fallback: broadcast a todos cuando no hay multi-tenant en DB
function broadcastAll(data) {
  const payload = JSON.stringify(data);
  wss.clients.forEach(ws => { if (ws.readyState === 1) ws.send(payload); });
}

wss.on('connection', (ws) => {
  let tenantId = null;

  ws.on('message', async (raw) => {
    try {
      const msg = JSON.parse(raw.toString());
      if (msg.type === 'auth') {
        try {
          const decoded = jwt.verify(msg.token, JWT_SECRET);
          tenantId = String(decoded.tenantId);
          if (!tenantClients.has(tenantId)) tenantClients.set(tenantId, new Set());
          tenantClients.get(tenantId).add(ws);
          const convos = await dbGetConversations(tenantId);
          ws.send(JSON.stringify({ type: 'init', conversations: convos }));
          console.log(`🔌 Tenant ${tenantId} conectado via WebSocket`);
        } catch {
          ws.send(JSON.stringify({ type: 'auth_error', message: 'Token inválido' }));
          // Aun así permitir conexión en modo sin DB (legado)
          ws.send(JSON.stringify({ type: 'init', conversations: [] }));
        }
      }
    } catch {}
  });

  // Compatibilidad: si en 3s no autenticó, enviar init vacío
  const fallbackTimer = setTimeout(() => {
    if (!tenantId && ws.readyState === 1) {
      ws.send(JSON.stringify({ type: 'init', conversations: [] }));
    }
  }, 3000);

  ws.on('close', () => {
    clearTimeout(fallbackTimer);
    if (tenantId && tenantClients.has(tenantId)) {
      tenantClients.get(tenantId).delete(ws);
    }
  });
});

// ══════════════════════════════════════════════════════════════
// JWT MIDDLEWARE
// ══════════════════════════════════════════════════════════════
function requireAuth(req, res, next) {
  const auth = req.headers.authorization;
  if (!auth || !auth.startsWith('Bearer ')) {
    return res.status(401).json({ error: 'No autenticado — incluye Authorization: Bearer <token>' });
  }
  try {
    req.tenant = jwt.verify(auth.slice(7), JWT_SECRET);
    next();
  } catch {
    res.status(401).json({ error: 'Token inválido o expirado' });
  }
}

async function getTenantCredentials(tenantId) {
  // Prioridad: DB → env vars (compatibilidad)
  if (dbReady && tenantId) {
    const { rows } = await pool.query(
      'SELECT whatsapp_phone_number_id, whatsapp_token, instagram_access_token, instagram_account_id FROM tenants WHERE id = $1',
      [tenantId]
    );
    if (rows[0]) return rows[0];
  }
  return {
    whatsapp_phone_number_id: DEFAULT_PHONE_ID,
    whatsapp_token:           DEFAULT_WA_TOKEN,
    instagram_access_token:   DEFAULT_IG_TOKEN,
    instagram_account_id:     DEFAULT_IG_ACCT,
  };
}

// ══════════════════════════════════════════════════════════════
// PUPPETEER — renderizador de tickets HTML → imagen
// ══════════════════════════════════════════════════════════════
let browserInstance = null;

async function getBrowser() {
  if (browserInstance && browserInstance.isConnected()) return browserInstance;
  const opts = {
    headless: 'new',
    args: ['--no-sandbox','--disable-setuid-sandbox','--disable-dev-shm-usage','--disable-gpu','--single-process'],
  };
  if (process.env.PUPPETEER_EXECUTABLE_PATH) {
    opts.executablePath = process.env.PUPPETEER_EXECUTABLE_PATH;
  } else {
    const fs = require('fs');
    const paths = ['/usr/bin/chromium','/usr/bin/chromium-browser','/nix/var/nix/profiles/default/bin/chromium'];
    const found = paths.find(p => { try { return fs.existsSync(p); } catch { return false; } });
    if (found) opts.executablePath = found;
  }
  browserInstance = await puppeteer.launch(opts);
  return browserInstance;
}

async function renderHtmlToImage(htmlContent, width = 380) {
  const browser = await getBrowser();
  const page = await browser.newPage();
  try {
    await page.setViewport({ width, height: 100, deviceScaleFactor: 2 });
    await page.setContent(`<!DOCTYPE html><html><head><meta charset="UTF-8"><style>*{box-sizing:border-box;margin:0;padding:0;font-family:-apple-system,'Segoe UI',Arial,sans-serif;}body{background:#fff;padding:20px;width:${width-40}px;}</style></head><body>${htmlContent}</body></html>`, { waitUntil: 'networkidle0' });
    const body = await page.$('body');
    const box  = await body.boundingBox();
    return await page.screenshot({ type: 'png', clip: { x:0, y:0, width, height: Math.ceil(box.height) } });
  } finally {
    await page.close();
  }
}

// ══════════════════════════════════════════════════════════════
// HELPERS — Media (Meta Graph API)
// ══════════════════════════════════════════════════════════════

async function downloadMediaFromMeta(mediaId, token) {
  const t = token || DEFAULT_WA_TOKEN;
  const urlRes = await axios.get(`${GRAPH_BASE}/${mediaId}`, { headers: { Authorization: `Bearer ${t}` } });
  const { url, mime_type, file_size } = urlRes.data;
  const fileRes = await axios.get(url, { headers: { Authorization: `Bearer ${t}` }, responseType: 'arraybuffer' });
  const buffer = Buffer.from(fileRes.data);
  mediaCache[mediaId] = { buffer, mimeType: mime_type, size: file_size };
  return mediaCache[mediaId];
}

async function uploadMediaToMeta(buffer, mimeType, filename, phoneId, token) {
  const form = new FormData();
  form.append('file', buffer, { filename, contentType: mimeType });
  form.append('messaging_product', 'whatsapp');
  const res = await axios.post(`${GRAPH_BASE}/${phoneId}/media`, form, {
    headers: { Authorization: `Bearer ${token}`, ...form.getHeaders() },
  });
  return res.data.id;
}

function whatsappTypeFromMime(mimeType) {
  if (mimeType.startsWith('image/')) return 'image';
  if (mimeType.startsWith('audio/')) return 'audio';
  if (mimeType.startsWith('video/')) return 'video';
  return 'document';
}

async function sendMediaMessage(waId, mediaId, waType, caption, filename, phoneId, token) {
  const payload = { messaging_product:'whatsapp', to: waId, type: waType, [waType]: { id: mediaId } };
  if (caption && waType !== 'audio') payload[waType].caption = caption;
  if (waType === 'document' && filename) payload[waType].filename = filename;
  const res = await axios.post(`${GRAPH_BASE}/${phoneId}/messages`, payload, {
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
  });
  return res.data;
}

// ══════════════════════════════════════════════════════════════
// WEBHOOK — verificación
// ══════════════════════════════════════════════════════════════
app.get('/webhook', (req, res) => {
  const { 'hub.mode': mode, 'hub.verify_token': token, 'hub.challenge': challenge } = req.query;
  if (mode === 'subscribe' && token === VERIFY_TOKEN) {
    console.log('✅ Webhook verificado por Meta');
    return res.status(200).send(challenge);
  }
  console.warn('❌ Verificación de webhook fallida');
  res.sendStatus(403);
});

// ══════════════════════════════════════════════════════════════
// WEBHOOK — Instagram DMs
// ══════════════════════════════════════════════════════════════
async function handleInstagramWebhook(body) {
  try {
    const entry     = body.entry?.[0];
    const messaging = entry?.messaging?.[0];
    if (!messaging) return;

    const senderId = messaging.sender?.id;
    if (!senderId) return;

    // Identificar tenant por INSTAGRAM_ACCOUNT_ID del entry
    const igAccountId = entry.id; // Meta envía el ID de la cuenta IG en entry.id
    let tenantId = null;
    let igToken  = DEFAULT_IG_TOKEN;
    let igAcct   = DEFAULT_IG_ACCT;

    if (senderId === igAcct) return; // eco propio

    const tenant = await dbGetTenantByIgAccountId(igAccountId);
    if (tenant) {
      tenantId = tenant.id;
      igToken  = tenant.instagram_access_token;
      igAcct   = tenant.instagram_account_id;
      if (senderId === igAcct) return;
    }

    const msgData = messaging.message;
    if (!msgData) return;

    let senderName = senderId;
    try {
      const profileRes = await axios.get(
        `https://graph.instagram.com/${GRAPH_API_VERSION}/${senderId}`,
        { params: { fields: 'name,username', access_token: igToken } }
      );
      senderName = profileRes.data.name || profileRes.data.username || senderId;
    } catch {}

    const convoKey = `ig_${senderId}`;
    await dbGetOrCreateConvo(convoKey, senderName, tenantId, 'ig', senderId);

    const newMessage = {
      id:   msgData.mid || 'ig_' + Date.now(),
      role: 'in',
      type: msgData.attachments ? 'image' : 'text',
      text: msgData.text || (msgData.attachments ? '[Archivo adjunto]' : '[Mensaje vacío]'),
      time: new Date().toISOString(),
    };

    await dbSaveMessage(newMessage, convoKey, tenantId);
    console.log(`📸 Instagram DM de ${senderName}: ${newMessage.text}`);

    const broadcastData = { type:'new_message', waId:convoKey, name:senderName, platform:'ig', message:newMessage };
    if (tenantId) broadcastToTenant(tenantId, broadcastData);
    else broadcastAll(broadcastData);
  } catch (err) {
    console.error('Error procesando webhook Instagram:', err.message);
  }
}

// ══════════════════════════════════════════════════════════════
// WEBHOOK — WhatsApp Business
// ══════════════════════════════════════════════════════════════
app.post('/webhook', async (req, res) => {
  res.sendStatus(200);

  try {
    const body = req.body;

    if (body.object === 'instagram') {
      await handleInstagramWebhook(body);
      return;
    }

    if (body.object !== 'whatsapp_business_account') return;

    const entry  = body.entry?.[0];
    const change = entry?.changes?.[0];
    const value  = change?.value;

    // Identificar tenant por phone_number_id
    const phoneNumberId = value?.metadata?.phone_number_id;
    let tenantId  = null;
    let waToken   = DEFAULT_WA_TOKEN;
    let phoneId   = DEFAULT_PHONE_ID;

    const tenant = phoneNumberId ? await dbGetTenantByPhoneId(phoneNumberId) : null;
    if (tenant) {
      tenantId = tenant.id;
      waToken  = tenant.whatsapp_token;
      phoneId  = tenant.whatsapp_phone_number_id;
    }

    const messages = value?.messages;
    if (!messages || messages.length === 0) {
      const statuses = value?.statuses;
      if (statuses?.length > 0) {
        statuses.forEach(st => {
          const data = { type:'message_status', messageId:st.id, status:st.status, waId:st.recipient_id };
          if (tenantId) broadcastToTenant(tenantId, data);
          else broadcastAll(data);
        });
      }
      return;
    }

    for (const msg of messages) {
      const waId        = msg.from;
      const contactName = value.contacts?.[0]?.profile?.name || waId;

      await dbGetOrCreateConvo(waId, contactName, tenantId, 'wa');

      let newMessage = { id: msg.id, role:'in', time: new Date(parseInt(msg.timestamp) * 1000).toISOString() };

      if (msg.type === 'text') {
        newMessage.type = 'text';
        newMessage.text = msg.text.body;
      } else if (['image','audio','video','document','sticker'].includes(msg.type)) {
        const info = msg[msg.type];
        newMessage.type     = msg.type === 'sticker' ? 'image' : msg.type;
        newMessage.mediaId  = info.id;
        newMessage.mimeType = info.mime_type;
        newMessage.caption  = info.caption || '';
        newMessage.filename = info.filename || null;
        newMessage.mediaUrl = `/api/media/${info.id}`;
        try { await downloadMediaFromMeta(info.id, waToken); } catch {}
      } else {
        newMessage.type = 'text';
        newMessage.text = `[Tipo no soportado: ${msg.type}]`;
      }

      await dbSaveMessage(newMessage, waId, tenantId);
      console.log(`📩 Mensaje de ${contactName} (${waId}): ${newMessage.type}`);

      const broadcastData = { type:'new_message', waId, name:contactName, message:newMessage };
      if (tenantId) broadcastToTenant(tenantId, broadcastData);
      else broadcastAll(broadcastData);
    }
  } catch (err) {
    console.error('Error procesando webhook WhatsApp:', err);
  }
});

// ══════════════════════════════════════════════════════════════
// API — Tenants: registro, login, perfil
// ══════════════════════════════════════════════════════════════

app.post('/api/tenant/register', async (req, res) => {
  const { businessName, email, password, plan } = req.body;
  if (!businessName || !email || !password) {
    return res.status(400).json({ error: 'Faltan businessName, email o password' });
  }
  if (!dbReady) return res.status(503).json({ error: 'Base de datos no disponible' });

  try {
    const hash = await bcrypt.hash(password, 10);
    const { rows } = await pool.query(`
      INSERT INTO tenants (business_name, email, password_hash, plan)
      VALUES ($1, $2, $3, $4)
      RETURNING id, business_name, email, plan, created_at
    `, [businessName, email, hash, plan || 'basic']);
    const tenant = rows[0];
    const token  = jwt.sign({ tenantId: tenant.id, email: tenant.email }, JWT_SECRET, { expiresIn: '30d' });
    res.json({ success: true, token, tenant });
  } catch (err) {
    if (err.code === '23505') return res.status(409).json({ error: 'El correo ya está registrado' });
    res.status(500).json({ error: 'Error al registrar', details: err.message });
  }
});

app.post('/api/tenant/login', async (req, res) => {
  const { email, password } = req.body;
  if (!email || !password) return res.status(400).json({ error: 'Faltan email o password' });
  if (!dbReady) return res.status(503).json({ error: 'Base de datos no disponible' });

  try {
    const { rows } = await pool.query('SELECT * FROM tenants WHERE email = $1 AND active = true', [email]);
    const tenant = rows[0];
    if (!tenant) return res.status(401).json({ error: 'Credenciales incorrectas' });
    const valid = await bcrypt.compare(password, tenant.password_hash);
    if (!valid)  return res.status(401).json({ error: 'Credenciales incorrectas' });
    const token = jwt.sign({ tenantId: tenant.id, email: tenant.email }, JWT_SECRET, { expiresIn: '30d' });
    res.json({ success: true, token, tenant: { id: tenant.id, businessName: tenant.business_name, email: tenant.email, plan: tenant.plan } });
  } catch (err) {
    res.status(500).json({ error: 'Error al iniciar sesión', details: err.message });
  }
});

app.get('/api/tenant/profile', requireAuth, async (req, res) => {
  try {
    const { rows } = await pool.query(
      'SELECT id, business_name, email, plan, whatsapp_phone_number_id, instagram_account_id, created_at FROM tenants WHERE id = $1',
      [req.tenant.tenantId]
    );
    if (!rows[0]) return res.status(404).json({ error: 'Tenant no encontrado' });
    res.json(rows[0]);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.put('/api/tenant/whatsapp', requireAuth, async (req, res) => {
  const { phoneNumberId, token, instagramAccessToken, instagramAccountId } = req.body;
  if (!dbReady) return res.status(503).json({ error: 'Base de datos no disponible' });
  try {
    await pool.query(`
      UPDATE tenants SET
        whatsapp_phone_number_id = COALESCE($1, whatsapp_phone_number_id),
        whatsapp_token           = COALESCE($2, whatsapp_token),
        instagram_access_token   = COALESCE($3, instagram_access_token),
        instagram_account_id     = COALESCE($4, instagram_account_id)
      WHERE id = $5
    `, [phoneNumberId || null, token || null, instagramAccessToken || null, instagramAccountId || null, req.tenant.tenantId]);
    res.json({ success: true, message: 'Credenciales actualizadas' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ══════════════════════════════════════════════════════════════
// API — Conversaciones
// ══════════════════════════════════════════════════════════════

app.get('/api/conversations', requireAuth, async (req, res) => {
  try {
    const convos = await dbGetConversations(req.tenant.tenantId);
    res.json(convos);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/conversations/:waId', requireAuth, async (req, res) => {
  try {
    const { rows } = await pool.query(`
      SELECT c.wa_id, c.name, c.platform, c.ig_sender_id, c.last_message_at,
        COALESCE(
          json_agg(json_build_object(
            'id', m.id, 'role', m.role, 'type', m.type, 'text', m.text,
            'mediaId', m.media_id, 'mimeType', m.mime_type, 'mediaUrl', m.media_url,
            'caption', m.caption, 'filename', m.filename, 'orderType', m.order_type,
            'time', to_char(m.time AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
          ) ORDER BY m.time) FILTER (WHERE m.id IS NOT NULL), '[]'::json
        ) AS messages
      FROM conversations c
      LEFT JOIN messages m ON m.wa_id = c.wa_id AND m.tenant_id = c.tenant_id
      WHERE c.wa_id = $1 AND c.tenant_id = $2
      GROUP BY c.wa_id, c.tenant_id
    `, [req.params.waId, req.tenant.tenantId]);

    if (!rows[0]) return res.status(404).json({ error: 'Conversación no encontrada' });
    res.json({ waId: rows[0].wa_id, name: rows[0].name, platform: rows[0].platform,
               igSenderId: rows[0].ig_sender_id, messages: rows[0].messages || [] });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ══════════════════════════════════════════════════════════════
// API — Media
// ══════════════════════════════════════════════════════════════

app.get('/api/media/:mediaId', requireAuth, async (req, res) => {
  const { mediaId } = req.params;
  try {
    let cached = mediaCache[mediaId];
    if (!cached) {
      const creds = await getTenantCredentials(req.tenant.tenantId);
      cached = await downloadMediaFromMeta(mediaId, creds.whatsapp_token);
    }
    res.set('Content-Type', cached.mimeType);
    res.send(cached.buffer);
  } catch (err) {
    res.status(404).json({ error: 'Archivo no encontrado o expirado' });
  }
});

// ══════════════════════════════════════════════════════════════
// API — Enviar mensaje de texto (WhatsApp)
// ══════════════════════════════════════════════════════════════

app.post('/api/send', requireAuth, async (req, res) => {
  const { waId, text } = req.body;
  if (!waId || !text) return res.status(400).json({ error: 'Faltan waId o text' });

  try {
    const creds = await getTenantCredentials(req.tenant.tenantId);
    if (!creds.whatsapp_phone_number_id || !creds.whatsapp_token) {
      return res.status(500).json({ error: 'WhatsApp no configurado para este tenant' });
    }

    const response = await axios.post(
      `${GRAPH_BASE}/${creds.whatsapp_phone_number_id}/messages`,
      { messaging_product:'whatsapp', to:waId, type:'text', text:{ body:text } },
      { headers: { Authorization:`Bearer ${creds.whatsapp_token}`, 'Content-Type':'application/json' } }
    );

    const sentMessage = {
      id:   response.data.messages?.[0]?.id || 'local_' + Date.now(),
      role: 'out', type: 'text', text, time: new Date().toISOString(),
    };
    await dbGetOrCreateConvo(waId, null, req.tenant.tenantId, 'wa');
    await dbSaveMessage(sentMessage, waId, req.tenant.tenantId);
    broadcastToTenant(req.tenant.tenantId, { type:'message_sent', waId, message:sentMessage });
    res.json({ success: true, message: sentMessage });
  } catch (err) {
    console.error('Error enviando mensaje:', err.response?.data || err.message);
    res.status(500).json({ error:'Error al enviar', details: err.response?.data || err.message });
  }
});

// ══════════════════════════════════════════════════════════════
// API — Enviar archivo adjunto (WhatsApp)
// ══════════════════════════════════════════════════════════════

app.post('/api/send-media', requireAuth, upload.single('file'), async (req, res) => {
  const { waId, caption } = req.body;
  const file = req.file;
  if (!waId || !file) return res.status(400).json({ error: 'Faltan waId o file' });

  try {
    const creds  = await getTenantCredentials(req.tenant.tenantId);
    if (!creds.whatsapp_phone_number_id || !creds.whatsapp_token) {
      return res.status(500).json({ error: 'WhatsApp no configurado' });
    }
    const mediaId = await uploadMediaToMeta(file.buffer, file.mimetype, file.originalname, creds.whatsapp_phone_number_id, creds.whatsapp_token);
    const waType  = whatsappTypeFromMime(file.mimetype);
    const result  = await sendMediaMessage(waId, mediaId, waType, caption, file.originalname, creds.whatsapp_phone_number_id, creds.whatsapp_token);

    mediaCache[mediaId] = { buffer: file.buffer, mimeType: file.mimetype, size: file.size };

    const sentMessage = {
      id: result.messages?.[0]?.id || 'local_' + Date.now(),
      role:'out', type:waType, mediaId, mimeType:file.mimetype,
      filename:file.originalname, caption:caption||'', mediaUrl:`/api/media/${mediaId}`,
      time: new Date().toISOString(),
    };
    await dbGetOrCreateConvo(waId, null, req.tenant.tenantId, 'wa');
    await dbSaveMessage(sentMessage, waId, req.tenant.tenantId);
    broadcastToTenant(req.tenant.tenantId, { type:'message_sent', waId, message:sentMessage });
    res.json({ success: true, message: sentMessage });
  } catch (err) {
    console.error('Error enviando archivo:', err.response?.data || err.message);
    res.status(500).json({ error:'Error al enviar archivo', details: err.response?.data || err.message });
  }
});

// ══════════════════════════════════════════════════════════════
// API — Enviar ticket como imagen PNG (WhatsApp)
// ══════════════════════════════════════════════════════════════

app.post('/api/send-ticket', requireAuth, async (req, res) => {
  const { waId, ticketHtml, caption, orderType } = req.body;
  if (!waId || !ticketHtml) return res.status(400).json({ error: 'Faltan waId o ticketHtml' });

  try {
    const creds = await getTenantCredentials(req.tenant.tenantId);
    if (!creds.whatsapp_phone_number_id || !creds.whatsapp_token) {
      return res.status(500).json({ error: 'WhatsApp no configurado' });
    }

    const imageBuffer = await renderHtmlToImage(ticketHtml);
    const mediaId     = await uploadMediaToMeta(imageBuffer, 'image/png', `ticket_${Date.now()}.png`, creds.whatsapp_phone_number_id, creds.whatsapp_token);
    const result      = await sendMediaMessage(waId, mediaId, 'image', caption||'', null, creds.whatsapp_phone_number_id, creds.whatsapp_token);

    mediaCache[mediaId] = { buffer: imageBuffer, mimeType:'image/png', size: imageBuffer.length };

    const sentMessage = {
      id: result.messages?.[0]?.id || 'local_' + Date.now(),
      role:'out', type:'image', mediaId, mimeType:'image/png',
      caption:caption||'', mediaUrl:`/api/media/${mediaId}`, orderType:orderType||null,
      time: new Date().toISOString(),
    };
    await dbGetOrCreateConvo(waId, null, req.tenant.tenantId, 'wa');
    await dbSaveMessage(sentMessage, waId, req.tenant.tenantId);
    broadcastToTenant(req.tenant.tenantId, { type:'message_sent', waId, message:sentMessage });
    console.log(`🧾 Ticket (${orderType||'sin tipo'}) enviado a ${waId}`);
    res.json({ success:true, message:sentMessage, mediaId });
  } catch (err) {
    console.error('Error enviando ticket:', err.response?.data || err.message);
    res.status(500).json({ error:'Error al enviar ticket', details: err.response?.data || err.message });
  }
});

// ══════════════════════════════════════════════════════════════
// API — Enviar mensaje de texto por Instagram
// ══════════════════════════════════════════════════════════════

app.post('/api/send-instagram', requireAuth, async (req, res) => {
  const { igId, text } = req.body;
  if (!igId || !text) return res.status(400).json({ error: 'Faltan igId o text' });

  try {
    const creds = await getTenantCredentials(req.tenant.tenantId);
    if (!creds.instagram_access_token) return res.status(500).json({ error: 'Instagram no configurado' });

    const response = await axios.post(
      `https://graph.instagram.com/${GRAPH_API_VERSION}/me/messages`,
      { recipient:{ id: igId }, message:{ text } },
      { headers: { Authorization:`Bearer ${creds.instagram_access_token}`, 'Content-Type':'application/json' } }
    );

    const convoKey = `ig_${igId}`;
    await dbGetOrCreateConvo(convoKey, igId, req.tenant.tenantId, 'ig', igId);
    const sentMessage = {
      id:   response.data.message_id || 'ig_out_' + Date.now(),
      role: 'out', type: 'text', text, time: new Date().toISOString(),
    };
    await dbSaveMessage(sentMessage, convoKey, req.tenant.tenantId);
    broadcastToTenant(req.tenant.tenantId, { type:'message_sent', waId:convoKey, platform:'ig', message:sentMessage });
    res.json({ success:true, message:sentMessage });
  } catch (err) {
    console.error('Error enviando Instagram DM:', err.response?.data || err.message);
    res.status(500).json({ error:'Error al enviar por Instagram', details: err.response?.data || err.message });
  }
});

// ══════════════════════════════════════════════════════════════
// Deauth/delete callbacks requeridos por Meta
// ══════════════════════════════════════════════════════════════
app.get('/auth/instagram/deauth',  (req, res) => res.sendStatus(200));
app.post('/auth/instagram/deauth', (req, res) => res.sendStatus(200));
app.get('/auth/instagram/delete',  (req, res) => res.sendStatus(200));
app.post('/auth/instagram/delete', (req, res) => res.sendStatus(200));

// ══════════════════════════════════════════════════════════════
// Health check & privacy
// ══════════════════════════════════════════════════════════════
app.get('/', (req, res) => res.json({
  status:    'ok',
  service:   'CENTRAL Backend',
  version:   '5.0 — Multi-tenant + PostgreSQL + WhatsApp + Instagram',
  db_ready:  dbReady,
  whatsapp_configured:  !!DEFAULT_PHONE_ID,
  instagram_configured: !!DEFAULT_IG_TOKEN,
}));

app.get('/health', (req, res) => res.json({ status:'ok', db:dbReady, timestamp:new Date().toISOString() }));

app.get('/privacy', (req, res) => {
  res.send(`<!DOCTYPE html><html><head><meta charset="UTF-8"><title>Política de Privacidad — CENTRAL</title>
  <style>body{font-family:Arial,sans-serif;max-width:800px;margin:40px auto;padding:0 20px;line-height:1.6;}h1{color:#333;}h2{color:#555;margin-top:30px;}</style></head>
  <body>
  <h1>Política de Privacidad de CENTRAL</h1>
  <p><strong>Última actualización:</strong> ${new Date().toLocaleDateString('es-ES')}</p>
  <h2>1. Información que recopilamos</h2>
  <p>CENTRAL recopila mensajes de WhatsApp e Instagram Business exclusivamente para facilitar la gestión de comunicaciones comerciales.</p>
  <h2>2. Uso de la información</h2>
  <p>La información se usa únicamente dentro de la plataforma CENTRAL. No se comparte con terceros.</p>
  <h2>3. Almacenamiento</h2>
  <p>Los datos se almacenan de forma segura en servidores protegidos. Los usuarios pueden solicitar la eliminación en cualquier momento.</p>
  <h2>4. Contacto</h2>
  <p><a href="mailto:vyralvideos.creators@gmail.com">vyralvideos.creators@gmail.com</a></p>
  </body></html>`);
});

// ══════════════════════════════════════════════════════════════
// ARRANQUE
// ══════════════════════════════════════════════════════════════
initDB().then(() => {
  server.listen(PORT, () => console.log(`🚀 CENTRAL Backend v5.0 escuchando en puerto ${PORT}`));
});
