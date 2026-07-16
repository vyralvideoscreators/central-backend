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
const Stripe     = require('stripe');
const crypto     = require('crypto');
const nodemailer = require('nodemailer');

const app = express();
app.use(cors());
// El webhook de Stripe necesita el body sin procesar para validar la firma
// (ver /api/billing/webhook) — se excluye acá del parser JSON global.
app.use((req, res, next) => {
  if (req.originalUrl === '/api/billing/webhook') return next();
  express.json({ limit: '20mb' })(req, res, next);
});

const PORT             = process.env.PORT || 3000;
const GRAPH_API_VERSION = 'v21.0';
const GRAPH_BASE       = `https://graph.facebook.com/${GRAPH_API_VERSION}`;
const JWT_SECRET       = process.env.JWT_SECRET || 'central_jwt_dev_secret_cambiar_en_prod';

const stripe           = process.env.STRIPE_SECRET_KEY ? Stripe(process.env.STRIPE_SECRET_KEY, { apiVersion: '2024-06-20' }) : null;
const STRIPE_WEBHOOK_SECRET = process.env.STRIPE_WEBHOOK_SECRET || '';
const STRIPE_PRICE_PRO      = process.env.STRIPE_PRICE_PRO || '';
const STRIPE_PRICE_BUSINESS = process.env.STRIPE_PRICE_BUSINESS || '';
const APP_URL               = process.env.APP_URL || 'https://central-backend-production.up.railway.app';

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
        full_name                 TEXT,
        email                     TEXT UNIQUE NOT NULL,
        password_hash             TEXT NOT NULL,
        plan                      TEXT DEFAULT 'basic',
        country                   TEXT,
        whatsapp_number           TEXT,
        stripe_customer_id        TEXT,
        stripe_subscription_id    TEXT,
        plan_expires_at           TIMESTAMP,
        billing_status            TEXT DEFAULT 'free',
        whatsapp_phone_number_id  TEXT,
        whatsapp_token            TEXT,
        instagram_access_token    TEXT,
        instagram_account_id      TEXT,
        webhook_verify_token      TEXT DEFAULT 'central_webhook_secreto_123',
        reset_token               TEXT,
        reset_token_expires       TIMESTAMP,
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
    ON CONFLICT (email) DO NOTHING
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
  if (!dbReady || !tenantId) return;
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
  const { businessName, fullName, email, password, country, whatsappNumber, plan } = req.body;
  if (!businessName || !email || !password) {
    return res.status(400).json({ error: 'Faltan campos obligatorios', field: 'businessName' });
  }
  if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
    return res.status(400).json({ error: 'Correo electrónico inválido', field: 'email' });
  }
  if (password.length < 8) {
    return res.status(400).json({ error: 'La contraseña debe tener al menos 8 caracteres', field: 'password' });
  }
  if (!dbReady) return res.status(503).json({ error: 'Base de datos no disponible' });

  try {
    const hash = await bcrypt.hash(password, 10);
    const { rows } = await pool.query(`
      INSERT INTO tenants (business_name, full_name, email, password_hash, plan, country, whatsapp_number)
      VALUES ($1, $2, $3, $4, $5, $6, $7)
      RETURNING id, business_name, full_name, email, plan, country, created_at
    `, [businessName, fullName || null, email, hash, plan || 'basic', country || null, whatsappNumber || null]);
    const tenant = rows[0];
    const token  = jwt.sign({ tenantId: tenant.id, email: tenant.email }, JWT_SECRET, { expiresIn: '30d' });
    res.json({
      success: true,
      token,
      tenant: { id: tenant.id, businessName: tenant.business_name, fullName: tenant.full_name, email: tenant.email, plan: tenant.plan, country: tenant.country },
    });
  } catch (err) {
    if (err.code === '23505') return res.status(409).json({ error: 'Este correo ya está registrado', field: 'email' });
    res.status(500).json({ error: 'Error al registrar', details: err.message });
  }
});

app.post('/api/tenant/login', async (req, res) => {
  const { email, password } = req.body;
  if (!email || !password) return res.status(400).json({ error: 'Completa todos los campos' });
  if (!dbReady) return res.status(503).json({ error: 'Base de datos no disponible' });

  try {
    const { rows } = await pool.query('SELECT * FROM tenants WHERE email = $1', [email]);
    const tenant = rows[0];
    if (!tenant) return res.status(401).json({ error: 'Correo o contraseña incorrectos' });
    if (!tenant.active) return res.status(403).json({ error: 'Tu cuenta ha sido suspendida. Contacta soporte.' });
    const valid = await bcrypt.compare(password, tenant.password_hash);
    if (!valid)  return res.status(401).json({ error: 'Correo o contraseña incorrectos' });
    const token = jwt.sign({ tenantId: tenant.id, email: tenant.email }, JWT_SECRET, { expiresIn: '30d' });
    res.json({
      success: true,
      token,
      tenant: { id: tenant.id, businessName: tenant.business_name, fullName: tenant.full_name, email: tenant.email, plan: tenant.plan, country: tenant.country },
    });
  } catch (err) {
    res.status(500).json({ error: 'Error al iniciar sesión', details: err.message });
  }
});

// ── Email transporter ──
const emailTransporter = process.env.SMTP_USER ? nodemailer.createTransport({
  host:   process.env.SMTP_HOST || 'smtp.gmail.com',
  port:   parseInt(process.env.SMTP_PORT || '587'),
  secure: false,
  auth: { user: process.env.SMTP_USER, pass: process.env.SMTP_PASS },
}) : null;

// ── Forgot password ──
app.post('/api/tenant/forgot-password', async (req, res) => {
  const { email } = req.body;
  if (!email) return res.status(400).json({ error: 'Correo requerido' });
  if (!dbReady) return res.status(503).json({ error: 'Base de datos no disponible' });
  try {
    const { rows } = await pool.query('SELECT id FROM tenants WHERE email = $1 AND active = true', [email]);
    // Siempre responder OK para no revelar si el correo existe
    if (rows[0]) {
      const token   = crypto.randomBytes(32).toString('hex');
      const expires = new Date(Date.now() + 60 * 60 * 1000); // 1 hora
      await pool.query('UPDATE tenants SET reset_token=$1, reset_token_expires=$2 WHERE id=$3', [token, expires, rows[0].id]);
      if (emailTransporter) {
        const resetUrl = `${process.env.APP_URL || 'https://central-backend-production.up.railway.app'}/reset-password.html?token=${token}`;
        await emailTransporter.sendMail({
          from: `"CENTRAL" <${process.env.SMTP_USER}>`,
          to:   email,
          subject: 'Restablecer contraseña — CENTRAL',
          html: `
            <div style="font-family:sans-serif;max-width:480px;margin:0 auto;padding:32px;background:#0d0d14;color:#e8e8f0;border-radius:16px;">
              <h2 style="color:#a78bfa;margin-bottom:8px;">Restablecer contraseña</h2>
              <p style="color:#9999bb;margin-bottom:24px;">Haz clic en el botón para crear una nueva contraseña. El enlace expira en 1 hora.</p>
              <a href="${resetUrl}" style="display:inline-block;padding:12px 28px;background:linear-gradient(135deg,#7c5cfc,#a78bfa);color:#fff;border-radius:10px;text-decoration:none;font-weight:700;">Restablecer contraseña</a>
              <p style="color:#5555aa;font-size:12px;margin-top:24px;">Si no solicitaste esto, ignora este correo.</p>
            </div>`,
        });
      } else {
        console.log(`[RESET] Token para ${email}: ${token}`);
      }
    }
    res.json({ success: true });
  } catch (err) {
    console.error('forgot-password error:', err);
    res.status(500).json({ error: 'Error al procesar solicitud' });
  }
});

// ── Reset password ──
app.post('/api/tenant/reset-password', async (req, res) => {
  const { token, password } = req.body;
  if (!token || !password) return res.status(400).json({ error: 'Datos incompletos' });
  if (password.length < 8)  return res.status(400).json({ error: 'La contraseña debe tener al menos 8 caracteres' });
  if (!dbReady) return res.status(503).json({ error: 'Base de datos no disponible' });
  try {
    const { rows } = await pool.query(
      'SELECT id FROM tenants WHERE reset_token=$1 AND reset_token_expires > NOW() AND active=true',
      [token]
    );
    if (!rows[0]) return res.status(400).json({ error: 'El enlace no es válido o ha expirado' });
    const hash = await bcrypt.hash(password, 12);
    await pool.query('UPDATE tenants SET password_hash=$1, reset_token=NULL, reset_token_expires=NULL WHERE id=$2', [hash, rows[0].id]);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: 'Error al restablecer contraseña' });
  }
});

app.get('/api/tenant/profile', requireAuth, async (req, res) => {
  try {
    const { rows } = await pool.query(
      'SELECT id, business_name, full_name, email, plan, country, whatsapp_number, whatsapp_phone_number_id, instagram_account_id, created_at FROM tenants WHERE id = $1',
      [req.tenant.tenantId]
    );
    if (!rows[0]) return res.status(404).json({ error: 'Tenant no encontrado' });
    const t = rows[0];
    const phoneId = t.whatsapp_phone_number_id;
    res.json({
      id:                      t.id,
      business_name:           t.business_name,
      full_name:               t.full_name,
      email:                   t.email,
      plan:                    t.plan,
      country:                 t.country,
      whatsapp_number:         t.whatsapp_number,
      created_at:              t.created_at,
      instagram_account_id:    t.instagram_account_id,
      whatsapp_connected:      !!phoneId,
      whatsapp_phone_number_id: phoneId
        ? '*'.repeat(Math.max(0, phoneId.length - 4)) + phoneId.slice(-4)
        : null,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.put('/api/tenant/whatsapp', requireAuth, async (req, res) => {
  const { whatsapp_phone_number_id, whatsapp_token } = req.body;
  if (!whatsapp_phone_number_id || !whatsapp_token) {
    return res.status(400).json({ error: 'Phone Number ID y Token son obligatorios' });
  }
  if (!dbReady) return res.status(503).json({ error: 'Base de datos no disponible' });
  try {
    await pool.query(
      'UPDATE tenants SET whatsapp_phone_number_id = $1, whatsapp_token = $2 WHERE id = $3',
      [whatsapp_phone_number_id.trim(), whatsapp_token.trim(), req.tenant.tenantId]
    );
    res.json({ success: true, message: 'WhatsApp conectado correctamente' });
  } catch (err) {
    res.status(500).json({ error: 'Error al guardar las credenciales' });
  }
});

app.get('/api/tenant/test-whatsapp', requireAuth, async (req, res) => {
  if (!dbReady) return res.status(503).json({ success: false, error: 'Base de datos no disponible' });
  try {
    const { rows } = await pool.query(
      'SELECT whatsapp_phone_number_id, whatsapp_token FROM tenants WHERE id = $1',
      [req.tenant.tenantId]
    );
    const t = rows[0];
    if (!t?.whatsapp_phone_number_id || !t?.whatsapp_token) {
      return res.json({ success: false, error: 'No hay credenciales de WhatsApp guardadas' });
    }
    const metaRes = await axios.get(
      `${GRAPH_BASE}/${t.whatsapp_phone_number_id}`,
      { headers: { Authorization: `Bearer ${t.whatsapp_token}` } }
    );
    const d = metaRes.data;
    res.json({
      success:       true,
      phone_number:  d.display_phone_number || d.phone_number || '—',
      verified_name: d.verified_name || d.name || '—',
    });
  } catch (err) {
    const msg = err.response?.data?.error?.message || 'Credenciales inválidas o token expirado';
    res.json({ success: false, error: msg });
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
// Servir archivos estáticos (HTML, CSS, JS del frontend)
app.use(express.static(__dirname));

// ══════════════════════════════════════════════════════════════
// Health check & rutas raíz
// ══════════════════════════════════════════════════════════════
app.get('/', (req, res) => res.redirect('/central-registro.html'));
app.get('/health', (req, res) => res.json({ status:'ok', db:dbReady, timestamp:new Date().toISOString() }));
app.get('/status', (req, res) => res.json({
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
// BILLING — Stripe
// ══════════════════════════════════════════════════════════════

// Webhook necesita raw body — registrar ANTES de express.json()
// (express.json ya está arriba, así que usamos express.raw solo para este path)
app.post('/api/billing/webhook', express.raw({ type: 'application/json' }), async (req, res) => {
  if (!stripe) return res.status(503).json({ error: 'Stripe no configurado' });
  const sig = req.headers['stripe-signature'];
  let event;
  try {
    event = stripe.webhooks.constructEvent(req.body, sig, STRIPE_WEBHOOK_SECRET);
  } catch (err) {
    console.error('⚠️  Stripe webhook firma inválida:', err.message);
    return res.status(400).send('Webhook signature invalid');
  }

  try {
    if (event.type === 'checkout.session.completed') {
      const session    = event.data.object;
      const tenantId   = session.metadata?.tenantId;
      const plan       = session.metadata?.plan;
      const customerId = session.customer;
      const subId      = session.subscription;
      if (tenantId && plan) {
        await pool.query(
          'UPDATE tenants SET plan=$1, stripe_customer_id=$2, stripe_subscription_id=$3, billing_status=$4 WHERE id=$5',
          [plan, customerId, subId, 'active', tenantId]
        );
        console.log(`✅ Stripe: tenant ${tenantId} activó plan ${plan}`);
      }
    }

    if (event.type === 'invoice.payment_succeeded') {
      const invoice = event.data.object;
      const subId   = invoice.subscription;
      if (subId) {
        const periodEnd = new Date(invoice.lines?.data?.[0]?.period?.end * 1000 || Date.now());
        await pool.query(
          'UPDATE tenants SET billing_status=$1, plan_expires_at=$2 WHERE stripe_subscription_id=$3',
          ['active', periodEnd, subId]
        );
        console.log(`✅ Stripe: pago exitoso para sub ${subId}`);
      }
    }

    if (event.type === 'invoice.payment_failed') {
      const invoice = event.data.object;
      const subId   = invoice.subscription;
      if (subId) {
        await pool.query(
          'UPDATE tenants SET billing_status=$1 WHERE stripe_subscription_id=$2',
          ['past_due', subId]
        );
        console.warn(`⚠️  Stripe: pago fallido para sub ${subId}`);
      }
    }

    if (event.type === 'customer.subscription.updated') {
      const sub = event.data.object;
      // Si el usuario reactivó desde el portal, sincronizar estado
      const billing_status = sub.status === 'active' ? 'active' : sub.status;
      const plan_map = {}; // plan se determina por metadata o price, no lo cambiamos aquí
      if (sub.cancel_at_period_end) {
        // Cancelación programada — sigue activo hasta fin de período
        await pool.query(
          'UPDATE tenants SET billing_status=$1 WHERE stripe_subscription_id=$2',
          ['cancelled', sub.id]
        );
      } else if (sub.status === 'active') {
        await pool.query(
          'UPDATE tenants SET billing_status=$1 WHERE stripe_subscription_id=$2',
          ['active', sub.id]
        );
      }
    }

    if (event.type === 'customer.subscription.deleted') {
      const sub = event.data.object;
      await pool.query(
        'UPDATE tenants SET plan=$1, billing_status=$2, stripe_subscription_id=$3 WHERE stripe_subscription_id=$4',
        ['basic', 'cancelled', null, sub.id]
      );
      console.log(`⚠️  Stripe: suscripción ${sub.id} cancelada — tenant bajado a basic`);
    }
  } catch (err) {
    console.error('Error procesando evento Stripe:', err.message);
  }

  res.json({ received: true });
});

app.post('/api/billing/create-checkout', requireAuth, async (req, res) => {
  if (!stripe) return res.status(503).json({ error: 'Stripe no configurado' });
  const { plan } = req.body;
  if (!['pro','business'].includes(plan)) return res.status(400).json({ error: 'Plan inválido' });
  const priceId = plan === 'pro' ? STRIPE_PRICE_PRO : STRIPE_PRICE_BUSINESS;
  if (!priceId) return res.status(503).json({ error: `STRIPE_PRICE_${plan.toUpperCase()} no configurado` });

  try {
    const { rows } = await pool.query('SELECT * FROM tenants WHERE id=$1', [req.tenant.tenantId]);
    const tenant   = rows[0];
    if (!tenant) return res.status(404).json({ error: 'Tenant no encontrado' });

    let customerId = tenant.stripe_customer_id;
    if (!customerId) {
      const customer = await stripe.customers.create({ email: tenant.email, name: tenant.business_name, metadata: { tenantId: tenant.id } });
      customerId = customer.id;
      await pool.query('UPDATE tenants SET stripe_customer_id=$1 WHERE id=$2', [customerId, tenant.id]);
    }

    const session = await stripe.checkout.sessions.create({
      customer:            customerId,
      payment_method_types: ['card'],
      mode:                'subscription',
      line_items:          [{ price: priceId, quantity: 1 }],
      success_url:         `${APP_URL.replace(/\/$/, '')}/central-mvp-v63%20(1)%20-%20copia.html?payment=success`,
      cancel_url:          `${APP_URL.replace(/\/$/, '')}/central-mvp-v63%20(1)%20-%20copia.html?payment=cancelled`,
      metadata:            { tenantId: tenant.id, plan },
    });

    res.json({ checkoutUrl: session.url });
  } catch (err) {
    console.error('❌ Stripe checkout error:', err.message, err.type, err.code);
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/billing/status', requireAuth, async (req, res) => {
  try {
    const { rows } = await pool.query(
      'SELECT plan, billing_status, plan_expires_at, stripe_subscription_id FROM tenants WHERE id=$1',
      [req.tenant.tenantId]
    );
    const t = rows[0];
    if (!t) return res.status(404).json({ error: 'Tenant no encontrado' });

    let nextBillingDate = null;
    if (stripe && t.stripe_subscription_id) {
      try {
        const sub = await stripe.subscriptions.retrieve(t.stripe_subscription_id);
        nextBillingDate = new Date(sub.current_period_end * 1000).toISOString();
      } catch {}
    }

    res.json({
      plan:           t.plan,
      billingStatus:  t.billing_status || 'free',
      planExpiresAt:  t.plan_expires_at,
      nextBillingDate,
      hasSubscription: !!t.stripe_subscription_id,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/billing/cancel', requireAuth, async (req, res) => {
  if (!stripe) return res.status(503).json({ error: 'Stripe no configurado' });
  try {
    const { rows } = await pool.query('SELECT stripe_subscription_id FROM tenants WHERE id=$1', [req.tenant.tenantId]);
    const subId = rows[0]?.stripe_subscription_id;
    if (!subId) return res.status(400).json({ error: 'No tienes una suscripción activa' });

    const sub = await stripe.subscriptions.update(subId, { cancel_at_period_end: true });
    const cancelDate = new Date(sub.current_period_end * 1000).toLocaleDateString('es-ES', { day:'2-digit', month:'long', year:'numeric' });
    // billing_status se actualiza vía webhook customer.subscription.updated, no aquí
    // para que refleje el estado real de Stripe
    res.json({ success: true, message: `Tu suscripción se cancelará el ${cancelDate}` });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/billing/portal', requireAuth, async (req, res) => {
  if (!stripe) return res.status(503).json({ error: 'Stripe no configurado' });
  try {
    const { rows } = await pool.query('SELECT stripe_customer_id FROM tenants WHERE id=$1', [req.tenant.tenantId]);
    const customerId = rows[0]?.stripe_customer_id;
    if (!customerId) return res.status(400).json({ error: 'No tienes una cuenta de facturación aún' });

    const session = await stripe.billingPortal.sessions.create({
      customer:   customerId,
      return_url: `${APP_URL}/central-mvp-v63%20(1)%20-%20copia.html`,
    });
    res.json({ portalUrl: session.url });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ══════════════════════════════════════════════════════════════
// ADMIN — middleware y endpoints de superadmin
// ══════════════════════════════════════════════════════════════

const ADMIN_EMAIL    = process.env.ADMIN_EMAIL;
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD;

function requireAdmin(req, res, next) {
  const auth = req.headers.authorization;
  if (!auth || !auth.startsWith('Bearer ')) {
    return res.status(401).json({ error: 'No autenticado' });
  }
  try {
    const decoded = jwt.verify(auth.slice(7), JWT_SECRET);
    if (decoded.role !== 'superadmin') return res.status(403).json({ error: 'Acceso denegado' });
    req.admin = decoded;
    next();
  } catch {
    res.status(401).json({ error: 'Token inválido o expirado' });
  }
}

app.post('/api/admin/login', async (req, res) => {
  const { email, password } = req.body;
  if (!ADMIN_EMAIL || !ADMIN_PASSWORD) {
    return res.status(503).json({ error: 'Panel de admin no configurado — define ADMIN_EMAIL y ADMIN_PASSWORD en Railway' });
  }
  if (!email || !password) return res.status(400).json({ error: 'Completa todos los campos' });
  if (email !== ADMIN_EMAIL || password !== ADMIN_PASSWORD) {
    return res.status(401).json({ error: 'Credenciales incorrectas' });
  }
  const token = jwt.sign({ role: 'superadmin', email }, JWT_SECRET, { expiresIn: '8h' });
  res.json({ success: true, token });
});

app.get('/api/admin/stats', requireAdmin, async (req, res) => {
  if (!dbReady) return res.status(503).json({ error: 'Base de datos no disponible' });
  try {
    const [totals, byPlan, newThisWeek, convos, msgs] = await Promise.all([
      pool.query(`SELECT COUNT(*) total, COUNT(*) FILTER (WHERE active) activos, COUNT(*) FILTER (WHERE NOT active) suspendidos FROM tenants`),
      pool.query(`SELECT plan, COUNT(*) count FROM tenants GROUP BY plan`),
      pool.query(`SELECT COUNT(*) count FROM tenants WHERE created_at >= NOW() - INTERVAL '7 days'`),
      pool.query(`SELECT COUNT(*) count FROM conversations`),
      pool.query(`SELECT COUNT(*) count FROM messages`),
    ]);
    const planMap = {};
    byPlan.rows.forEach(r => { planMap[r.plan] = parseInt(r.count); });
    res.json({
      total:         parseInt(totals.rows[0].total),
      activos:       parseInt(totals.rows[0].activos),
      suspendidos:   parseInt(totals.rows[0].suspendidos),
      porPlan:       planMap,
      nuevosEstaSemana: parseInt(newThisWeek.rows[0].count),
      totalConversaciones: parseInt(convos.rows[0].count),
      totalMensajes:       parseInt(msgs.rows[0].count),
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/admin/tenants', requireAdmin, async (req, res) => {
  if (!dbReady) return res.status(503).json({ error: 'Base de datos no disponible' });
  const { search = '', plan = '', status = '', page = 1, limit = 20 } = req.query;
  const offset = (parseInt(page) - 1) * parseInt(limit);
  try {
    const conditions = [];
    const params     = [];
    if (search) { params.push(`%${search}%`); conditions.push(`(business_name ILIKE $${params.length} OR email ILIKE $${params.length})`); }
    if (plan)   { params.push(plan);   conditions.push(`plan = $${params.length}`); }
    if (status === 'active')    conditions.push(`active = true`);
    if (status === 'suspended') conditions.push(`active = false`);
    const where = conditions.length ? 'WHERE ' + conditions.join(' AND ') : '';

    const countRes = await pool.query(`SELECT COUNT(*) count FROM tenants ${where}`, params);
    params.push(parseInt(limit), offset);
    const dataRes  = await pool.query(`
      SELECT
        t.id, t.business_name, t.full_name, t.email, t.plan, t.country,
        t.whatsapp_number, t.active, t.created_at,
        CASE WHEN t.whatsapp_phone_number_id IS NOT NULL THEN true ELSE false END AS whatsapp_connected,
        CASE WHEN t.whatsapp_phone_number_id IS NOT NULL
          THEN repeat('*', GREATEST(0, length(t.whatsapp_phone_number_id)-4)) || right(t.whatsapp_phone_number_id,4)
          ELSE NULL END AS whatsapp_phone_id_masked,
        (SELECT COUNT(*) FROM conversations c WHERE c.tenant_id = t.id) AS total_convos,
        (SELECT COUNT(*) FROM messages m WHERE m.tenant_id = t.id) AS total_msgs
      FROM tenants t
      ${where}
      ORDER BY t.created_at DESC
      LIMIT $${params.length - 1} OFFSET $${params.length}
    `, params);

    res.json({
      total: parseInt(countRes.rows[0].count),
      page:  parseInt(page),
      limit: parseInt(limit),
      tenants: dataRes.rows,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/admin/tenants/:tenantId', requireAdmin, async (req, res) => {
  if (!dbReady) return res.status(503).json({ error: 'Base de datos no disponible' });
  try {
    const { rows } = await pool.query(`
      SELECT
        t.id, t.business_name, t.full_name, t.email, t.plan, t.country,
        t.whatsapp_number, t.active, t.created_at, t.instagram_account_id,
        CASE WHEN t.whatsapp_phone_number_id IS NOT NULL THEN true ELSE false END AS whatsapp_connected,
        CASE WHEN t.whatsapp_phone_number_id IS NOT NULL
          THEN repeat('*', GREATEST(0, length(t.whatsapp_phone_number_id)-4)) || right(t.whatsapp_phone_number_id,4)
          ELSE NULL END AS whatsapp_phone_id_masked,
        (SELECT COUNT(*) FROM conversations c WHERE c.tenant_id = t.id) AS total_convos,
        (SELECT COUNT(*) FROM messages m WHERE m.tenant_id = t.id) AS total_msgs
      FROM tenants t WHERE t.id = $1
    `, [req.params.tenantId]);
    if (!rows[0]) return res.status(404).json({ error: 'Tenant no encontrado' });
    res.json(rows[0]);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.put('/api/admin/tenants/:tenantId/plan', requireAdmin, async (req, res) => {
  const { plan } = req.body;
  if (!['basic','pro','business'].includes(plan)) return res.status(400).json({ error: 'Plan inválido' });
  if (!dbReady) return res.status(503).json({ error: 'Base de datos no disponible' });
  try {
    await pool.query('UPDATE tenants SET plan = $1 WHERE id = $2', [plan, req.params.tenantId]);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.put('/api/admin/tenants/:tenantId/status', requireAdmin, async (req, res) => {
  const { active } = req.body;
  if (typeof active !== 'boolean') return res.status(400).json({ error: 'active debe ser true o false' });
  if (!dbReady) return res.status(503).json({ error: 'Base de datos no disponible' });
  try {
    await pool.query('UPDATE tenants SET active = $1 WHERE id = $2', [active, req.params.tenantId]);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.delete('/api/admin/tenants/:tenantId', requireAdmin, async (req, res) => {
  if (req.headers['x-confirm-delete'] !== 'true') {
    return res.status(400).json({ error: 'Falta header X-Confirm-Delete: true' });
  }
  if (!dbReady) return res.status(503).json({ error: 'Base de datos no disponible' });
  try {
    await pool.query('DELETE FROM tenants WHERE id = $1', [req.params.tenantId]);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ══════════════════════════════════════════════════════════════
// ARRANQUE
// ══════════════════════════════════════════════════════════════
initDB().then(() => {
  server.listen(PORT, () => console.log(`🚀 CENTRAL Backend v5.0 escuchando en puerto ${PORT}`));
});
