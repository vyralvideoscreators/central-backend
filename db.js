// ══════════════════════════════════════════════════════════════
// db.js — Persistencia en PostgreSQL (Railway) para conversaciones y mensajes
// Si la base de datos no está disponible, el resto del backend sigue
// funcionando: las lecturas devuelven vacío y las escrituras se
// registran como warning en los logs, pero nunca crashean el proceso.
// ══════════════════════════════════════════════════════════════
const { Pool } = require('pg');

const DATABASE_URL = process.env.DATABASE_URL;

// La mayoría de instancias de Railway Postgres no requieren SSL cuando el
// backend corre dentro del mismo proyecto (usan la URL interna). Si tu
// proveedor de Postgres sí lo exige, define PGSSL=true en las variables de entorno.
const useSSL = process.env.PGSSL === 'true';

const pool = DATABASE_URL
  ? new Pool({
      connectionString: DATABASE_URL,
      ssl: useSSL ? { rejectUnauthorized: false } : false,
    })
  : null;

let dbReady = false;

if (pool) {
  // Evita que un error de conexión perdido (ej. Postgres reiniciando) tumbe el proceso.
  pool.on('error', (err) => {
    console.error('⚠️  Error inesperado en el pool de PostgreSQL:', err.message);
  });
}

async function initDb() {
  if (!pool) {
    console.warn('⚠️  DATABASE_URL no configurada — las conversaciones NO se guardarán de forma persistente.');
    return;
  }

  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS conversations (
        wa_id TEXT PRIMARY KEY,
        name TEXT,
        platform TEXT DEFAULT 'wa',
        ig_sender_id TEXT,
        last_message_at TIMESTAMP
      )
    `);

    await pool.query(`
      CREATE TABLE IF NOT EXISTS messages (
        id TEXT PRIMARY KEY,
        wa_id TEXT REFERENCES conversations(wa_id),
        role TEXT,
        type TEXT,
        text TEXT,
        media_id TEXT,
        mime_type TEXT,
        media_url TEXT,
        caption TEXT,
        filename TEXT,
        order_type TEXT,
        time TIMESTAMP,
        created_at TIMESTAMP DEFAULT NOW()
      )
    `);

    await pool.query(`CREATE INDEX IF NOT EXISTS idx_messages_wa_id ON messages(wa_id)`);

    dbReady = true;
    console.log('🗄️  PostgreSQL conectado — conversaciones y mensajes se guardan de forma persistente.');
  } catch (err) {
    dbReady = false;
    console.warn('⚠️  No se pudo conectar/inicializar PostgreSQL. El backend seguirá funcionando SIN persistencia.');
    console.warn('⚠️  Detalle:', err.message);
  }
}

function isReady() {
  return dbReady;
}

function rowToConvo(row) {
  return {
    waId: row.wa_id,
    name: row.name,
    platform: row.platform || 'wa',
    igSenderId: row.ig_sender_id || null,
    lastMessageAt: row.last_message_at,
    messages: [],
  };
}

function rowToMessage(row) {
  return {
    id: row.id,
    role: row.role,
    type: row.type,
    text: row.text || undefined,
    mediaId: row.media_id || undefined,
    mimeType: row.mime_type || undefined,
    mediaUrl: row.media_url || undefined,
    caption: row.caption || undefined,
    filename: row.filename || undefined,
    orderType: row.order_type || undefined,
    time: row.time,
  };
}

// Crea la conversación si no existe. Si ya existe, solo actualiza el nombre
// cuando updateName=true (usado por los webhooks, que traen el nombre real
// y actualizado del contacto) — así una llamada desde /api/send no pisa
// el nombre ya guardado con el waId/igSenderId "en crudo".
async function getOrCreateConvo(waId, name, extra = {}) {
  const { platform = 'wa', igSenderId = null, updateName = false } = extra;
  const insertName = name || waId;
  const fallback = { waId, name: insertName, platform, igSenderId, messages: [], lastMessageAt: null };

  if (!dbReady) return fallback;

  try {
    const result = await pool.query(
      `INSERT INTO conversations (wa_id, name, platform, ig_sender_id)
       VALUES ($1, $2, $3, $4)
       ON CONFLICT (wa_id) DO UPDATE SET
         name = CASE WHEN $5 THEN EXCLUDED.name ELSE conversations.name END
       RETURNING *`,
      [waId, insertName, platform, igSenderId, updateName]
    );
    return rowToConvo(result.rows[0]);
  } catch (err) {
    console.error('⚠️  Error guardando conversación en PostgreSQL:', err.message);
    return fallback;
  }
}

// Guarda un mensaje y actualiza last_message_at de su conversación.
async function saveMessage(waId, message) {
  if (!dbReady) return;

  try {
    await pool.query(
      `INSERT INTO messages (id, wa_id, role, type, text, media_id, mime_type, media_url, caption, filename, order_type, time)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
       ON CONFLICT (id) DO NOTHING`,
      [
        message.id,
        waId,
        message.role || null,
        message.type || null,
        message.text || null,
        message.mediaId || null,
        message.mimeType || null,
        message.mediaUrl || null,
        message.caption || null,
        message.filename || null,
        message.orderType || null,
        message.time || null,
      ]
    );

    await pool.query(`UPDATE conversations SET last_message_at = $1 WHERE wa_id = $2`, [message.time, waId]);
  } catch (err) {
    console.error('⚠️  Error guardando mensaje en PostgreSQL:', err.message);
  }
}

async function getAllConversations() {
  if (!dbReady) return [];

  try {
    const convosResult = await pool.query('SELECT * FROM conversations ORDER BY last_message_at DESC NULLS LAST');
    const messagesResult = await pool.query('SELECT * FROM messages ORDER BY time ASC');

    const messagesByWaId = {};
    for (const row of messagesResult.rows) {
      if (!messagesByWaId[row.wa_id]) messagesByWaId[row.wa_id] = [];
      messagesByWaId[row.wa_id].push(rowToMessage(row));
    }

    return convosResult.rows.map((row) => {
      const convo = rowToConvo(row);
      convo.messages = messagesByWaId[row.wa_id] || [];
      return convo;
    });
  } catch (err) {
    console.error('⚠️  Error leyendo conversaciones de PostgreSQL:', err.message);
    return [];
  }
}

async function getConversation(waId) {
  if (!dbReady) return null;

  try {
    const convoResult = await pool.query('SELECT * FROM conversations WHERE wa_id = $1', [waId]);
    if (convoResult.rows.length === 0) return null;

    const messagesResult = await pool.query('SELECT * FROM messages WHERE wa_id = $1 ORDER BY time ASC', [waId]);

    const convo = rowToConvo(convoResult.rows[0]);
    convo.messages = messagesResult.rows.map(rowToMessage);
    return convo;
  } catch (err) {
    console.error('⚠️  Error leyendo conversación de PostgreSQL:', err.message);
    return null;
  }
}

async function countConversations() {
  if (!dbReady) return 0;
  try {
    const result = await pool.query('SELECT COUNT(*)::int AS count FROM conversations');
    return result.rows[0].count;
  } catch (err) {
    console.error('⚠️  Error contando conversaciones en PostgreSQL:', err.message);
    return 0;
  }
}

module.exports = {
  initDb,
  isReady,
  getOrCreateConvo,
  saveMessage,
  getAllConversations,
  getConversation,
  countConversations,
};
