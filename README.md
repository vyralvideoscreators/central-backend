# CENTRAL Backend — Conexión con WhatsApp Business API

Backend que conecta la WhatsApp Business API de Meta con la webapp CENTRAL.

## ¿Qué hace?

- Recibe mensajes entrantes de WhatsApp via webhook
- Permite enviar mensajes desde CENTRAL hacia los clientes
- Mantiene las conversaciones en memoria (para producción real, cambiar a una base de datos)
- Notifica a CENTRAL en tiempo real via WebSocket cuando llega un mensaje nuevo

---

## Paso 1 — Subir a Railway

1. Ve a railway.app y crea una cuenta
2. Clic en "New Project" → "Deploy from GitHub repo"
3. Selecciona el repositorio central-backend
4. Ve a la pestaña "Variables" y agrega estas 3 variables:

   WHATSAPP_PHONE_NUMBER_ID = tu Phone Number ID
   WHATSAPP_TOKEN = tu Token de acceso
   WEBHOOK_VERIFY_TOKEN = central_webhook_secreto_123

5. Railway despliega automáticamente y te da una URL pública

---

## Paso 2 — Configurar el Webhook en Meta

1. Ve a developers.facebook.com, tu app → WhatsApp → Configuración → Webhook
2. URL de devolución de llamada: https://tu-url-de-railway.up.railway.app/webhook
3. Token de verificación: el mismo que pusiste en WEBHOOK_VERIFY_TOKEN
4. Verificar y guardar
5. Suscribirse al campo "messages"

---

## Paso 3 — Probar

1. Manda un mensaje de WhatsApp al número configurado
2. Revisa los logs de Railway
3. Visita https://tu-url.up.railway.app/api/conversations para ver el JSON

---

## Notas importantes

- El token temporal dura 24 horas. Para producción, generar un token permanente con un System User en el Business Manager.
- Este backend usa memoria, no base de datos. Si Railway reinicia, se pierde el historial.
- Para producción real, agregar validación de firma del webhook con X-Hub-Signature-256.
