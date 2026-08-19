const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const QRCode = require('qrcode');
const mongoose = require('mongoose');
const {
  default: makeWASocket,
  useMultiFileAuthState,
  DisconnectReason
} = require('@whiskeysockets/baileys');

const app = express();
const server = http.createServer(app);

app.use(cors());
app.use(express.json());

// KONEKSI MONGO DB
const MONGO_URI = process.env.MONGO_URI || "mongodb+srv://admin:rahasia123@cluster0.abcde.mongodb.net/wagateway?retryWrites=true&w=majority";

mongoose.connect(MONGO_URI)
  .then(() => console.log('MongoDB Connected Successfully'))
  .catch(err => console.error('MongoDB Connection Error:', err));

// Schema Model Pesan
// 'chatJid' menyimpan nomor lawan bicara (selalu nomor teman chat, bukan nomor kita)
const MessageSchema = new mongoose.Schema({
  chatJid: String,   // Contoh: '628123456789'
  text: String,
  fromMe: Boolean,
  timestamp: { type: Date, default: Date.now }
});
const Message = mongoose.model('Message', MessageSchema);

const io = new Server(server, {
  cors: { origin: "*", methods: ["GET", "POST"] }
});

let sock;

function cleanNumber(jidOrNumber) {
  if (!jidOrNumber) return '';
  let cleaned = jidOrNumber.replace('@s.whatsapp.net', '').replace('@g.us', '').replace(/[^0-9]/g, '');
  if (cleaned.startsWith('0')) cleaned = '62' + cleaned.slice(1);
  return cleaned;
}

async function connectToWhatsApp() {
  const { state, saveCreds } = await useMultiFileAuthState('auth_info_baileys');
  sock = makeWASocket({ 
    auth: state, 
    printQRInTerminal: true,
    browser: ["WA Web Lite", "Chrome", "1.0.0"]
  });

  sock.ev.on('creds.update', saveCreds);

  sock.ev.on('connection.update', async (update) => {
    const { connection, lastDisconnect, qr } = update;

    if (qr) {
      const qrBase64 = await QRCode.toDataURL(qr);
      io.emit('qr', qrBase64);
      io.emit('status', 'Silakan Scan QR Code');
    }

    if (connection === 'open') {
      io.emit('status', 'Terhubung');
      io.emit('qr', null);
    }

    if (connection === 'close') {
      const reason = lastDisconnect?.error?.output?.statusCode;
      const shouldReconnect = reason !== DisconnectReason.loggedOut;
      io.emit('status', 'Terputus. Menghubungkan ulang...');
      if (shouldReconnect) connectToWhatsApp();
    }
  });

  // Mendengarkan dan menyimpan pesan masuk/keluar dari WhatsApp
  sock.ev.on('messages.upsert', async (m) => {
    const msg = m.messages[0];
    if (!msg || !msg.key || !msg.message) return;

    // Abaikan pesan status/story
    if (msg.key.remoteJid === 'status@broadcast') return;

    const remoteJid = msg.key.remoteJid || '';
    const chatJid = cleanNumber(remoteJid);
    const fromMe = msg.key.fromMe || false;

    // Ambil teks dari berbagai jenis pesan Teks / Balasan / Web
    const text = 
      msg.message?.conversation || 
      msg.message?.extendedTextMessage?.text || 
      msg.message?.imageMessage?.caption || 
      '';

    if (text && chatJid) {
      try {
        // Simpan ke MongoDB dengan chatJid lawan bicara
        await Message.create({ chatJid, text, fromMe });

        // Broadcast real-time via Socket.IO ke Frontend
        io.emit('incoming-message', { chatJid, text, fromMe });
      } catch (err) {
        console.error('Gagal simpan pesan:', err);
      }
    }
  });
}

// Endpoint Ambil Seluruh Riwayat Pesan dari Database
app.get('/messages', async (req, res) => {
  try {
    const history = await Message.find().sort({ timestamp: 1 });
    res.json(history);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Endpoint Kirim Pesan
app.post('/send-message', async (req, res) => {
  const { number, message } = req.body;
  if (!sock) return res.status(500).json({ status: false, message: 'WA belum siap / terhubung' });

  try {
    const cleanedNumber = cleanNumber(number);
    const recipientJid = `${cleanedNumber}@s.whatsapp.net`;
    
    // Kirim via Baileys (nanti disimpen otomatis lewat event messages.upsert dari WA)
    await sock.sendMessage(recipientJid, { text: message });

    res.json({ status: true, message: 'Terkirim' });
  } catch (err) {
    res.status(500).json({ status: false, message: err.message });
  }
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  connectToWhatsApp();
});
