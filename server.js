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

const MONGO_URI = process.env.MONGO_URI || "mongodb+srv://admin:rahasia123@cluster0.abcde.mongodb.net/wagateway?retryWrites=true&w=majority";

mongoose.connect(MONGO_URI)
  .then(() => console.log('MongoDB Connected Successfully'))
  .catch(err => console.error('MongoDB Connection Error:', err));

// Schema Pesan
const MessageSchema = new mongoose.Schema({
  chatJid: String,
  text: String,
  fromMe: Boolean,
  timestamp: { type: Date, default: Date.now }
});
const Message = mongoose.model('Message', MessageSchema);

// Schema Kontak (Baru!)
const ContactSchema = new mongoose.Schema({
  jid: { type: String, unique: true }, // Contoh: 628123456789
  name: String
});
const Contact = mongoose.model('Contact', ContactSchema);

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

  // 1. Simpan Kontak dari Sinkronisasi Awal WA (Saat Scan)
  sock.ev.on('messaging-history.set', async ({ contacts }) => {
    if (contacts && contacts.length > 0) {
      for (const c of contacts) {
        const cleaned = cleanNumber(c.id);
        const name = c.name || c.notify || c.verifiedName;
        if (cleaned && name) {
          await Contact.findOneAndUpdate(
            { jid: cleaned },
            { name },
            { upsert: true, new: true }
          );
        }
      }
      io.emit('contacts-updated');
    }
  });

  // 2. Simpan Kontak yang Baru Masuk/Diupdate
  sock.ev.on('contacts.upsert', async (contacts) => {
    for (const c of contacts) {
      const cleaned = cleanNumber(c.id);
      const name = c.name || c.notify || c.verifiedName;
      if (cleaned && name) {
        await Contact.findOneAndUpdate(
          { jid: cleaned },
          { name },
          { upsert: true, new: true }
        );
      }
    }
    io.emit('contacts-updated');
  });

  // 3. Simpan Pesan dan Nama Pengirim
  sock.ev.on('messages.upsert', async (m) => {
    const msg = m.messages[0];
    if (!msg || !msg.key || !msg.message) return;
    if (msg.key.remoteJid === 'status@broadcast') return;

    const remoteJid = msg.key.remoteJid || '';
    const chatJid = cleanNumber(remoteJid);
    const fromMe = msg.key.fromMe || false;

    // Jika pesan membawa nama pushName/notifyName pengirim, simpan ke Kontak
    if (msg.pushName && chatJid) {
      await Contact.findOneAndUpdate(
        { jid: chatJid },
        { name: msg.pushName },
        { upsert: true }
      );
    }

    const text = 
      msg.message?.conversation || 
      msg.message?.extendedTextMessage?.text || 
      msg.message?.imageMessage?.caption || 
      '';

    if (text && chatJid) {
      try {
        await Message.create({ chatJid, text, fromMe });
        io.emit('incoming-message', { chatJid, text, fromMe, pushName: msg.pushName });
      } catch (err) {
        console.error('Gagal simpan pesan:', err);
      }
    }
  });
}

// Endpoint Ambil Daftar Kontak
app.get('/contacts', async (req, res) => {
  try {
    const contacts = await Contact.find();
    res.json(contacts);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/messages', async (req, res) => {
  try {
    const history = await Message.find().sort({ timestamp: 1 });
    res.json(history);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/send-message', async (req, res) => {
  const { number, message } = req.body;
  if (!sock) return res.status(500).json({ status: false, message: 'WA belum siap / terhubung' });

  try {
    const cleanedNumber = cleanNumber(number);
    const recipientJid = `${cleanedNumber}@s.whatsapp.net`;
    
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
