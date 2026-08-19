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

app.use(cors({ origin: "*", methods: ["GET", "POST"] }));
app.use(express.json());

// Ambil MONGO_URI dari Environment Variables Railway
const MONGO_URI = process.env.MONGO_URI;

if (!MONGO_URI) {
  console.error("FATAL ERROR: MONGO_URI belum di-set di Railway!");
}

mongoose.connect(MONGO_URI)
  .then(() => console.log('MongoDB Connected Successfully'))
  .catch(err => console.error('MongoDB Connection Error:', err));

// Skema Database MongoDB
const MessageSchema = new mongoose.Schema({
  chatJid: { type: String, required: true },
  text: { type: String, required: true },
  fromMe: { type: Boolean, default: false },
  timestamp: { type: Date, default: Date.now }
});
const Message = mongoose.model('Message', MessageSchema);

const ContactSchema = new mongoose.Schema({
  jid: { type: String, unique: true },
  name: String
});
const Contact = mongoose.model('Contact', ContactSchema);

const io = new Server(server, {
  cors: { origin: "*", methods: ["GET", "POST"] }
});

let sock;
let lastQR = null; // Penampung QR Code Instan

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
    browser: ["WA Web Gateway", "Chrome", "1.0.0"],
    syncFullHistory: true
  });

  sock.ev.on('creds.update', saveCreds);

  sock.ev.on('connection.update', async (update) => {
    const { connection, lastDisconnect, qr } = update;

    if (qr) {
      lastQR = await QRCode.toDataURL(qr);
      io.emit('qr', lastQR);
      io.emit('status', 'Silakan Scan QR Code');
    }

    if (connection === 'open') {
      lastQR = null;
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

  sock.ev.on('messaging-history.set', async ({ contacts, messages }) => {
    try {
      if (contacts && contacts.length > 0) {
        for (const c of contacts) {
          const cleaned = cleanNumber(c.id);
          const name = c.name || c.notify || c.verifiedName;
          if (cleaned && name) {
            await Contact.findOneAndUpdate({ jid: cleaned }, { name }, { upsert: true });
          }
        }
      }

      if (messages && messages.length > 0) {
        for (const msg of messages) {
          if (!msg.key || !msg.message || msg.key.remoteJid === 'status@broadcast') continue;
          
          let chatJid = cleanNumber(msg.key.remoteJid);
          if (!chatJid && sock.user) chatJid = cleanNumber(sock.user.id);

          const fromMe = msg.key.fromMe || false;
          const text = msg.message?.conversation || msg.message?.extendedTextMessage?.text || msg.message?.imageMessage?.caption || '';

          if (text && chatJid) {
            const time = msg.messageTimestamp ? new Date(msg.messageTimestamp * 1000) : new Date();
            await Message.create({ chatJid, text, fromMe, timestamp: time });
          }
        }
      }
      io.emit('contacts-updated');
    } catch (err) {
      console.error('Error Sync History:', err);
    }
  });

  sock.ev.on('messages.upsert', async (m) => {
    const msg = m.messages[0];
    if (!msg || !msg.key || !msg.message) return;

    const remoteJid = msg.key.remoteJid || '';
    if (remoteJid === 'status@broadcast') return;

    let chatJid = cleanNumber(remoteJid);
    const fromMe = msg.key.fromMe || false;
    const text = msg.message?.conversation || msg.message?.extendedTextMessage?.text || msg.message?.imageMessage?.caption || '';

    if (msg.pushName && chatJid) {
      await Contact.findOneAndUpdate({ jid: chatJid }, { name: msg.pushName }, { upsert: true });
      io.emit('contacts-updated');
    }

    if (text && chatJid) {
      try {
        const saved = await Message.create({ chatJid, text, fromMe });
        io.emit('incoming-message', { chatJid, text, fromMe, timestamp: saved.timestamp });
      } catch (err) {
        console.error('Gagal simpan pesan:', err);
      }
    }
  });
}

// Socket handler untuk koneksi instan
io.on('connection', (socket) => {
  if (sock && sock.user) {
    socket.emit('status', 'Terhubung');
    socket.emit('qr', null);
  } else {
    socket.emit('status', lastQR ? 'Silakan Scan QR Code' : 'Menyiapkan QR Code...');
    if (lastQR) socket.emit('qr', lastQR);
  }

  socket.on('request-qr', () => {
    if (lastQR) {
      socket.emit('qr', lastQR);
      socket.emit('status', 'Silakan Scan QR Code');
    } else if (sock && sock.user) {
      socket.emit('status', 'Terhubung');
      socket.emit('qr', null);
    } else {
      socket.emit('status', 'Menyiapkan QR Code...');
    }
  });
});

// REST API Endpoints
app.get('/contacts', async (req, res) => {
  try {
    const contacts = await Contact.find();
    res.json(contacts);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/messages', async (req, res) => {
  try {
    const history = await Message.find().sort({ timestamp: 1 });
    res.json(history);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/send-message', async (req, res) => {
  const { number, message } = req.body;
  if (!sock) return res.status(500).json({ status: false, message: 'WA belum siap' });

  try {
    const cleanedNumber = cleanNumber(number);
    const recipientJid = `${cleanedNumber}@s.whatsapp.net`;
    
    await sock.sendMessage(recipientJid, { text: message });
    const saved = await Message.create({ chatJid: cleanedNumber, text: message, fromMe: true });

    io.emit('incoming-message', { 
      chatJid: cleanedNumber, 
      text: message, 
      fromMe: true, 
      timestamp: saved.timestamp 
    });

    res.json({ status: true, message: 'Terkirim' });
  } catch (err) {
    res.status(500).json({ status: false, message: err.message });
  }
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  connectToWhatsApp();
});
