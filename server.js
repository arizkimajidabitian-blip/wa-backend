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

app.use(cors({ origin: "*", methods: ["GET", "POST", "DELETE", "PUT"] }));
app.use(express.json());

const MONGO_URI = process.env.MONGO_URI;

let isDbConnected = false;

// Penampung Cadangan (In-Memory Fallback)
let memoryContacts = [];
let memoryMessages = [];

if (MONGO_URI) {
  mongoose.set('bufferCommands', false);
  mongoose.connect(MONGO_URI, { serverSelectionTimeoutMS: 5000 })
    .then(() => {
      isDbConnected = true;
      console.log('MongoDB Atlas Connected Successfully!');
    })
    .catch(err => {
      isDbConnected = false;
      console.error('MongoDB Connection Error (Running on Fallback Mode):', err.message);
    });
}

// ================= SKEMA MONGODB =================
const MessageSchema = new mongoose.Schema({
  chatJid: { type: String, required: true },
  text: { type: String, required: true },
  fromMe: { type: Boolean, default: false },
  timestamp: { type: Date, default: Date.now }
});
const Message = mongoose.model('Message', MessageSchema);

const ContactSchema = new mongoose.Schema({
  jid: { type: String, required: true },
  name: { type: String, required: true },
  custom: { type: Boolean, default: false }
});
const Contact = mongoose.model('Contact', ContactSchema);

const io = new Server(server, { cors: { origin: "*", methods: ["GET", "POST"] } });

let sock;
let lastQR = null;

function cleanNumber(jidOrNumber) {
  if (!jidOrNumber) return '';
  let cleaned = jidOrNumber.toString().replace('@s.whatsapp.net', '').replace('@g.us', '').replace(/[^0-9]/g, '');
  if (cleaned.startsWith('0')) cleaned = '62' + cleaned.slice(1);
  return cleaned;
}

async function connectToWhatsApp() {
  const { state, saveCreds } = await useMultiFileAuthState('auth_info_baileys');
  
  sock = makeWASocket({ 
    auth: state, 
    printQRInTerminal: true,
    browser: ["WA Gateway Pro", "Chrome", "1.0.0"],
    syncFullHistory: true
  });

  sock.ev.on('creds.update', saveCreds);

  sock.ev.on('connection.update', async (update) => {
    const { connection, lastDisconnect, qr } = update;

    if (qr) {
      try {
        lastQR = await QRCode.toDataURL(qr);
        io.emit('qr', lastQR);
        io.emit('status', 'Silakan Scan QR Code');
      } catch (err) { console.error('QR Error:', err); }
    }

    if (connection === 'open') {
      lastQR = null;
      io.emit('status', 'Terhubung');
      io.emit('qr', null);
      console.log('WhatsApp Gateway Online!');
    }

    if (connection === 'close') {
      const reason = lastDisconnect?.error?.output?.statusCode;
      const shouldReconnect = reason !== DisconnectReason.loggedOut;
      io.emit('status', 'Terputus. Menghubungkan ulang...');
      if (shouldReconnect) setTimeout(connectToWhatsApp, 3000);
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

    if (text && chatJid) {
      const msgObj = { chatJid, text, fromMe, timestamp: new Date() };
      memoryMessages.push(msgObj);

      if (isDbConnected) {
        try { await Message.create(msgObj); } catch(e){}
      }

      io.emit('incoming-message', msgObj);
    }
  });
}

io.on('connection', (socket) => {
  if (sock && sock.user) {
    socket.emit('status', 'Terhubung');
    socket.emit('qr', null);
  } else if (lastQR) {
    socket.emit('qr', lastQR);
    socket.emit('status', 'Silakan Scan QR Code');
  } else {
    socket.emit('status', 'Menyiapkan QR Code...');
  }

  socket.on('request-qr', () => {
    if (lastQR) {
      socket.emit('qr', lastQR);
      socket.emit('status', 'Silakan Scan QR Code');
    } else if (sock && sock.user) {
      socket.emit('status', 'Terhubung');
      socket.emit('qr', null);
    }
  });
});

// ================= API ENDPOINTS =================

app.get('/contacts', async (req, res) => {
  if (isDbConnected) {
    try {
      const dbContacts = await Contact.find().sort({ name: 1 });
      return res.json(dbContacts);
    } catch (err) {}
  }
  res.json(memoryContacts);
});

app.post('/contacts', async (req, res) => {
  const { number, name } = req.body;
  let cleaned = cleanNumber(number);

  if (!cleaned || !name) {
    return res.status(400).json({ status: false, message: 'Nomor dan nama wajib diisi!' });
  }

  const contactObj = { jid: cleaned, name, custom: true };

  // Update memori lokal
  memoryContacts = memoryContacts.filter(c => c.jid !== cleaned);
  memoryContacts.push(contactObj);

  // Update DB jika terhubung
  if (isDbConnected) {
    try {
      await Contact.findOneAndUpdate(
        { jid: cleaned },
        { jid: cleaned, name, custom: true },
        { upsert: true }
      );
    } catch (err) {
      console.error("Gagal simpan DB Atlas:", err.message);
    }
  }

  io.emit('contacts-updated');
  return res.json({ status: true, contact: contactObj });
});

app.get('/messages', async (req, res) => {
  if (isDbConnected) {
    try {
      const dbMessages = await Message.find().sort({ timestamp: 1 });
      return res.json(dbMessages);
    } catch (err) {}
  }
  res.json(memoryMessages);
});

app.post('/send-message', async (req, res) => {
  const { number, message } = req.body;

  if (!sock) {
    return res.status(500).json({ status: false, message: 'WhatsApp belum terhubung' });
  }

  try {
    let cleanedNumber = cleanNumber(number);
    if (!cleanedNumber) return res.status(400).json({ status: false, message: 'Nomor tidak valid' });

    const recipientJid = `${cleanedNumber}@s.whatsapp.net`;
    await sock.sendMessage(recipientJid, { text: message });

    const msgObj = { chatJid: cleanedNumber, text: message, fromMe: true, timestamp: new Date() };
    memoryMessages.push(msgObj);

    if (isDbConnected) {
      try { await Message.create(msgObj); } catch(e){}
    }

    io.emit('incoming-message', msgObj);
    res.json({ status: true, message: 'Terkirim' });
  } catch (err) {
    res.status(500).json({ status: false, message: err.message });
  }
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Server Gateway Aktif di Port: ${PORT}`);
  connectToWhatsApp();
});
