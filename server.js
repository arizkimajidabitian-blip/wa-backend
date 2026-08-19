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

// Middleware CORS & Parser Body
app.use(cors({ origin: "*", methods: ["GET", "POST", "DELETE"] }));
app.use(express.json());

// Mengambil variabel MONGO_URI dari Environment Railway
const MONGO_URI = process.env.MONGO_URI;

if (!MONGO_URI) {
  console.error("FATAL ERROR: MONGO_URI belum di-set di Environment Variables Railway!");
}

// Koneksi ke Database MongoDB Atlas
mongoose.connect(MONGO_URI)
  .then(() => console.log('MongoDB Atlas Connected Successfully'))
  .catch(err => console.error('MongoDB Connection Error:', err));

// ================= SKEMA & MODEL MONGODB =================
const MessageSchema = new mongoose.Schema({
  chatJid: { type: String, required: true },
  text: { type: String, required: true },
  fromMe: { type: Boolean, default: false },
  timestamp: { type: Date, default: Date.now }
});
const Message = mongoose.model('Message', MessageSchema);

const ContactSchema = new mongoose.Schema({
  jid: { type: String, unique: true, required: true },
  name: { type: String, required: true },
  custom: { type: Boolean, default: false }
});
const Contact = mongoose.model('Contact', ContactSchema);

const StorySchema = new mongoose.Schema({
  senderJid: String,
  senderName: String,
  text: String,
  timestamp: { type: Date, default: Date.now }
});
const Story = mongoose.model('Story', StorySchema);

// Inisialisasi Socket.IO
const io = new Server(server, {
  cors: { origin: "*", methods: ["GET", "POST"] }
});

let sock;
let lastQR = null;

// Fungsi Format dan Sanitasi Nomor Telepon
function cleanNumber(jidOrNumber) {
  if (!jidOrNumber) return '';
  let cleaned = jidOrNumber.replace('@s.whatsapp.net', '').replace('@g.us', '').replace(/[^0-9]/g, '');
  if (cleaned.startsWith('0')) cleaned = '62' + cleaned.slice(1);
  return cleaned;
}

// Koneksi Utama WhatsApp dengan Baileys
async function connectToWhatsApp() {
  const { state, saveCreds } = await useMultiFileAuthState('auth_info_baileys');
  
  sock = makeWASocket({ 
    auth: state, 
    printQRInTerminal: true,
    browser: ["WA Gateway Pro", "Chrome", "1.0.0"],
    syncFullHistory: true
  });

  sock.ev.on('creds.update', saveCreds);

  // Penanganan Status Koneksi dan QR Code
  sock.ev.on('connection.update', async (update) => {
    const { connection, lastDisconnect, qr } = update;

    if (qr) {
      try {
        lastQR = await QRCode.toDataURL(qr);
        io.emit('qr', lastQR);
        io.emit('status', 'Silakan Scan QR Code');
      } catch (err) {
        console.error('Error Generating Base64 QR Image:', err);
      }
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
      console.log(`Koneksi terputus (Reason: ${reason}). Reconnecting: ${shouldReconnect}`);
      if (shouldReconnect) {
        setTimeout(connectToWhatsApp, 3000);
      }
    }
  });

  // Sinkronisasi History Chat dan Kontak Pertama Kali
  sock.ev.on('messaging-history.set', async ({ contacts, messages }) => {
    try {
      if (contacts && contacts.length > 0) {
        for (const c of contacts) {
          const cleaned = cleanNumber(c.id);
          const name = c.name || c.notify || c.verifiedName;
          if (cleaned && name) {
            await Contact.findOneAndUpdate(
              { jid: cleaned }, 
              { $setOnInsert: { name, custom: false } }, 
              { upsert: true }
            );
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
      console.error('Error Sync Messaging History:', err);
    }
  });

  // Penambahan Kontak Bawaan Secara Otomatis
  sock.ev.on('contacts.upsert', async (contacts) => {
    for (const c of contacts) {
      const cleaned = cleanNumber(c.id);
      const name = c.name || c.notify || c.verifiedName;
      if (cleaned && name) {
        await Contact.findOneAndUpdate(
          { jid: cleaned },
          { $setOnInsert: { name, custom: false } },
          { upsert: true }
        );
      }
    }
    io.emit('contacts-updated');
  });

  // Event Pesan Masuk & Capture Story WA
  sock.ev.on('messages.upsert', async (m) => {
    const msg = m.messages[0];
    if (!msg || !msg.key || !msg.message) return;

    const remoteJid = msg.key.remoteJid || '';

    // Menangkap Story / Status WA
    if (remoteJid === 'status@broadcast') {
      const senderJid = cleanNumber(msg.key.participant || msg.participant || '');
      const senderName = msg.pushName || senderJid;
      const textStory = msg.message?.conversation || msg.message?.extendedTextMessage?.text || msg.message?.imageMessage?.caption || '';

      if (senderJid && textStory) {
        const newStory = await Story.create({ senderJid, senderName, text: textStory });
        io.emit('incoming-story', newStory);
      }
      return;
    }

    let chatJid = cleanNumber(remoteJid);
    const fromMe = msg.key.fromMe || false;

    if (!chatJid && sock.user) {
      chatJid = cleanNumber(sock.user.id);
    }

    const text = 
      msg.message?.conversation || 
      msg.message?.extendedTextMessage?.text || 
      msg.message?.imageMessage?.caption || '';

    // Otomatis Simpan Nama Kontak dari PushName Pesan
    if (msg.pushName && chatJid) {
      const existing = await Contact.findOne({ jid: chatJid });
      if (!existing) {
        await Contact.create({ jid: chatJid, name: msg.pushName, custom: false });
        io.emit('contacts-updated');
      }
    }

    // Simpan Pesan ke DB Atlas dan Broadcast Realtime
    if (text && chatJid) {
      try {
        const saved = await Message.create({ chatJid, text, fromMe });
        io.emit('incoming-message', { 
          chatJid, 
          text, 
          fromMe, 
          timestamp: saved.timestamp 
        });
      } catch (err) {
        console.error('Gagal Menyimpan Pesan Masuk:', err);
      }
    }
  });
}

// ================= SOCKET.IO HANDLERS =================
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
    } else {
      socket.emit('status', 'Menyiapkan QR Code...');
    }
  });
});

// ================= REST API ENDPOINTS =================

// Endpoint Ambil Semua Kontak
app.get('/contacts', async (req, res) => {
  try {
    const contacts = await Contact.find().sort({ name: 1 });
    res.json(contacts);
  } catch (err) { 
    res.status(500).json([]); 
  }
});

// Endpoint Tambah Kontak Manual Permanen ke MongoDB Atlas
app.post('/contacts', async (req, res) => {
  try {
    const { number, name } = req.body;
    let cleaned = cleanNumber(number);

    if (!cleaned || !name) {
      return res.status(400).json({ status: false, message: 'Nomor dan nama wajib diisi!' });
    }

    const contact = await Contact.findOneAndUpdate(
      { jid: cleaned },
      { name, custom: true },
      { upsert: true, new: true }
    );

    io.emit('contacts-updated');
    res.json({ status: true, contact });
  } catch (err) {
    res.status(500).json({ status: false, message: err.message });
  }
});

// Endpoint Ambil Semua Riwayat Pesan
app.get('/messages', async (req, res) => {
  try {
    const history = await Message.find().sort({ timestamp: 1 });
    res.json(history);
  } catch (err) { 
    res.status(500).json([]); 
  }
});

// Endpoint Ambil Riwayat Story
app.get('/stories', async (req, res) => {
  try {
    const stories = await Story.find().sort({ timestamp: -1 }).limit(30);
    res.json(stories);
  } catch (err) { 
    res.status(500).json([]); 
  }
});

// Endpoint Kirim Pesan WA
app.post('/send-message', async (req, res) => {
  const { number, message } = req.body;

  if (!sock) {
    return res.status(500).json({ status: false, message: 'WhatsApp belum terhubung' });
  }

  try {
    let cleanedNumber = cleanNumber(number);
    if (!cleanedNumber) {
      return res.status(400).json({ status: false, message: 'Format nomor telepon tidak valid' });
    }

    const recipientJid = `${cleanedNumber}@s.whatsapp.net`;
    
    // Kirim via Baileys
    await sock.sendMessage(recipientJid, { text: message });

    // Simpan ke DB Atlas
    const saved = await Message.create({ chatJid: cleanedNumber, text: message, fromMe: true });

    // Broadcast Realtime ke Seluruh Klien (PC & HP)
    io.emit('incoming-message', { 
      chatJid: cleanedNumber, 
      text: message, 
      fromMe: true, 
      timestamp: saved.timestamp 
    });

    res.json({ status: true, message: 'Pesan Berhasil Terkirim' });
  } catch (err) {
    console.error('Error Send Message Endpoint:', err);
    res.status(500).json({ status: false, message: err.message });
  }
});

// Jalankan Server HTTP & Socket
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Server Gateway Aktif di Port: ${PORT}`);
  connectToWhatsApp();
});
