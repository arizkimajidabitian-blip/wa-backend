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

// Schema Kontak
const ContactSchema = new mongoose.Schema({
  jid: { type: String, unique: true },
  name: String
});
const Contact = mongoose.model('Contact', ContactSchema);

// Schema Story WA
const StorySchema = new mongoose.Schema({
  senderJid: String,
  senderName: String,
  text: String,
  timestamp: { type: Date, default: Date.now }
});
const Story = mongoose.model('Story', StorySchema);

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
    browser: ["WA Web Lite v2", "Chrome", "1.0.0"],
    syncFullHistory: true // Paksa WA kirim seluruh history chat & kontak
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

  // 1. TANGKAP HISTORY LENGKAP SAAT SCAN QR PERTAMA
  sock.ev.on('messaging-history.set', async ({ contacts, messages, chats }) => {
    try {
      console.log('--- MENERIMA SINKRONISASI HISTORY DARI WA ---');
      
      // Simpan Kontak
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
      }

      // Simpan Riwayat Pesan
      if (messages && messages.length > 0) {
        for (const msg of messages) {
          if (!msg.key || !msg.message || msg.key.remoteJid === 'status@broadcast') continue;
          
          const chatJid = cleanNumber(msg.key.remoteJid);
          const fromMe = msg.key.fromMe || false;
          const text = msg.message?.conversation || msg.message?.extendedTextMessage?.text || msg.message?.imageMessage?.caption || '';

          if (text && chatJid) {
            const time = msg.messageTimestamp ? new Date(msg.messageTimestamp * 1000) : new Date();
            await Message.create({ chatJid, text, fromMe, timestamp: time });
          }
        }
      }

      console.log('--- HISTORY BERHASIL DISIMPAN KE DB ---');
      io.emit('contacts-updated');
    } catch (err) {
      console.error('Error Sync History:', err);
    }
  });

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

  sock.ev.on('messages.upsert', async (m) => {
    const msg = m.messages[0];
    if (!msg || !msg.key || !msg.message) return;

    const remoteJid = msg.key.remoteJid || '';
    const text = 
      msg.message?.conversation || 
      msg.message?.extendedTextMessage?.text || 
      msg.message?.imageMessage?.caption || 
      '';

    if (remoteJid === 'status@broadcast') {
      const senderJid = cleanNumber(msg.key.participant || msg.participant || '');
      const senderName = msg.pushName || senderJid;
      
      if (senderJid && text) {
        const newStory = await Story.create({ senderJid, senderName, text });
        io.emit('incoming-story', newStory);
      }
      return;
    }

    const chatJid = cleanNumber(remoteJid);
    const fromMe = msg.key.fromMe || false;

    if (msg.pushName && chatJid) {
      await Contact.findOneAndUpdate(
        { jid: chatJid },
        { name: msg.pushName },
        { upsert: true }
      );
    }

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

app.get('/stories', async (req, res) => {
  try {
    const stories = await Story.find().sort({ timestamp: -1 }).limit(30);
    res.json(stories);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/send-message', async (req, res) => {
  const { number, message } = req.body;
  if (!sock) return res.status(500).json({ status: false, message: 'WA belum siap' });

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
