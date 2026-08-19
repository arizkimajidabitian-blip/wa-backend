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

function parseJid(jidOrNumber) {
  if (!jidOrNumber) return { cleanJid: '', isGroup: false };
  let str = jidOrNumber.toString().trim();
  
  if (str.endsWith('@g.us')) {
    return { cleanJid: str, isGroup: true };
  }
  
  let cleaned = str.split('@')[0].replace(/[^0-9]/g, '');
  if (cleaned.startsWith('0')) cleaned = '62' + cleaned.slice(1);
  
  return { cleanJid: cleaned, isGroup: false };
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
    try {
      const msg = m.messages[0];
      if (!msg || !msg.message) return;

      const rawJid = msg.key.remoteJid || '';
      if (rawJid.includes('status@broadcast')) return;

      const { cleanJid, isGroup } = parseJid(rawJid);
      if (!cleanJid) return;

      const fromMe = msg.key.fromMe || false;

      const text = 
        msg.message.conversation || 
        msg.message.extendedTextMessage?.text || 
        msg.message.imageMessage?.caption || 
        msg.message.videoMessage?.caption || '';

      if (!text) return;

      let chatName = cleanJid;
      if (isGroup) {
        try {
          const groupMeta = await sock.groupMetadata(cleanJid);
          chatName = groupMeta.subject || cleanJid;
        } catch (e) {
          chatName = 'Grup WA';
        }
      } else {
        chatName = msg.pushName || cleanJid;
      }

      let existingContact = memoryContacts.find(c => c.jid === cleanJid);
      if (!existingContact) {
        const newContact = { jid: cleanJid, name: chatName, custom: false };
        memoryContacts.push(newContact);
        
        if (isDbConnected) {
          try {
            await Contact.findOneAndUpdate(
              { jid: cleanJid },
              { jid: cleanJid, name: chatName, custom: false },
              { upsert: true }
            );
          } catch (e) {}
        }
        io.emit('contacts-updated');
      }

      const msgObj = { _id: new mongoose.Types.ObjectId().toString(), chatJid: cleanJid, text, fromMe, timestamp: new Date() };
      
      if (isDbConnected) {
        try { 
          const saved = await Message.create({ chatJid: cleanJid, text, fromMe });
          msgObj._id = saved._id.toString();
        } catch(e){}
      }

      memoryMessages.push(msgObj);
      io.emit('incoming-message', msgObj);

    } catch (err) {
      console.error('Error messages.upsert:', err);
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

// ================= REST API ENDPOINTS =================

app.get('/contacts', async (req, res) => {
  if (isDbConnected) {
    try {
      const dbContacts = await Contact.find().sort({ name: 1 });
      if (dbContacts && dbContacts.length > 0) return res.json(dbContacts);
    } catch (err) {}
  }
  res.json(memoryContacts);
});

app.post('/contacts', async (req, res) => {
  const { number, name } = req.body;
  const { cleanJid } = parseJid(number);

  if (!cleanJid || !name) {
    return res.status(400).json({ status: false, message: 'Nomor dan nama wajib diisi!' });
  }

  const contactObj = { jid: cleanJid, name, custom: true };

  memoryContacts = memoryContacts.filter(c => c.jid !== cleanJid);
  memoryContacts.push(contactObj);

  if (isDbConnected) {
    try {
      await Contact.findOneAndUpdate(
        { jid: cleanJid },
        { jid: cleanJid, name, custom: true },
        { upsert: true }
      );
    } catch (err) {}
  }

  io.emit('contacts-updated');
  return res.json({ status: true, contact: contactObj });
});

app.get('/messages', async (req, res) => {
  if (isDbConnected) {
    try {
      const dbMessages = await Message.find().sort({ timestamp: 1 });
      if (dbMessages && dbMessages.length > 0) return res.json(dbMessages);
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
    const { cleanJid, isGroup } = parseJid(number);
    if (!cleanJid) return res.status(400).json({ status: false, message: 'Nomor/JID tidak valid' });

    const recipientJid = isGroup ? cleanJid : `${cleanJid}@s.whatsapp.net`;
    await sock.sendMessage(recipientJid, { text: message });

    const msgObj = { _id: new mongoose.Types.ObjectId().toString(), chatJid: cleanJid, text: message, fromMe: true, timestamp: new Date() };

    if (isDbConnected) {
      try { 
        const saved = await Message.create({ chatJid: cleanJid, text: message, fromMe: true }); 
        msgObj._id = saved._id.toString();
      } catch(e){}
    }

    memoryMessages.push(msgObj);
    io.emit('incoming-message', msgObj);
    res.json({ status: true, message: 'Terkirim' });
  } catch (err) {
    res.status(500).json({ status: false, message: err.message });
  }
});

// FITUR BARU: HAPUS SEMUA CHAT BERDASARKAN KONTAK
app.delete('/chat/:jid', async (req, res) => {
  const { jid } = req.params;
  const { cleanJid } = parseJid(jid);

  memoryMessages = memoryMessages.filter(m => m.chatJid !== cleanJid);

  if (isDbConnected) {
    try { await Message.deleteMany({ chatJid: cleanJid }); } catch(err){}
  }

  io.emit('messages-deleted', { chatJid: cleanJid });
  res.json({ status: true, message: 'Chat berhasil dihapus' });
});

// FITUR BARU: HAPUS SPESIFIK SATU PESAN BY ID
app.delete('/messages/:id', async (req, res) => {
  const { id } = req.params;

  memoryMessages = memoryMessages.filter(m => m._id !== id);

  if (isDbConnected) {
    try { await Message.findByIdAndDelete(id); } catch(err){}
  }

  io.emit('message-single-deleted', { id });
  res.json({ status: true, message: 'Pesan berhasil dihapus' });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Server Gateway Aktif di Port: ${PORT}`);
  connectToWhatsApp();
});
