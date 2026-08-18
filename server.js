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
// Masukkan URI MongoDB Atlas kamu di sini atau lewat Environment Variable MONGO_URI di Railway
const MONGO_URI = process.env.MONGO_URI || "mongodb+srv://admin:rahasia123@cluster0.abcde.mongodb.net/wagateway?retryWrites=true&w=majority";

mongoose.connect(MONGO_URI)
  .then(() => console.log('MongoDB Connected Successfully'))
  .catch(err => console.error('MongoDB Connection Error:', err));

// Schema Model Pesan
const MessageSchema = new mongoose.Schema({
  from: String,
  text: String,
  fromMe: Boolean,
  timestamp: { type: Date, default: Date.now }
});
const Message = mongoose.model('Message', MessageSchema);

const io = new Server(server, {
  cors: { origin: "*", methods: ["GET", "POST"] }
});

let sock;

async function connectToWhatsApp() {
  const { state, saveCreds } = await useMultiFileAuthState('auth_info_baileys');
  sock = makeWASocket({ auth: state, printQRInTerminal: true });

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

  // Mendengarkan dan menyimpan pesan masuk ke Database
  sock.ev.on('messages.upsert', async (m) => {
    const msg = m.messages[0];
    if (!msg || !msg.key || !msg.message) return;
    
    if (m.type === 'notify') {
      const sender = msg.key.remoteJid.replace('@s.whatsapp.net', '').replace('@g.us', '');
      const text = msg.message?.conversation || msg.message?.extendedTextMessage?.text || '';
      const fromMe = msg.key.fromMe || false;

      if (text) {
        // Simpan ke MongoDB
        await Message.create({ from: sender, text, fromMe });
        // Emit ke UI real-time
        io.emit('incoming-message', { from: sender, text, fromMe });
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
  if (!sock) return res.status(500).json({ status: false, message: 'WA belum siap' });

  try {
    let cleanedNumber = number.replace(/[^0-9]/g, '');
    if (cleanedNumber.startsWith('0')) cleanedNumber = '62' + cleanedNumber.slice(1);
    const recipientJid = `${cleanedNumber}@s.whatsapp.net`;
    
    await sock.sendMessage(recipientJid, { text: message });

    // Simpan pesan keluar ke MongoDB
    await Message.create({ from: cleanedNumber, text: message, fromMe: true });

    res.json({ status: true, message: 'Terkirim' });
  } catch (err) {
    res.status(500).json({ status: false, message: err.message });
  }
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  connectToWhatsApp();
});
