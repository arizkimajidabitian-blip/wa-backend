const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const QRCode = require('qrcode');
const {
  default: makeWASocket,
  useMultiFileAuthState,
  DisconnectReason
} = require('@whiskeysockets/baileys');

const app = express();
const server = http.createServer(app);

app.use(cors());
app.use(express.json());

const io = new Server(server, {
  cors: { origin: "*", methods: ["GET", "POST"] }
});

let sock;

async function connectToWhatsApp() {
  const { state, saveCreds } = await useMultiFileAuthState('auth_info_baileys');
  sock = makeWASocket({ auth: state, printQRInTerminal: false });

  sock.ev.on('creds.update', saveCreds);

  sock.ev.on('connection.update', async (update) => {
    const { connection, lastDisconnect, qr } = update;

    if (qr) {
      const qrBase64 = await QRCode.toDataURL(qr);
      io.emit('qr', qrBase64);
      io.emit('status', 'Silakan Scan QR Code');
    }

    if (connection === 'open') {
      io.emit('status', 'Terhubung ke WhatsApp!');
      io.emit('qr', null);
    }

    if (connection === 'close') {
      const reason = lastDisconnect?.error?.output?.statusCode;
      const shouldReconnect = reason !== DisconnectReason.loggedOut;
      io.emit('status', 'Koneksi terputus. Menghubungkan ulang...');
      if (shouldReconnect) connectToWhatsApp();
    }
  });
}

app.post('/send-message', async (req, res) => {
  const { number, message } = req.body;
  if (!sock) return res.status(500).json({ status: false, message: 'WA belum siap' });
  if (!number || !message) return res.status(400).json({ status: false, message: 'Isi nomor dan pesan' });

  try {
    let cleanedNumber = number.replace(/[^0-9]/g, '');
    if (cleanedNumber.startsWith('0')) cleanedNumber = '62' + cleanedNumber.slice(1);
    const recipientJid = `${cleanedNumber}@s.whatsapp.net`;
    
    await sock.sendMessage(recipientJid, { text: message });
    res.json({ status: true, message: 'Pesan berhasil terkirim!' });
  } catch (err) {
    res.status(500).json({ status: false, message: err.message });
  }
});

io.on('connection', (socket) => {
  socket.emit('status', 'Terhubung ke Gateway Server');
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Server WA Gateway aktif di port ${PORT}`);
  connectToWhatsApp();
});
