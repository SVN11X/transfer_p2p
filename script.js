// Configuración: PeerJS usa su servidor cloud gratuito
const PEER_CONFIG = {
  host: '0.peerjs.com',
  port: 443,
  secure: true,
};

let peer;
let conn;
let selectedFile;

// ----- Funciones de utilidad -----
function formatBytes(bytes) {
  if (bytes === 0) return '0 Bytes';
  const k = 1024;
  const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

// ----- Panel de envío -----
const fileInput = document.getElementById('fileInput');
const fileInfo = document.getElementById('fileInfo');
const createRoomBtn = document.getElementById('createRoomBtn');
const roomInfo = document.getElementById('roomInfo');
const roomIdDisplay = document.getElementById('roomIdDisplay');
const roomLink = document.getElementById('roomLink');
const copyBtn = document.getElementById('copyBtn');
const sendProgress = document.getElementById('sendProgress');
const sendBarFill = document.getElementById('sendBarFill');
const sendPercent = document.getElementById('sendPercent');
const sendStatus = document.getElementById('sendStatus');

fileInput.addEventListener('change', (e) => {
  selectedFile = e.target.files[0];
  if (selectedFile) {
    fileInfo.textContent = `Archivo: ${selectedFile.name} (${formatBytes(selectedFile.size)})`;
    createRoomBtn.disabled = false;
  } else {
    fileInfo.textContent = '';
    createRoomBtn.disabled = true;
  }
});

createRoomBtn.addEventListener('click', () => {
  if (!selectedFile) return;
  // Inicializar Peer como remitente
  peer = new Peer(undefined, PEER_CONFIG); // ID aleatorio

  peer.on('open', (id) => {
    roomIdDisplay.textContent = id;
    const currentUrl = new URL(window.location.href);
    currentUrl.searchParams.set('room', id);
    roomLink.href = currentUrl.toString();
    roomLink.textContent = currentUrl.toString();
    roomInfo.style.display = 'block';
    createRoomBtn.style.display = 'none';
    fileInput.disabled = true;
    document.querySelector('.file-label').style.pointerEvents = 'none';
    sendStatus.textContent = 'Esperando al receptor...';
  });

  peer.on('connection', (connection) => {
    conn = connection;
    sendStatus.textContent = 'Receptor conectado. Iniciando transferencia...';

    conn.on('open', () => {
      // Iniciar envío del archivo en chunks
      sendFile();
    });

    conn.on('close', () => {
      sendStatus.textContent = 'Conexión cerrada.';
    });

    conn.on('error', (err) => {
      sendStatus.textContent = 'Error: ' + err;
    });
  });

  peer.on('error', (err) => {
    sendStatus.textContent = 'Error: ' + err;
  });
});

copyBtn.addEventListener('click', () => {
  navigator.clipboard.writeText(roomIdDisplay.textContent);
  copyBtn.textContent = '✅ Copiado';
  setTimeout(() => (copyBtn.textContent = '📋 Copiar'), 2000);
});

async function sendFile() {
  if (!conn || !selectedFile) return;
  const chunkSize = 16 * 1024; // 16 KB chunks
  const totalChunks = Math.ceil(selectedFile.size / chunkSize);
  let sentChunks = 0;

  sendProgress.style.display = 'block';

  // Enviar metadata primero
  conn.send({
    type: 'meta',
    name: selectedFile.name,
    size: selectedFile.size,
    totalChunks: totalChunks,
  });

  const reader = selectedFile.stream().getReader();
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      
      // Enviar chunk
      const chunkData = new Uint8Array(value);
      // Convertir a ArrayBuffer para enviar (PeerJS data channel usa strings o buffers)
      // Pero el data channel soporta ArrayBuffer directamente.
      conn.send(chunkData.buffer);
      
      sentChunks++;
      const percent = Math.floor((sentChunks / totalChunks) * 100);
      sendBarFill.style.width = percent + '%';
      sendPercent.textContent = percent + '%';
    }
    // Enviar mensaje de finalización
    conn.send({ type: 'end' });
    sendStatus.textContent = 'Archivo enviado completamente.';
  } catch (err) {
    sendStatus.textContent = 'Error al enviar: ' + err;
  }
}

// ----- Panel de recepción -----
const remoteIdInput = document.getElementById('remoteIdInput');
const connectBtn = document.getElementById('connectBtn');
const receiveProgress = document.getElementById('receiveProgress');
const receiveBarFill = document.getElementById('receiveBarFill');
const receivePercent = document.getElementById('receivePercent');
const receiveStatus = document.getElementById('receiveStatus');

// Si la URL tiene un room ID, rellenarlo automáticamente
const urlParams = new URLSearchParams(window.location.search);
if (urlParams.has('room')) {
  remoteIdInput.value = urlParams.get('room');
}

let receivedChunks = [];
let fileMeta = null;
let totalReceivedChunks = 0;

connectBtn.addEventListener('click', () => {
  const remoteId = remoteIdInput.value.trim();
  if (!remoteId) {
    receiveStatus.textContent = 'Por favor ingresa un ID o enlace.';
    return;
  }

  // Extraer ID si es una URL completa
  let peerId = remoteId;
  try {
    const url = new URL(remoteId);
    const roomParam = url.searchParams.get('room');
    if (roomParam) peerId = roomParam;
  } catch (e) { /* no es URL, usar el valor como ID */ }

  peer = new Peer(undefined, PEER_CONFIG);

  peer.on('open', (id) => {
    receiveStatus.textContent = 'Conectando...';
    conn = peer.connect(peerId, { reliable: true });

    conn.on('open', () => {
      receiveStatus.textContent = 'Conexión establecida. Esperando archivo...';
      connectBtn.disabled = true;
      remoteIdInput.disabled = true;
    });

    conn.on('data', (data) => {
      // Distinguir entre metadata, chunk binario, o señal de fin
      if (data instanceof ArrayBuffer) {
        // Chunk de archivo
        receivedChunks.push(new Uint8Array(data));
        totalReceivedChunks++;
        if (fileMeta) {
          const percent = Math.floor((totalReceivedChunks / fileMeta.totalChunks) * 100);
          receiveBarFill.style.width = percent + '%';
          receivePercent.textContent = percent + '%';
        }
      } else if (data && typeof data === 'object') {
        if (data.type === 'meta') {
          // Metadata del archivo
          fileMeta = data;
          receivedChunks = [];
          totalReceivedChunks = 0;
          receiveProgress.style.display = 'block';
          receiveStatus.textContent = `Recibiendo: ${data.name} (${formatBytes(data.size)})`;
        } else if (data.type === 'end') {
          // Finalización: reconstruir archivo
          receiveStatus.textContent = 'Archivo recibido. Guardando...';
          const blob = new Blob(receivedChunks);
          const url = URL.createObjectURL(blob);
          const a = document.createElement('a');
          a.href = url;
          a.download = fileMeta.name;
          document.body.appendChild(a);
          a.click();
          document.body.removeChild(a);
          URL.revokeObjectURL(url);
          receiveStatus.textContent = 'Descarga completada.';
          // Cerrar conexión
          conn.close();
          peer.destroy();
        }
      }
    });

    conn.on('close', () => {
      receiveStatus.textContent = 'Conexión cerrada.';
    });

    conn.on('error', (err) => {
      receiveStatus.textContent = 'Error: ' + err;
    });
  });

  peer.on('error', (err) => {
    receiveStatus.textContent = 'Error: ' + err;
  });
});
