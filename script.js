// Configuración de PeerJS (servidor público gratuito)
const PEER_CONFIG = {
  host: '0.peerjs.com',
  port: 443,
  secure: true,
  debug: 1,        // 1 = errores, 2 = warnings, 3 = todos los logs
};

let peer;
let conn;
let selectedFile;

// Utilidad: formato de bytes
function formatBytes(bytes) {
  if (bytes === 0) return '0 Bytes';
  const k = 1024;
  const sizes = ['Bytes', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

// ---------- PANEL DE ENVÍO ----------
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

  // Destruir instancia previa si existe
  if (peer) peer.destroy();
  peer = new Peer(undefined, PEER_CONFIG);

  peer.on('open', (id) => {
    console.log('Emisor: Peer abierto con ID', id);
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
    console.log('Emisor: Conexión entrante establecida');

    conn.on('open', () => {
      console.log('Emisor: DataChannel abierto, comenzando envío');
      sendFile().catch(err => {
        console.error('Error en envío:', err);
        sendStatus.textContent = 'Error: ' + err;
      });
    });

    conn.on('close', () => {
      console.log('Emisor: DataChannel cerrado');
      sendStatus.textContent = 'Conexión cerrada.';
    });

    conn.on('error', (err) => {
      console.error('Emisor: Error en conexión:', err);
      sendStatus.textContent = 'Error: ' + err;
    });
  });

  peer.on('error', (err) => {
    console.error('Emisor: Error en Peer:', err);
    sendStatus.textContent = 'Error: ' + err;
  });
});

copyBtn.addEventListener('click', () => {
  navigator.clipboard.writeText(roomIdDisplay.textContent);
  copyBtn.textContent = '✅ Copiado';
  setTimeout(() => (copyBtn.textContent = '📋 Copiar'), 2000);
});

// Función de envío con control de flujo sencillo basado en setTimeout
async function sendFile() {
  if (!conn || !selectedFile) return;

  const CHUNK_SIZE = 16 * 1024; // 16 KB
  const totalChunks = Math.ceil(selectedFile.size / CHUNK_SIZE);
  let sentChunks = 0;

  sendProgress.style.display = 'block';

  // Enviar metadata
  conn.send({
    type: 'meta',
    name: selectedFile.name,
    size: selectedFile.size,
    totalChunks: totalChunks,
  });
  console.log('Emisor: Metadata enviada');

  // Leer archivo como ArrayBuffer para evitar problemas de stream
  const arrayBuffer = await selectedFile.arrayBuffer();
  const fileData = new Uint8Array(arrayBuffer);

  // Función auxiliar que espera a que el buffer baje de cierto umbral
  const waitBuffer = () => {
    return new Promise(resolve => {
      if (conn.bufferedAmount <= 65536) { // 64 KB
        resolve();
        return;
      }
      const check = setInterval(() => {
        if (conn.bufferedAmount <= 65536) {
          clearInterval(check);
          resolve();
        }
      }, 10);
    });
  };

  for (let i = 0; i < totalChunks; i++) {
    const start = i * CHUNK_SIZE;
    const end = Math.min(start + CHUNK_SIZE, fileData.length);
    const chunk = fileData.slice(start, end);

    // Enviar chunk (PeerJS acepta Uint8Array/ArrayBuffer)
    conn.send(chunk.buffer);
    sentChunks++;

    const percent = Math.floor((sentChunks / totalChunks) * 100);
    sendBarFill.style.width = percent + '%';
    sendPercent.textContent = percent + '%';

    // Control de backpressure: si el buffer crece mucho, esperar
    if (conn.bufferedAmount > 1048576) { // 1 MB
      await waitBuffer();
    }

    // Pequeña pausa entre chunks para dejar respirar al navegador (opcional pero ayuda)
    if (i % 100 === 0) {
      await new Promise(r => setTimeout(r, 0));
    }
  }

  // Enviar señal de fin
  conn.send({ type: 'end' });
  console.log('Emisor: Fin enviado');
  sendStatus.textContent = '✅ Archivo enviado completamente.';
}

// ---------- PANEL DE RECEPCIÓN ----------
const remoteIdInput = document.getElementById('remoteIdInput');
const connectBtn = document.getElementById('connectBtn');
const receiveProgress = document.getElementById('receiveProgress');
const receiveBarFill = document.getElementById('receiveBarFill');
const receivePercent = document.getElementById('receivePercent');
const receiveStatus = document.getElementById('receiveStatus');

// Rellenar ID desde URL si existe
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

  let peerId = remoteId;
  try {
    const url = new URL(remoteId);
    const roomParam = url.searchParams.get('room');
    if (roomParam) peerId = roomParam;
  } catch (e) {}

  // Destruir instancia previa
  if (peer) peer.destroy();
  peer = new Peer(undefined, PEER_CONFIG);

  peer.on('open', (id) => {
    console.log('Receptor: Peer abierto con ID', id);
    receiveStatus.textContent = 'Conectando...';
    conn = peer.connect(peerId, { reliable: true });

    conn.on('open', () => {
      console.log('Receptor: DataChannel abierto');
      receiveStatus.textContent = 'Conexión establecida. Esperando archivo...';
      connectBtn.disabled = true;
      remoteIdInput.disabled = true;
    });

    conn.on('data', (data) => {
      // Distinguir tipo de dato
      if (data instanceof ArrayBuffer) {
        // Chunk binario
        const chunk = new Uint8Array(data);
        receivedChunks.push(chunk);
        totalReceivedChunks++;
        if (fileMeta && totalReceivedChunks <= fileMeta.totalChunks) {
          const percent = Math.floor((totalReceivedChunks / fileMeta.totalChunks) * 100);
          receiveBarFill.style.width = percent + '%';
          receivePercent.textContent = percent + '%';
        }
        return;
      }

      // Mensaje de control (objeto JSON)
      if (typeof data === 'object' && data !== null) {
        if (data.type === 'meta') {
          console.log('Receptor: Metadata recibida', data);
          fileMeta = data;
          receivedChunks = [];
          totalReceivedChunks = 0;
          receiveProgress.style.display = 'block';
          receiveStatus.textContent = `Recibiendo: ${data.name} (${formatBytes(data.size)})`;
        } else if (data.type === 'end') {
          console.log('Receptor: Fin recibido');
          receiveStatus.textContent = 'Archivo recibido. Guardando...';
          // Ensamblar y descargar
          const blob = new Blob(receivedChunks);
          const url = URL.createObjectURL(blob);
          const a = document.createElement('a');
          a.href = url;
          a.download = fileMeta.name;
          document.body.appendChild(a);
          a.click();
          document.body.removeChild(a);
          URL.revokeObjectURL(url);
          receiveStatus.textContent = '✅ Descarga completada.';
          // Cerrar conexión limpiamente
          setTimeout(() => {
            if (conn) conn.close();
            if (peer) peer.destroy();
          }, 500);
        }
      }
    });

    conn.on('close', () => {
      console.log('Receptor: DataChannel cerrado');
      receiveStatus.textContent = 'Conexión cerrada.';
    });

    conn.on('error', (err) => {
      console.error('Receptor: Error en conexión:', err);
      receiveStatus.textContent = 'Error: ' + err;
    });
  });

  peer.on('error', (err) => {
    console.error('Receptor: Error en Peer:', err);
    receiveStatus.textContent = 'Error: ' + err;
  });
});
