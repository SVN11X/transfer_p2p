// Transferencia P2P con PeerJS + WebRTC
// Versión corregida: mensajes tipados, control de buffer real y descarga sin revocar el Blob antes de tiempo.

const PEER_CONFIG = {
  host: '0.peerjs.com',
  port: 443,
  secure: true,
  debug: 2,
  config: {
    iceServers: [
      { urls: 'stun:stun.l.google.com:19302' },
      { urls: 'stun:global.stun.twilio.com:3478' }
    ]
  }
};

const CHUNK_SIZE = 64 * 1024; // 64 KB
const MAX_BUFFERED_AMOUNT = 8 * 1024 * 1024; // Pausa si hay más de 8 MB en cola
const LOW_BUFFERED_AMOUNT = 2 * 1024 * 1024; // Retoma cuando baja a 2 MB

let peer = null;
let conn = null;
let selectedFile = null;
let sendCompleted = false;
let receiveCompleted = false;
let receiverReady = false;
let receiverReadyResolver = null;

// ---------- Utilidades ----------
function formatBytes(bytes) {
  if (bytes === 0) return '0 Bytes';
  const k = 1024;
  const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return `${parseFloat((bytes / Math.pow(k, i)).toFixed(2))} ${sizes[i]}`;
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function safeCloseCurrentPeer() {
  try {
    if (conn && conn.open) conn.close();
  } catch (e) {
    console.warn('No se pudo cerrar la conexión anterior:', e);
  }

  try {
    if (peer && !peer.destroyed) peer.destroy();
  } catch (e) {
    console.warn('No se pudo destruir el peer anterior:', e);
  }

  conn = null;
  peer = null;
}

function extractPeerId(input) {
  const value = input.trim();
  try {
    const url = new URL(value);
    return url.searchParams.get('room') || value;
  } catch (e) {
    return value;
  }
}

async function normalizeBinary(payload) {
  if (payload instanceof ArrayBuffer) return payload;

  if (payload instanceof Blob) {
    return await payload.arrayBuffer();
  }

  if (ArrayBuffer.isView(payload)) {
    return payload.buffer.slice(payload.byteOffset, payload.byteOffset + payload.byteLength);
  }

  return null;
}

async function waitForLowBuffer(connection) {
  const dc = connection?.dataChannel;
  if (!dc) {
    await sleep(5);
    return;
  }

  if (dc.bufferedAmount <= MAX_BUFFERED_AMOUNT) return;

  dc.bufferedAmountLowThreshold = LOW_BUFFERED_AMOUNT;

  while (connection.open && dc.readyState === 'open' && dc.bufferedAmount > LOW_BUFFERED_AMOUNT) {
    await new Promise(resolve => {
      let resolved = false;

      const finish = () => {
        if (resolved) return;
        resolved = true;
        clearTimeout(timer);
        dc.removeEventListener('bufferedamountlow', finish);
        resolve();
      };

      const timer = setTimeout(finish, 80);
      dc.addEventListener('bufferedamountlow', finish, { once: true });
    });
  }
}

function downloadBlob(blob, filename) {
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename || 'archivo_recibido';
  a.style.display = 'none';
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);

  // No revocar de inmediato: algunos navegadores todavía están iniciando la descarga.
  setTimeout(() => URL.revokeObjectURL(url), 60_000);
}

function setSendProgress(sentBytes, totalBytes) {
  const percent = totalBytes === 0 ? 100 : Math.min(100, Math.floor((sentBytes / totalBytes) * 100));
  sendBarFill.style.width = `${percent}%`;
  sendPercent.textContent = `${percent}%`;
}

function setReceiveProgress(receivedBytes, totalBytes) {
  const percent = totalBytes === 0 ? 100 : Math.min(100, Math.floor((receivedBytes / totalBytes) * 100));
  receiveBarFill.style.width = `${percent}%`;
  receivePercent.textContent = `${percent}%`;
}

// ---------- Elementos del panel de envío ----------
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

// ---------- Elementos del panel de recepción ----------
const remoteIdInput = document.getElementById('remoteIdInput');
const connectBtn = document.getElementById('connectBtn');
const receiveProgress = document.getElementById('receiveProgress');
const receiveBarFill = document.getElementById('receiveBarFill');
const receivePercent = document.getElementById('receivePercent');
const receiveStatus = document.getElementById('receiveStatus');

// Rellenar ID desde URL si existe.
const urlParams = new URLSearchParams(window.location.search);
if (urlParams.has('room')) {
  remoteIdInput.value = urlParams.get('room');
}

// ---------- Envío ----------
fileInput.addEventListener('change', (event) => {
  selectedFile = event.target.files[0] || null;

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

  safeCloseCurrentPeer();
  sendCompleted = false;
  receiverReady = false;
  receiverReadyResolver = null;

  sendProgress.style.display = 'none';
  sendBarFill.style.width = '0%';
  sendPercent.textContent = '0%';
  sendStatus.textContent = 'Creando sala de envío...';

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
    sendStatus.textContent = 'Receptor conectado. Preparando transferencia...';
    console.log('Emisor: Conexión entrante establecida');

    conn.on('data', (message) => {
      if (message && typeof message === 'object' && message.type === 'ready') {
        receiverReady = true;
        if (receiverReadyResolver) receiverReadyResolver();
      }
    });

    conn.on('open', () => {
      console.log('Emisor: DataChannel abierto');
      sendFile().catch(error => {
        console.error('Emisor: Error en envío:', error);
        sendStatus.textContent = `Error en envío: ${error.message || error}`;
      });
    });

    conn.on('close', () => {
      console.log('Emisor: DataChannel cerrado');
      if (!sendCompleted) {
        sendStatus.textContent = 'Conexión cerrada antes de terminar la transferencia.';
      }
    });

    conn.on('error', (error) => {
      console.error('Emisor: Error en conexión:', error);
      sendStatus.textContent = `Error de conexión: ${error.message || error}`;
    });
  });

  peer.on('error', (error) => {
    console.error('Emisor: Error en Peer:', error);
    sendStatus.textContent = `Error PeerJS: ${error.message || error}`;
  });
});

copyBtn.addEventListener('click', async () => {
  try {
    await navigator.clipboard.writeText(roomIdDisplay.textContent);
    copyBtn.textContent = '✅ Copiado';
  } catch (e) {
    copyBtn.textContent = 'Copia manualmente';
  }

  setTimeout(() => (copyBtn.textContent = '📋 Copiar'), 2000);
});

async function waitForReceiverReady(timeoutMs = 30_000) {
  if (receiverReady) return;

  await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('El receptor no confirmó que está listo.')), timeoutMs);

    receiverReadyResolver = () => {
      clearTimeout(timer);
      resolve();
    };
  });
}

async function sendFile() {
  if (!conn || !selectedFile) return;

  const totalChunks = Math.ceil(selectedFile.size / CHUNK_SIZE);
  let sentBytes = 0;

  sendProgress.style.display = 'block';
  setSendProgress(0, selectedFile.size);

  conn.send({
    type: 'meta',
    name: selectedFile.name,
    size: selectedFile.size,
    totalChunks,
    chunkSize: CHUNK_SIZE
  });

  console.log('Emisor: Metadata enviada');
  sendStatus.textContent = 'Esperando confirmación del receptor...';
  await waitForReceiverReady();

  sendStatus.textContent = 'Enviando archivo...';

  for (let index = 0; index < totalChunks; index++) {
    if (!conn.open) throw new Error('La conexión se cerró durante el envío.');

    const start = index * CHUNK_SIZE;
    const end = Math.min(start + CHUNK_SIZE, selectedFile.size);
    const payload = await selectedFile.slice(start, end).arrayBuffer();

    conn.send({
      type: 'chunk',
      index,
      size: payload.byteLength,
      payload
    });

    sentBytes += payload.byteLength;
    setSendProgress(sentBytes, selectedFile.size);
    await waitForLowBuffer(conn);

    if (index % 50 === 0) await sleep(0);
  }

  conn.send({ type: 'end' });
  sendCompleted = true;
  setSendProgress(selectedFile.size, selectedFile.size);
  sendStatus.textContent = '✅ Archivo enviado completamente.';
  console.log('Emisor: Transferencia finalizada');
}

// ---------- Recepción ----------
let receivedChunks = [];
let fileMeta = null;
let receivedBytes = 0;
let pendingChunkTasks = [];
let legacyBinaryIndex = 0;

connectBtn.addEventListener('click', () => {
  const rawRemoteId = remoteIdInput.value.trim();
  if (!rawRemoteId) {
    receiveStatus.textContent = 'Por favor ingresa un ID o enlace.';
    return;
  }

  const peerId = extractPeerId(rawRemoteId);

  safeCloseCurrentPeer();
  receiveCompleted = false;
  receivedChunks = [];
  fileMeta = null;
  receivedBytes = 0;
  pendingChunkTasks = [];
  legacyBinaryIndex = 0;

  receiveProgress.style.display = 'none';
  receiveBarFill.style.width = '0%';
  receivePercent.textContent = '0%';
  receiveStatus.textContent = 'Creando conexión...';

  peer = new Peer(undefined, PEER_CONFIG);

  peer.on('open', (id) => {
    console.log('Receptor: Peer abierto con ID', id);
    receiveStatus.textContent = 'Conectando con el emisor...';

    conn = peer.connect(peerId, {
      reliable: true,
      serialization: 'binary'
    });

    conn.on('open', () => {
      console.log('Receptor: DataChannel abierto');
      receiveStatus.textContent = 'Conexión establecida. Esperando archivo...';
      connectBtn.disabled = true;
      remoteIdInput.disabled = true;
    });

    conn.on('data', (data) => {
      handleIncomingData(data).catch(error => {
        console.error('Receptor: Error procesando datos:', error);
        receiveStatus.textContent = `Error recibiendo archivo: ${error.message || error}`;
      });
    });

    conn.on('close', () => {
      console.log('Receptor: DataChannel cerrado');
      if (!receiveCompleted) {
        receiveStatus.textContent = 'Conexión cerrada antes de completar la descarga.';
      }
    });

    conn.on('error', (error) => {
      console.error('Receptor: Error en conexión:', error);
      receiveStatus.textContent = `Error de conexión: ${error.message || error}`;
    });
  });

  peer.on('error', (error) => {
    console.error('Receptor: Error en Peer:', error);
    receiveStatus.textContent = `Error PeerJS: ${error.message || error}`;
  });
});

async function handleIncomingData(data) {
  if (data && typeof data === 'object' && data.type === 'meta') {
    fileMeta = data;
    receivedChunks = new Array(data.totalChunks);
    receivedBytes = 0;
    pendingChunkTasks = [];
    legacyBinaryIndex = 0;

    receiveProgress.style.display = 'block';
    setReceiveProgress(0, data.size);
    receiveStatus.textContent = `Recibiendo: ${data.name} (${formatBytes(data.size)})`;

    // Confirmación explícita para que el emisor no dispare chunks antes de que el receptor esté listo.
    conn.send({ type: 'ready' });
    console.log('Receptor: Metadata recibida y confirmada', data);
    return;
  }

  if (data && typeof data === 'object' && data.type === 'chunk') {
    const task = receiveChunk(data.index, data.payload, data.size);
    pendingChunkTasks.push(task);
    await task;
    return;
  }

  if (data && typeof data === 'object' && data.type === 'end') {
    console.log('Receptor: Señal de fin recibida');
    await finishReceivingFile();
    return;
  }

  // Compatibilidad con versiones anteriores que enviaban binario sin envolver en objeto.
  const binary = await normalizeBinary(data);
  if (binary && fileMeta) {
    receivedChunks[legacyBinaryIndex] = binary;
    legacyBinaryIndex++;
    receivedBytes += binary.byteLength;
    setReceiveProgress(receivedBytes, fileMeta.size);
  }
}

async function receiveChunk(index, payload, declaredSize) {
  if (!fileMeta) throw new Error('Llegó un chunk antes de la metadata.');

  const binary = await normalizeBinary(payload);
  if (!binary) throw new Error(`Chunk ${index} no llegó como dato binario válido.`);

  receivedChunks[index] = binary;
  receivedBytes += declaredSize || binary.byteLength;
  setReceiveProgress(receivedBytes, fileMeta.size);
}

async function finishReceivingFile() {
  if (!fileMeta) throw new Error('No hay metadata del archivo recibido.');

  receiveStatus.textContent = 'Archivo recibido. Preparando descarga...';
  await Promise.all(pendingChunkTasks);

  const missingChunks = [];
  for (let i = 0; i < fileMeta.totalChunks; i++) {
    if (!receivedChunks[i]) missingChunks.push(i);
  }

  if (missingChunks.length > 0) {
    throw new Error(`Faltan ${missingChunks.length} partes del archivo. Primera parte faltante: ${missingChunks[0]}.`);
  }

  const blob = new Blob(receivedChunks, { type: 'application/octet-stream' });

  if (blob.size !== fileMeta.size) {
    throw new Error(`Tamaño incorrecto: se esperaban ${formatBytes(fileMeta.size)} y se recibieron ${formatBytes(blob.size)}.`);
  }

  downloadBlob(blob, fileMeta.name);
  setReceiveProgress(fileMeta.size, fileMeta.size);
  receiveCompleted = true;
  receiveStatus.textContent = `✅ Descarga completada: ${fileMeta.name} (${formatBytes(blob.size)}).`;
  console.log('Receptor: Descarga completada', { name: fileMeta.name, size: blob.size });

  setTimeout(() => {
    try {
      if (conn && conn.open) conn.close();
    } catch (e) {
      console.warn('No se pudo cerrar la conexión:', e);
    }
  }, 1500);
}
