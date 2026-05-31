// Transferencia P2P con PeerJS + WebRTC
// Versión optimizada para archivos grandes:
// - Chunks de 256 KB.
// - Envío binario directo, sin envolver cada chunk en objetos.
// - Control de buffer real con RTCDataChannel.bufferedAmount.
// - Recepción compatible con memoria y guardado directo en disco si el navegador lo permite.

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

// Para más estabilidad usa 256 KB. Si tu red es muy buena, puedes probar 512 * 1024.
const CHUNK_SIZE = 256 * 1024; // 256 KB
const MAX_BUFFERED_AMOUNT = 8 * 1024 * 1024; // Pausa si hay más de 8 MB en cola
const LOW_BUFFERED_AMOUNT = 2 * 1024 * 1024; // Retoma cuando baja a 2 MB
const LARGE_FILE_THRESHOLD = 512 * 1024 * 1024; // Desde 512 MB se recomienda guardar directo en disco

let peer = null;
let conn = null;
let selectedFile = null;
let sendCompleted = false;
let receiveCompleted = false;
let receiverReady = false;
let receiverReadyResolver = null;

let sendStartTime = null;
let receiveStartTime = null;
let activeDownloadUrl = null;

// ---------- Utilidades ----------
function formatBytes(bytes) {
  if (!Number.isFinite(bytes) || bytes <= 0) return '0 Bytes';

  const k = 1024;
  const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.min(Math.floor(Math.log(bytes) / Math.log(k)), sizes.length - 1);

  return `${parseFloat((bytes / Math.pow(k, i)).toFixed(2))} ${sizes[i]}`;
}

function formatTime(seconds) {
  if (!Number.isFinite(seconds) || seconds < 0) return '--';

  const totalSeconds = Math.ceil(seconds);
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const secs = totalSeconds % 60;

  if (hours > 0) return `${hours} h ${minutes} min ${secs} s`;
  if (minutes > 0) return `${minutes} min ${secs} s`;
  return `${secs} s`;
}

function calculateTransferStats(doneBytes, totalBytes, startTime) {
  if (!startTime || doneBytes <= 0) {
    return {
      speedText: '0 MB/s',
      remainingText: '--'
    };
  }

  const elapsedSeconds = Math.max((Date.now() - startTime) / 1000, 0.001);
  const bytesPerSecond = doneBytes / elapsedSeconds;
  const remainingBytes = Math.max(totalBytes - doneBytes, 0);
  const remainingSeconds = bytesPerSecond > 0 ? remainingBytes / bytesPerSecond : Infinity;

  return {
    speedText: `${formatBytes(bytesPerSecond)}/s`,
    remainingText: formatTime(remainingSeconds)
  };
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
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

function supportsDirectDiskSave() {
  return Boolean(window.isSecureContext && window.showSaveFilePicker);
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
  if (activeDownloadUrl) {
    URL.revokeObjectURL(activeDownloadUrl);
    activeDownloadUrl = null;
  }

  const url = URL.createObjectURL(blob);
  activeDownloadUrl = url;

  const a = document.createElement('a');
  a.href = url;
  a.download = filename || 'archivo_recibido';
  a.style.display = 'none';
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);

  // No revocar de inmediato: algunos navegadores todavía están iniciando la descarga.
  setTimeout(() => {
    if (activeDownloadUrl === url) {
      URL.revokeObjectURL(url);
      activeDownloadUrl = null;
    }
  }, 60_000);
}

function setSendProgress(sentBytes, totalBytes) {
  const percent = totalBytes === 0 ? 100 : Math.min(100, Math.floor((sentBytes / totalBytes) * 100));
  const stats = calculateTransferStats(sentBytes, totalBytes, sendStartTime);

  sendBarFill.style.width = `${percent}%`;
  sendPercent.textContent = `${percent}%`;

  if (sendStartTime && sentBytes > 0 && sentBytes < totalBytes) {
    sendStatus.textContent = `Enviando archivo... ${formatBytes(sentBytes)} de ${formatBytes(totalBytes)} · ${stats.speedText} · restante: ${stats.remainingText}`;
  }
}

function setReceiveProgress(receivedBytes, totalBytes) {
  const percent = totalBytes === 0 ? 100 : Math.min(100, Math.floor((receivedBytes / totalBytes) * 100));
  const stats = calculateTransferStats(receivedBytes, totalBytes, receiveStartTime);

  receiveBarFill.style.width = `${percent}%`;
  receivePercent.textContent = `${percent}%`;

  if (receiveStartTime && receivedBytes > 0 && receivedBytes < totalBytes && fileMeta) {
    receiveStatus.textContent = `Recibiendo: ${fileMeta.name} · ${formatBytes(receivedBytes)} de ${formatBytes(totalBytes)} · ${stats.speedText} · restante: ${stats.remainingText}`;
  }
}

function clearStorageChoiceControls() {
  const oldControls = document.getElementById('storageChoiceControls');
  if (oldControls) oldControls.remove();
}

function createStorageChoiceControls(meta, onDirectDisk, onMemory) {
  clearStorageChoiceControls();

  const wrapper = document.createElement('div');
  wrapper.id = 'storageChoiceControls';
  wrapper.style.marginTop = '12px';
  wrapper.style.display = 'flex';
  wrapper.style.flexDirection = 'column';
  wrapper.style.gap = '8px';

  const info = document.createElement('p');
  info.style.margin = '0';
  info.textContent = `Archivo entrante: ${meta.name} (${formatBytes(meta.size)}).`;
  wrapper.appendChild(info);

  if (supportsDirectDiskSave()) {
    const diskButton = document.createElement('button');
    diskButton.type = 'button';
    diskButton.textContent = 'Guardar directo en disco recomendado';
    diskButton.addEventListener('click', onDirectDisk);
    wrapper.appendChild(diskButton);
  }

  const memoryButton = document.createElement('button');
  memoryButton.type = 'button';
  memoryButton.textContent = supportsDirectDiskSave()
    ? 'Usar descarga normal consume más memoria'
    : 'Continuar con descarga normal';
  memoryButton.addEventListener('click', onMemory);
  wrapper.appendChild(memoryButton);

  receiveStatus.insertAdjacentElement('afterend', wrapper);
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
fileInput.addEventListener('change', event => {
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
  sendStartTime = null;

  sendProgress.style.display = 'none';
  sendBarFill.style.width = '0%';
  sendPercent.textContent = '0%';
  sendStatus.textContent = 'Creando sala de envío...';

  peer = new Peer(undefined, PEER_CONFIG);

  peer.on('open', id => {
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

  peer.on('connection', connection => {
    conn = connection;
    sendStatus.textContent = 'Receptor conectado. Preparando transferencia...';
    console.log('Emisor: Conexión entrante establecida');

    conn.on('data', message => {
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

    conn.on('error', error => {
      console.error('Emisor: Error en conexión:', error);
      sendStatus.textContent = `Error de conexión: ${error.message || error}`;
    });
  });

  peer.on('error', error => {
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

async function waitForReceiverReady(timeoutMs = 5 * 60 * 1000) {
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

  sendStartTime = Date.now();
  sendStatus.textContent = 'Enviando archivo...';

  for (let index = 0; index < totalChunks; index++) {
    if (!conn.open) throw new Error('La conexión se cerró durante el envío.');

    const start = index * CHUNK_SIZE;
    const end = Math.min(start + CHUNK_SIZE, selectedFile.size);
    const payload = await selectedFile.slice(start, end).arrayBuffer();

    // Envío binario directo: más liviano que envolver cada chunk en un objeto.
    conn.send(payload);

    sentBytes += payload.byteLength;
    setSendProgress(sentBytes, selectedFile.size);
    await waitForLowBuffer(conn);

    // Cede control al navegador cada cierto número de chunks para evitar congelamientos.
    if (index % 50 === 0) await sleep(0);
  }

  await waitForLowBuffer(conn);
  conn.send({ type: 'end' });

  sendCompleted = true;
  setSendProgress(selectedFile.size, selectedFile.size);
  sendStatus.textContent = `✅ Archivo enviado completamente: ${selectedFile.name} (${formatBytes(selectedFile.size)}).`;
  console.log('Emisor: Transferencia finalizada');
}

// ---------- Recepción ----------
let receivedChunks = [];
let fileMeta = null;
let receivedBytes = 0;
let nextChunkIndex = 0;
let receiveQueue = Promise.resolve();
let receiveMode = 'memory';
let fileWriter = null;

connectBtn.addEventListener('click', () => {
  const rawRemoteId = remoteIdInput.value.trim();

  if (!rawRemoteId) {
    receiveStatus.textContent = 'Por favor ingresa un ID o enlace.';
    return;
  }

  const peerId = extractPeerId(rawRemoteId);

  safeCloseCurrentPeer();
  resetReceivingState({ abortWriter: true });

  receiveCompleted = false;
  receiveStartTime = null;
  receiveQueue = Promise.resolve();

  receiveProgress.style.display = 'none';
  receiveBarFill.style.width = '0%';
  receivePercent.textContent = '0%';
  receiveStatus.textContent = 'Creando conexión...';
  clearStorageChoiceControls();

  peer = new Peer(undefined, PEER_CONFIG);

  peer.on('open', id => {
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

    conn.on('data', data => {
      receiveQueue = receiveQueue
        .then(() => handleIncomingData(data))
        .catch(error => {
          console.error('Receptor: Error procesando datos:', error);
          receiveStatus.textContent = `Error recibiendo archivo: ${error.message || error}`;
          resetReceivingState({ abortWriter: true });
        });
    });

    conn.on('close', () => {
      console.log('Receptor: DataChannel cerrado');
      if (!receiveCompleted) {
        receiveStatus.textContent = 'Conexión cerrada antes de completar la descarga.';
        resetReceivingState({ abortWriter: true });
      }
    });

    conn.on('error', error => {
      console.error('Receptor: Error en conexión:', error);
      receiveStatus.textContent = `Error de conexión: ${error.message || error}`;
      resetReceivingState({ abortWriter: true });
    });
  });

  peer.on('error', error => {
    console.error('Receptor: Error en Peer:', error);
    receiveStatus.textContent = `Error PeerJS: ${error.message || error}`;
    resetReceivingState({ abortWriter: true });
  });
});

async function handleIncomingData(data) {
  if (data && typeof data === 'object' && data.type === 'meta') {
    await prepareReceivingFile(data);
    return;
  }

  if (data && typeof data === 'object' && data.type === 'chunk') {
    // Compatibilidad con versiones anteriores que enviaban chunks dentro de objetos.
    await receiveBinaryChunk(data.payload, data.index, data.size);
    return;
  }

  if (data && typeof data === 'object' && data.type === 'end') {
    console.log('Receptor: Señal de fin recibida');
    await finishReceivingFile();
    return;
  }

  // Versión optimizada: los chunks llegan como ArrayBuffer directo.
  const binary = await normalizeBinary(data);

  if (binary && fileMeta) {
    await receiveBinaryChunk(binary, nextChunkIndex, binary.byteLength);
    nextChunkIndex += 1;
    return;
  }

  console.warn('Receptor: Mensaje no reconocido', data);
}

async function prepareReceivingFile(meta) {
  fileMeta = meta;
  receivedBytes = 0;
  nextChunkIndex = 0;
  receiveStartTime = null;
  clearStorageChoiceControls();

  receiveProgress.style.display = 'block';
  setReceiveProgress(0, meta.size);

  const shouldAskForStorage = meta.size >= LARGE_FILE_THRESHOLD;

  if (shouldAskForStorage) {
    receiveStatus.textContent = supportsDirectDiskSave()
      ? 'Archivo grande detectado. Elige cómo guardarlo para iniciar la transferencia.'
      : 'Archivo grande detectado. Este navegador no permite guardado directo en disco; se usará descarga normal.';

    await chooseReceivingMode(meta);
  } else {
    receiveMode = 'memory';
    receivedChunks = new Array(meta.totalChunks);
  }

  receiveStartTime = Date.now();
  receiveStatus.textContent = `Listo para recibir: ${meta.name} (${formatBytes(meta.size)})`;

  // Confirmación explícita para que el emisor no dispare chunks antes de que el receptor esté listo.
  conn.send({ type: 'ready' });
  console.log('Receptor: Metadata recibida y confirmada', meta, 'modo:', receiveMode);
}

function chooseReceivingMode(meta) {
  return new Promise(resolve => {
    const useMemory = () => {
      receiveMode = 'memory';
      receivedChunks = new Array(meta.totalChunks);
      fileWriter = null;
      clearStorageChoiceControls();
      resolve();
    };

    const useDirectDisk = async () => {
      try {
        const handle = await window.showSaveFilePicker({
          suggestedName: meta.name || 'archivo_recibido'
        });

        fileWriter = await handle.createWritable();
        receiveMode = 'disk';
        receivedChunks = [];
        clearStorageChoiceControls();
        resolve();
      } catch (error) {
        if (error?.name === 'AbortError') {
          receiveStatus.textContent = 'No se eligió ubicación. Elige una opción para iniciar la transferencia.';
          return;
        }

        console.error('No se pudo usar guardado directo:', error);
        receiveStatus.textContent = 'No se pudo usar guardado directo. Puedes continuar con descarga normal.';
      }
    };

    if (!supportsDirectDiskSave()) {
      useMemory();
      return;
    }

    createStorageChoiceControls(meta, useDirectDisk, useMemory);
  });
}

async function receiveBinaryChunk(payload, index, declaredSize) {
  if (!fileMeta) throw new Error('Llegó un chunk antes de la metadata.');

  const binary = await normalizeBinary(payload);
  if (!binary) throw new Error(`Chunk ${index} no llegó como dato binario válido.`);

  if (receiveMode === 'disk') {
    if (!fileWriter) throw new Error('No existe escritor de archivo para guardar en disco.');
    await fileWriter.write(binary);
  } else {
    receivedChunks[index] = binary;
  }

  receivedBytes += declaredSize || binary.byteLength;
  setReceiveProgress(receivedBytes, fileMeta.size);
}

async function finishReceivingFile() {
  if (!fileMeta) throw new Error('No hay metadata del archivo recibido.');

  receiveStatus.textContent = 'Archivo recibido. Verificando integridad...';

  if (receivedBytes !== fileMeta.size) {
    throw new Error(`Tamaño incorrecto: se esperaban ${formatBytes(fileMeta.size)} y se recibieron ${formatBytes(receivedBytes)}.`);
  }

  if (receiveMode === 'disk') {
    if (!fileWriter) throw new Error('No existe escritor de archivo para finalizar el guardado.');

    await fileWriter.close();
    fileWriter = null;

    setReceiveProgress(fileMeta.size, fileMeta.size);
    receiveCompleted = true;
    receiveStatus.textContent = `✅ Archivo guardado completamente: ${fileMeta.name} (${formatBytes(fileMeta.size)}).`;
    console.log('Receptor: Archivo guardado en disco', { name: fileMeta.name, size: fileMeta.size });
  } else {
    receiveStatus.textContent = 'Archivo recibido. Preparando descarga...';

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

    // Se liberan referencias a chunks después de iniciar la descarga.
    receivedChunks = [];
  }

  setTimeout(() => {
    try {
      if (conn && conn.open) conn.close();
    } catch (e) {
      console.warn('No se pudo cerrar la conexión:', e);
    }
  }, 1500);
}

function resetReceivingState({ abortWriter = false } = {}) {
  if (abortWriter && fileWriter) {
    try {
      fileWriter.abort();
    } catch (e) {
      console.warn('No se pudo abortar el escritor de archivo:', e);
    }
  }

  receivedChunks = [];
  fileMeta = null;
  receivedBytes = 0;
  nextChunkIndex = 0;
  receiveMode = 'memory';
  fileWriter = null;
  clearStorageChoiceControls();
}

window.addEventListener('beforeunload', () => {
  try {
    if (activeDownloadUrl) URL.revokeObjectURL(activeDownloadUrl);
  } catch (e) {
    console.warn('No se pudo liberar la URL temporal:', e);
  }

  try {
    if (conn && conn.open) conn.close();
  } catch (e) {
    console.warn('No se pudo cerrar la conexión al salir:', e);
  }

  try {
    if (peer && !peer.destroyed) peer.destroy();
  } catch (e) {
    console.warn('No se pudo destruir el peer al salir:', e);
  }
});
