const PEER_CONFIG = {
  host: '0.peerjs.com',
  port: 443,
  secure: true,
};

let peer;
let conn;
let selectedFile;

function formatBytes(bytes) {
  if (bytes === 0) return '0 Bytes';
  const k = 1024;
  const sizes = ['Bytes', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

// ----- Envío -----
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

  peer = new Peer(undefined, PEER_CONFIG);

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
    sendStatus.textContent = 'Receptor conectado. Preparando transferencia...';

    conn.on('open', () => {
      sendFile().catch(err => {
        sendStatus.textContent = 'Error: ' + err;
      });
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

// Función que espera a que el buffer del canal se vacíe lo suficiente
function waitForBufferDrain(dataChannel, threshold = 65536) {
  return new Promise(resolve => {
    if (dataChannel.bufferedAmount <= threshold) {
      resolve();
      return;
    }
    const onBufferedAmountLow = () => {
      dataChannel.removeEventListener('bufferedamountlow', onBufferedAmountLow);
      resolve();
    };
    dataChannel.addEventListener('bufferedamountlow', onBufferedAmountLow);
    // Ajustar el umbral para que el evento se dispare cuando bajemos de ese valor
    dataChannel.bufferedAmountLowThreshold = threshold;
  });
}

async function sendFile() {
  if (!conn || !selectedFile) return;

  const dataChannel = conn.dataChannel; // Canal subyacente
  const CHUNK_SIZE = 16 * 1024;         // 16 KB
  const BUFFER_LIMIT = 256 * 1024;      // Pausar si el buffer supera 256 KB

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

  const reader = selectedFile.stream().getReader();

  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;

      // Crear copia exacta del chunk para evitar enviar buffers inflados
      const chunk = new Uint8Array(value);
      dataChannel.send(chunk.buffer);
      sentChunks++;

      // Actualizar progreso
      const percent = Math.floor((sentChunks / totalChunks) * 100);
      sendBarFill.style.width = percent + '%';
      sendPercent.textContent = percent + '%';

      // Control de backpressure: esperar si el buffer está muy lleno
      if (dataChannel.bufferedAmount > BUFFER_LIMIT) {
        await waitForBufferDrain(dataChannel, BUFFER_LIMIT / 2);
      }
    }

    // Enviar mensaje de finalización
    conn.send({ type: 'end' });
    sendStatus.textContent = '✅ Archivo enviado completamente.';
  } catch (err) {
    sendStatus.textContent = 'Error al enviar: ' + err;
    // Intentar notificar al receptor del error (opcional)
    try { conn.send({ type: 'error', message: err.toString() }); } catch(e) {}
  }
}

// ----- Recepción -----
const remoteIdInput = document.getElementById('remoteIdInput');
const connectBtn = document.getElementById('connectBtn');
const receiveProgress = document.getElementById('receiveProgress');
const receiveBarFill = document.getElementById('receiveBarFill');
const receivePercent = document.getElementById('receivePercent');
const receiveStatus = document.getElementById('receiveStatus');

const urlParams = new URLSearchParams(window.location.search);
if (urlParams.has('room')) {
  remoteIdInput.value = urlParams.get('room');
}

let receivedChunks = [];
let fileMeta = null;
let totalReceivedChunks = 0;

// Manejador para escritura en disco (archivos muy grandes)
let fileWriter = null;
let writableStream = null;

async function saveToDisk(chunk) {
  if (!writableStream) {
    // Primera vez: pedir al usuario dónde guardar
    const handle = await window.showSaveFilePicker({
      suggestedName: fileMeta.name,
      types: [{
        description: 'Archivo',
        accept: { 'application/octet-stream': ['.' + (fileMeta.name.split('.').pop() || 'bin')] }
      }]
    });
    writableStream = await handle.createWritable();
  }
  await writableStream.write(chunk);
}

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

  peer = new Peer(undefined, PEER_CONFIG);

  peer.on('open', () => {
    receiveStatus.textContent = 'Conectando...';
    conn = peer.connect(peerId, { reliable: true });

    conn.on('open', () => {
      receiveStatus.textContent = 'Conexión establecida. Esperando archivo...';
      connectBtn.disabled = true;
      remoteIdInput.disabled = true;
    });

    conn.on('data', async (data) => {
      // Chunk binario
      if (data instanceof ArrayBuffer) {
        const chunk = new Uint8Array(data);
        receivedChunks.push(chunk);       // respaldo en RAM para archivos no enormes
        totalReceivedChunks++;
        if (fileMeta && totalReceivedChunks <= fileMeta.totalChunks) {
          const percent = Math.floor((totalReceivedChunks / fileMeta.totalChunks) * 100);
          receiveBarFill.style.width = percent + '%';
          receivePercent.textContent = percent + '%';
        }
        // Si ya tenemos escritor en disco (archivo grande), escribir incrementalmente
        if (writableStream) {
          await writableStream.write(chunk);
        }
        return;
      }

      // Mensajes de control (objetos)
      if (data && typeof data === 'object') {
        if (data.type === 'meta') {
          fileMeta = data;
          receivedChunks = [];
          totalReceivedChunks = 0;
          receiveProgress.style.display = 'block';
          receiveStatus.textContent = `Recibiendo: ${data.name} (${formatBytes(data.size)})`;
          // Si el archivo supera los 500 MB, intentar usar escritura en disco
          if (data.size > 500 * 1024 * 1024 && window.showSaveFilePicker) {
            try {
              await saveToDisk(null); // iniciar el flujo
              receiveStatus.textContent += ' (guardando directamente en disco)';
            } catch (e) {
              // El usuario canceló o el navegador no soporta, caer en Blob
              writableStream = null;
            }
          }
        } else if (data.type === 'end') {
          receiveStatus.textContent = 'Archivo recibido. Finalizando...';
          // Si usamos escritura en disco, cerramos el stream
          if (writableStream) {
            await writableStream.close();
            receiveStatus.textContent = '✅ Descarga completada.';
          } else {
            // Método clásico con Blob
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
          }
          conn.close();
          peer.destroy();
        } else if (data.type === 'error') {
          receiveStatus.textContent = 'Error del remitente: ' + data.message;
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
