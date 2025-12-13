const net = require('net');
const {
  MSG_WRITE,
  MSG_READ,
  MSG_READ_AND_DELETE,
  RES_OK,
  RES_ERROR,
  RES_NOT_FOUND,
  END_MARKER,
} = require('./common.cjs');

class NetCacheServer {
  /**
   * @param {object} options
   * @param {number} [options.maxLifetimeMs=60000] - Maximum lifetime of each cache entry in milliseconds
   * @param {number} [options.port=11211] - TCP port to listen on
   */
  constructor(options = {}) {
    this.maxLifetimeMs = options.maxLifetimeMs ?? 60000;
    this.port = options.port ?? 11211;

    /** @type {Map<string, { value: Buffer, timer: NodeJS.Timeout }>} */
    this.cache = new Map();

    this.server = net.createServer((socket) => this.#handleConnection(socket));
  }

  /**
   * Start listening on the configured port
   * @returns {Promise<void>}
   */
  start() {
    return new Promise((resolve, reject) => {
      this.server.once('error', reject);
      this.server.listen(this.port, '127.0.0.1', () => {
        console.log(`net-cache listening on 127.0.0.1:${this.port}`);
        resolve();
      });
    });
  }

  /**
   * Stop the server and clear the cache
   * @returns {Promise<void>}
   */
  stop() {
    return new Promise((resolve) => {
      // Clear all timers
      for (const entry of this.cache.values()) {
        clearTimeout(entry.timer);
      }
      this.cache.clear();

      this.server.close(() => {
        console.log('net-cache stopped');
        resolve();
      });
    });
  }

  /**
   * Handle incoming socket connection
   * @param {net.Socket} socket
   */
  #handleConnection(socket) {
    let buffer = Buffer.alloc(0);

    socket.on('data', (chunk) => {
      buffer = Buffer.concat([buffer, chunk]);
      buffer = this.#processBuffer(buffer, socket);
    });

    socket.on('error', (err) => {
      console.error('Socket error:', err.message);
    });
  }

  /**
   * Process the incoming buffer and handle complete messages
   * @param {Buffer} buffer
   * @param {net.Socket} socket
   * @returns {Buffer} remaining buffer
   */
  #processBuffer(buffer, socket) {
    // Minimum message size: 8 (id) + 1 (type) + 4 (keyLen) + 4 (valueLen) + 1 (end marker) = 18
    const HEADER_SIZE = 17; // bytes before payload

    while (buffer.length >= HEADER_SIZE) {
      const keyLen = buffer.readUInt32BE(9);
      const valueLen = buffer.readUInt32BE(13);
      const totalLen = HEADER_SIZE + keyLen + valueLen + 1; // +1 for end marker

      if (buffer.length < totalLen) {
        break; // wait for more data
      }

      // Check end marker
      if (buffer[totalLen - 1] !== END_MARKER) {
        // Protocol error: respond with error and discard
        const msgId = buffer.subarray(0, 8);
        this.#sendError(socket, msgId, 'Invalid end marker');
        buffer = buffer.subarray(totalLen);
        continue;
      }

      // Extract message parts
      const msgId = buffer.subarray(0, 8);
      const msgType = buffer[8];
      const key = buffer.subarray(HEADER_SIZE, HEADER_SIZE + keyLen).toString('utf8');
      const value = buffer.subarray(HEADER_SIZE + keyLen, HEADER_SIZE + keyLen + valueLen);

      // Handle message
      this.#handleMessage(socket, msgId, msgType, key, value);

      // Move past this message
      buffer = buffer.subarray(totalLen);
    }

    return buffer;
  }

  /**
   * Handle a parsed message
   * @param {net.Socket} socket
   * @param {Buffer} msgId
   * @param {number} msgType
   * @param {string} key
   * @param {Buffer} value
   */
  #handleMessage(socket, msgId, msgType, key, value) {
    switch (msgType) {
      case MSG_WRITE:
        this.#write(key, value);
        this.#sendOk(socket, msgId);
        break;

      case MSG_READ: {
        const entry = this.cache.get(key);
        if (entry) {
          this.#sendOkWithPayload(socket, msgId, entry.value);
        } else {
          this.#sendNotFound(socket, msgId);
        }
        break;
      }

      case MSG_READ_AND_DELETE: {
        const entry = this.cache.get(key);
        if (entry) {
          clearTimeout(entry.timer);
          this.cache.delete(key);
          this.#sendOkWithPayload(socket, msgId, entry.value);
        } else {
          this.#sendNotFound(socket, msgId);
        }
        break;
      }

      default:
        this.#sendError(socket, msgId, `Unknown message type: 0x${msgType.toString(16)}`);
    }
  }

  /**
   * Write a key-value pair to the cache
   * @param {string} key
   * @param {Buffer} value
   */
  #write(key, value) {
    // Clear existing timer if key exists
    const existing = this.cache.get(key);
    if (existing) {
      clearTimeout(existing.timer);
    }

    // Set expiration timer
    const timer = setTimeout(() => {
      this.cache.delete(key);
    }, this.maxLifetimeMs);

    this.cache.set(key, { value: Buffer.from(value), timer });
  }

  /**
   * Send OK response (no payload)
   * @param {net.Socket} socket
   * @param {Buffer} msgId
   */
  #sendOk(socket, msgId) {
    // 8 bytes id + 1 byte status + 4 bytes payload length (0)
    const response = Buffer.alloc(13);
    msgId.copy(response, 0);
    response[8] = RES_OK;
    response.writeUInt32BE(0, 9);
    socket.write(response);
  }

  /**
   * Send not found response (no payload)
   * @param {net.Socket} socket
   * @param {Buffer} msgId
   */
  #sendNotFound(socket, msgId) {
    const response = Buffer.alloc(13);
    msgId.copy(response, 0);
    response[8] = RES_NOT_FOUND;
    response.writeUInt32BE(0, 9);
    socket.write(response);
  }

  /**
   * Send OK response with payload
   * @param {net.Socket} socket
   * @param {Buffer} msgId
   * @param {Buffer} payload
   */
  #sendOkWithPayload(socket, msgId, payload) {
    const response = Buffer.alloc(13 + payload.length);
    msgId.copy(response, 0);
    response[8] = RES_OK;
    response.writeUInt32BE(payload.length, 9);
    payload.copy(response, 13);
    socket.write(response);
  }

  /**
   * Send error response
   * @param {net.Socket} socket
   * @param {Buffer} msgId
   * @param {string} reason
   */
  #sendError(socket, msgId, reason) {
    const reasonBuf = Buffer.from(reason, 'utf8');
    const response = Buffer.alloc(13 + reasonBuf.length);
    msgId.copy(response, 0);
    response[8] = RES_ERROR;
    response.writeUInt32BE(reasonBuf.length, 9);
    reasonBuf.copy(response, 13);
    socket.write(response);
  }
}

class NetCacheClient {
  /**
   * @param {object} options
   * @param {string} [options.host='127.0.0.1'] - Server host
   * @param {number} [options.port=11211] - Server port
   * @param {number} [options.reconnectDelayMs=1000] - Delay before reconnect attempt
   */
  constructor(options = {}) {
    this.host = options.host ?? '127.0.0.1';
    this.port = options.port ?? 11211;
    this.reconnectDelayMs = options.reconnectDelayMs ?? 1000;

    /** @type {net.Socket | null} */
    this.socket = null;
    this.connected = false;
    this.shouldReconnect = true;

    /** @type {bigint} */
    this.nextId = 0n;

    /** @type {Map<string, { resolve: Function, reject: Function }>} */
    this.pending = new Map();

    /** @type {Buffer} */
    this.buffer = Buffer.alloc(0);
  }

  /**
   * Connect to the server
   * @returns {Promise<void>}
   */
  connect() {
    return new Promise((resolve, reject) => {
      this.shouldReconnect = true;
      this.#doConnect(resolve, reject);
    });
  }

  /**
   * Internal connect logic
   * @param {Function} [resolveInitial]
   * @param {Function} [rejectInitial]
   */
  #doConnect(resolveInitial, rejectInitial) {
    this.socket = new net.Socket();

    this.socket.connect(this.port, this.host, () => {
      this.connected = true;
      if (resolveInitial) {
        resolveInitial();
      }
    });

    this.socket.on('data', (chunk) => {
      this.buffer = Buffer.concat([this.buffer, chunk]);
      this.#processBuffer();
    });

    this.socket.on('error', (err) => {
      if (rejectInitial) {
        rejectInitial(err);
        rejectInitial = null;
        resolveInitial = null;
      }
      // Reject all pending requests
      for (const [, pending] of this.pending) {
        pending.reject(new Error(`Connection error: ${err.message}`));
      }
      this.pending.clear();
    });

    this.socket.on('close', () => {
      this.connected = false;
      // Reject all pending requests
      for (const [, pending] of this.pending) {
        pending.reject(new Error('Connection closed'));
      }
      this.pending.clear();

      if (this.shouldReconnect) {
        setTimeout(() => {
          if (this.shouldReconnect) {
            this.#doConnect();
          }
        }, this.reconnectDelayMs);
      }
    });
  }

  /**
   * Disconnect from the server
   */
  disconnect() {
    this.shouldReconnect = false;
    if (this.socket) {
      this.socket.destroy();
      this.socket = null;
    }
    this.connected = false;
  }

  /**
   * Process incoming response buffer
   */
  #processBuffer() {
    const HEADER_SIZE = 13; // 8 id + 1 status + 4 payload length

    while (this.buffer.length >= HEADER_SIZE) {
      const payloadLen = this.buffer.readUInt32BE(9);
      const totalLen = HEADER_SIZE + payloadLen;

      if (this.buffer.length < totalLen) {
        break;
      }

      const msgId = this.buffer.subarray(0, 8);
      const status = this.buffer[8];
      const payload = this.buffer.subarray(HEADER_SIZE, totalLen);

      this.buffer = this.buffer.subarray(totalLen);

      const idKey = msgId.toString('hex');
      const pending = this.pending.get(idKey);
      if (pending) {
        this.pending.delete(idKey);
        if (status === RES_OK) {
          pending.resolve(payload);
        } else if (status === RES_NOT_FOUND) {
          pending.resolve(null);
        } else {
          pending.reject(new Error(payload.toString('utf8')));
        }
      }
    }
  }

  /**
   * Generate next message ID
   * @returns {Buffer}
   */
  #nextMsgId() {
    const id = this.nextId++;
    const buf = Buffer.alloc(8);
    buf.writeBigUInt64BE(id, 0);
    return buf;
  }

  /**
   * Build and send a request
   * @param {number} msgType
   * @param {string} key
   * @param {Buffer} value
   * @returns {Promise<Buffer>}
   */
  #sendRequest(msgType, key, value = Buffer.alloc(0)) {
    return new Promise((resolve, reject) => {
      if (!this.connected || !this.socket) {
        reject(new Error('Not connected'));
        return;
      }

      const msgId = this.#nextMsgId();
      const keyBuf = Buffer.from(key, 'utf8');

      const header = Buffer.alloc(17);
      msgId.copy(header, 0);
      header[8] = msgType;
      header.writeUInt32BE(keyBuf.length, 9);
      header.writeUInt32BE(value.length, 13);

      const message = Buffer.concat([header, keyBuf, value, Buffer.from([END_MARKER])]);

      const idKey = msgId.toString('hex');
      this.pending.set(idKey, { resolve, reject });

      this.socket.write(message);
    });
  }

  /**
   * Write a key-value pair to the cache
   * @param {string} key
   * @param {Buffer|string} value
   * @returns {Promise<boolean>}
   */
  async write(key, value) {
    const valueBuf = Buffer.isBuffer(value) ? value : Buffer.from(value, 'utf8');
    await this.#sendRequest(MSG_WRITE, key, valueBuf);
    return true;
  }

  /**
   * Read a value from the cache
   * @param {string} key
   * @returns {Promise<Buffer>}
   */
  async read(key) {
    return this.#sendRequest(MSG_READ, key);
  }

  /**
   * Read and delete a value from the cache
   * @param {string} key
   * @returns {Promise<Buffer>}
   */
  async readAndDelete(key) {
    return this.#sendRequest(MSG_READ_AND_DELETE, key);
  }
}

module.exports = { NetCacheServer, NetCacheClient };
