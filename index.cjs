const net = require('net');
const {
  MSG_WRITE,
  MSG_READ,
  MSG_READ_AND_DELETE,
  RES_OK,
  RES_ERROR,
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
          this.#sendError(socket, msgId, 'Key not found');
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
          this.#sendError(socket, msgId, 'Key not found');
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

module.exports = { NetCacheServer };
