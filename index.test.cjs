const net = require('net');
const { test, describe, before, after } = require('node:test');
const assert = require('node:assert');
const { NetCacheServer } = require('./index.cjs');
const {
  MSG_WRITE,
  MSG_READ,
  MSG_READ_AND_DELETE,
  RES_OK,
  RES_ERROR,
  END_MARKER,
} = require('./common.cjs');

/**
 * Build a request message
 * @param {Buffer} msgId - 8-byte message ID
 * @param {number} msgType - message type byte
 * @param {string} key - cache key
 * @param {Buffer} value - cache value
 * @returns {Buffer}
 */
function buildRequest(msgId, msgType, key, value = Buffer.alloc(0)) {
  const keyBuf = Buffer.from(key, 'utf8');
  const header = Buffer.alloc(17);
  msgId.copy(header, 0);
  header[8] = msgType;
  header.writeUInt32BE(keyBuf.length, 9);
  header.writeUInt32BE(value.length, 13);

  return Buffer.concat([header, keyBuf, value, Buffer.from([END_MARKER])]);
}

/**
 * Parse a response message
 * @param {Buffer} buf - response buffer
 * @returns {{ msgId: Buffer, status: number, payload: Buffer }}
 */
function parseResponse(buf) {
  const msgId = buf.subarray(0, 8);
  const status = buf[8];
  const payloadLen = buf.readUInt32BE(9);
  const payload = buf.subarray(13, 13 + payloadLen);
  return { msgId, status, payload };
}

/**
 * Send a request and wait for a response
 * @param {net.Socket} socket
 * @param {Buffer} request
 * @returns {Promise<Buffer>}
 */
function sendRequest(socket, request) {
  return new Promise((resolve) => {
    socket.once('data', (data) => resolve(data));
    socket.write(request);
  });
}

describe('NetCacheServer', () => {
  let server;
  let client;
  const TEST_PORT = 11299;

  before(async () => {
    server = new NetCacheServer({ port: TEST_PORT, maxLifetimeMs: 5000 });
    await server.start();

    client = new net.Socket();
    await new Promise((resolve, reject) => {
      client.connect(TEST_PORT, '127.0.0.1', resolve);
      client.once('error', reject);
    });
  });

  after(async () => {
    client.destroy();
    await server.stop();
  });

  test('write and read a value', async () => {
    const msgId = Buffer.from([0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01]);
    const key = 'testKey';
    const value = Buffer.from('testValue');

    // Write
    const writeReq = buildRequest(msgId, MSG_WRITE, key, value);
    const writeRes = await sendRequest(client, writeReq);
    const writeParsed = parseResponse(writeRes);

    assert.deepStrictEqual(writeParsed.msgId, msgId);
    assert.strictEqual(writeParsed.status, RES_OK);
    assert.strictEqual(writeParsed.payload.length, 0);

    // Read
    const readMsgId = Buffer.from([0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02]);
    const readReq = buildRequest(readMsgId, MSG_READ, key);
    const readRes = await sendRequest(client, readReq);
    const readParsed = parseResponse(readRes);

    assert.deepStrictEqual(readParsed.msgId, readMsgId);
    assert.strictEqual(readParsed.status, RES_OK);
    assert.deepStrictEqual(readParsed.payload, value);
  });

  test('read non-existent key returns error', async () => {
    const msgId = Buffer.from([0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03]);
    const key = 'nonExistent';

    const req = buildRequest(msgId, MSG_READ, key);
    const res = await sendRequest(client, req);
    const parsed = parseResponse(res);

    assert.deepStrictEqual(parsed.msgId, msgId);
    assert.strictEqual(parsed.status, RES_ERROR);
    assert.strictEqual(parsed.payload.toString('utf8'), 'Key not found');
  });

  test('readAndDelete returns value and removes it', async () => {
    const writeId = Buffer.from([0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04]);
    const key = 'deleteMe';
    const value = Buffer.from('toBeDeleted');

    // Write
    const writeReq = buildRequest(writeId, MSG_WRITE, key, value);
    await sendRequest(client, writeReq);

    // Read and delete
    const readDelId = Buffer.from([0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05]);
    const readDelReq = buildRequest(readDelId, MSG_READ_AND_DELETE, key);
    const readDelRes = await sendRequest(client, readDelReq);
    const readDelParsed = parseResponse(readDelRes);

    assert.deepStrictEqual(readDelParsed.msgId, readDelId);
    assert.strictEqual(readDelParsed.status, RES_OK);
    assert.deepStrictEqual(readDelParsed.payload, value);

    // Verify deleted
    const verifyId = Buffer.from([0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x06]);
    const verifyReq = buildRequest(verifyId, MSG_READ, key);
    const verifyRes = await sendRequest(client, verifyReq);
    const verifyParsed = parseResponse(verifyRes);

    assert.strictEqual(verifyParsed.status, RES_ERROR);
    assert.strictEqual(verifyParsed.payload.toString('utf8'), 'Key not found');
  });

  test('overwrite existing key', async () => {
    const key = 'overwriteKey';
    const value1 = Buffer.from('first');
    const value2 = Buffer.from('second');

    // Write first value
    const write1Id = Buffer.from([0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07]);
    await sendRequest(client, buildRequest(write1Id, MSG_WRITE, key, value1));

    // Write second value
    const write2Id = Buffer.from([0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08]);
    await sendRequest(client, buildRequest(write2Id, MSG_WRITE, key, value2));

    // Read should return second value
    const readId = Buffer.from([0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09]);
    const readRes = await sendRequest(client, buildRequest(readId, MSG_READ, key));
    const readParsed = parseResponse(readRes);

    assert.strictEqual(readParsed.status, RES_OK);
    assert.deepStrictEqual(readParsed.payload, value2);
  });

  test('unknown message type returns error', async () => {
    const msgId = Buffer.from([0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0a]);
    const unknownType = 0x99;

    const req = buildRequest(msgId, unknownType, 'anyKey');
    const res = await sendRequest(client, req);
    const parsed = parseResponse(res);

    assert.deepStrictEqual(parsed.msgId, msgId);
    assert.strictEqual(parsed.status, RES_ERROR);
    assert.ok(parsed.payload.toString('utf8').includes('Unknown message type'));
  });
});
