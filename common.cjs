// Message types
const MSG_WRITE = 0x00;
const MSG_READ = 0x01;
const MSG_READ_AND_DELETE = 0x02;

// Response codes
const RES_OK = 0xa0;
const RES_ERROR = 0xa1;
const RES_NOT_FOUND = 0xa2;

// End-of-message marker
const END_MARKER = 0xff;

module.exports = {
  MSG_WRITE,
  MSG_READ,
  MSG_READ_AND_DELETE,
  RES_OK,
  RES_ERROR,
  RES_NOT_FOUND,
  END_MARKER,
};
