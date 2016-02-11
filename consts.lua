--
-- Copyright (C) 2016 Meng Zhang @ Yottaa,Inc
--
-- Constants definition
--
module("consts",package.seeall)

local amqp = {

   DEFAULT_CHANNEL           = 0,
   DEFAULT_FRAME_SIZE        = 131072,
   DEFAULT_MAX_CHANNELS      = 65535,
   DEFAULT_HEARTBEAT         = 60,

   PROTOCOL_VERSION_MAJOR    = 0,
   PROTOCOL_VERSION_MINOR    = 9,
   PROTOCOL_VERSION_REVISION = 1,
   PROTOCOL_PORT             = 5672,
   PROTOCOL_SSL_PORT         = 5671,

   state = {
      CLOSED        = 0,
      ESTABLISHED   = 1,
      CLOSE_WAIT    = 2,
   },
      
   frame = {
      METHOD_FRAME    = 1,
      HEADER_FRAME    = 2,
      BODY_FRAME      = 3,
      HEARTBEAT_FRAME = 8,
      
      FRAME_MIN_SIZE  = 4096,
      FRAME_END       = 0xCE
   },

   method = {
      connection = { 
	 START     = 0x0A,
	 START_OK  = 0x0B,
	 SECURE    = 0x14,
	 SECURE_OK = 0x15,
	 TUNE      = 0x1E,
	 TUNE_OK   = 0x1F,
	 OPEN      = 0x28,
	 OPEN_OK   = 0x29,
	 CLOSE     = 0x32,
	 CLOSE_OK  = 0x33,
	 BLOCKED   = 0x3C,
	 UNBLOCKED = 0x3D,
      },
      channel = {
	 OPEN     = 0x0A,
	 OPEN_OK  = 0x0B,
	 FLOW     = 0x14,
	 FLOW_OK  = 0x15,
	 CLOSE    = 0x28,
	 CLOSE_OK = 0x29,
      },
      exchange = {
	 DECLARE     = 0x0A,
	 DECLARE_OK  = 0x0B,
	 DELETE      = 0x14,
	 DELETE_OK   = 0x15,
	 BIND        = 0x1E,
	 BIND_OK     = 0x1F,
	 UNBIND      = 0x28,
	 UNBIND_OK   = 0x33,
      },
      queue = {
	 DECLARE     = 0x0A,
	 DECLARE_OK  = 0x0B,
	 BIND        = 0x14,
	 BIND_OK     = 0x15,
	 PURGE       = 0x1E,
	 PURGE_OK    = 0x1F,
	 DELETE      = 0x28,
	 DELETE_OK   = 0x29,
	 UNBIND      = 0x32,
	 UNBIND_OK   = 0x33,
      },
      basic = {
	 QOS        = 0x0A,
	 QOS_OK     = 0x0B,
	 CONSUME    = 0x14,
	 CONSUME_OK = 0x15,
	 CANCEL     = 0x1E,
	 CANCEL_OK  = 0x1F,
	 PUBLISH    = 0x28,
	 RETURN     = 0x32,
	 DELIVER    = 0x3C,
	 GET        = 0x46,
	 GET_OK     = 0x47,
	 GET_EMPTY  = 0x48,
	 ACK        = 0x50,
	 REJECT     = 0x5A,
	 RECOVER_ASYNC  = 0x64,
	 RECOVER    = 0x6E,
	 RECOVER_OK = 0x6F,
	 NACK  = 0x78,
      },
      tx = {
	 SELECT      = 0x0A,
	 SELECT_OK   = 0x0B,
	 COMMIT      = 0x14,
	 COMMIT_OK   = 0x15,
	 ROLLBACK    = 0x1E,
	 ROLLBACK_OK = 0x1F,
      },
      confirm = {
	 SELECT     = 0x0A,
	 SELECT_OK  = 0x0B
      }
   },
   
   class = {
      CONNECTION  = 0x000A,
      CHANNEL     = 0x0014,
      EXCHANGE    = 0x0028,
      QUEUE       = 0x0032,
      BASIC       = 0x003C,
      TX          = 0x005A,
      CONFIRM     = 0x0055
   },

   flag = {
      CONTENT_TYPE     = 0x8000,
      CONTENT_ENCODING = 0x4000,
      HEADERS          = 0x2000,
      DELIVERY_MODE    = 0x1000,
      PRIORITY         = 0x0800,
      CORRELATION_ID   = 0x0400,
      REPLY_TO         = 0x0200,
      EXPIRATION       = 0x0100,
      MESSAGE_ID       = 0x0080,
      TIMESTAMP        = 0x0040,
      TYPE             = 0x0020,
      USER_ID          = 0x0010,
      APP_ID           = 0x0008,
      RESERVED1        = 0x0004
   },

   err = {
      REPLY_SUCCESS       = 200,
      CONTENT_TOO_LARGE   = 311,
      NO_ROUTE            = 312,
      NO_CONSUMERS        = 313,
      CONNECTION_FORCED   = 320,
      INVALID_PATH        = 402,
      ACCESS_REFUSED      = 403,
      NOT_FOUND           = 404,
      RESOURCE_LOCKED     = 405,
      PRECONDITION_FAILED = 406,
      FRAME_ERROR         = 501,
      SYNTAX_ERROR        = 502,
      COMMAND_INVALID     = 503,
      CHANNEL_ERROR       = 504,
      UNEXPECTED_FRAME    = 505,
      RESOURCE_ERROR      = 506,
      NOT_ALLOWED         = 530,
      NOT_IMPLEMENTED     = 540,
      INTERNAL_ERROR      = 541
   },



   PRODUCT = "amqp-lua",
   VERSION  = "1.0",
   COPYRIGHT = "Copyright (c) 2016 Meng Zhang @Yottaa,Inc",
   LOCALE = "en_US",
   MECHANISM_PLAIN = "PLAIN"
   
}

return amqp
