--
-- Copyright (C) 2016 Meng Zhang @ Yottaa,Inc
--
-- 
-- [0].https://www.rabbitmq.com/resources/specs/amqp0-9-1.pdf
-- [1].https://www.rabbitmq.com/amqp-0-9-1-reference.html
--

module("frame",package.seeall)

local c = require "consts"
local buffer = require "buffer"
local logger = require "logger"

-- Try to load bit library
local bit = require("bitopers")

local byte = string.byte
local format = string.format

local _M = {}

local function declare_exchange_flags(method)
   local bits = 0
   if method.passive then
      bits = bit.bor(bits,1)
   end
   
   if method.durable then
      bits = bit.bor(bits, 2)
   end

   if method.auto_delete then
      bits = bit.bor(bits, 4)
   end

   if method.internal then
      bits = bit.bor(bits,8)
   end

   if method.no_wait then
      bits = bit.bor(bits, 16)
   end
   
   return bits
end

local function declare_queue_flags(method)
   local bits = 0
   if method.passive then
      bits = bit.bor(bits,1)
   end
   
   if method.durable then
      bits = bit.bor(bits, 2)
   end

   if method.exclusive then
      bits = bit.bor(bits, 4)
   end

   if method.auto_delete then
      bits = bit.bor(bits, 8)
   end

   if method.no_wait then
      bits = bit.bor(bits, 16)
   end
   
   return bits
end

local function basic_consume_flags(method)
   local bits = 0
   if method.no_local then
      bits = bit.bor(bits,1)
   end

   if method.no_ack then
      bits = bit.bor(bits, 2)
   end

   if method.exclusive then
      bits = bit.bor(bits, 4)
   end

   if method.no_wait then
      bits = bit.bor(bits, 8)
   end
   return bits
end

local function decode_close_reply(b)
   local frame = {}
   frame.reply_code = b:get_i16()
   frame.reply_text = b:get_short_string()
   frame.class_id = b:get_i16()
   frame.method_id = b:get_i16()
   return frame
end

local function encode_close_reply(method)
   local b = buffer.new()
   b:put_i16(method.reply_code)
   b:put_short_string(method.reply_text)
   b:put_i16(method.class_id)
   b:put_i16(method.method_id)
   return b:payload()
end

local function nop()
   return nil
end

local methods_ = {
   [c.class.CONNECTION] = {
      name = "connection",
      --[[
	 major octet
	 minor octet
	 properties field_table
	 mechanism long_string
	 locales long_string
      --]]
      [c.method.connection.START] = {
	 name = "start",
	 r = function(b)
	    local major = b:get_i8()
	    local minor = b:get_i8()
	    local props = b:get_field_table()
	    local mechanism = b:get_long_string()
	    local locales = b:get_long_string()
	    return {
	       major = major,
	       minor = minor,
	       props = props,
	       mechanism = mechanism,
	       locales = locales
	    }
	 end
      },
      --[[
	 client_properties field_table
	 mechanism short_string
	 response long_string
	 locale short_string
      --]]
      [c.method.connection.START_OK] = {
	 name = "start_ok",
	 w = function(method)
	    local b = buffer.new()
	    b:put_field_table(method.properties)
	    b:put_short_string(method.mechanism)
	    b:put_long_string(method.response)
	    b:put_short_string(method.locale)
	    return b:payload()
	 end
	      
      },
      --[[
	 secure long_string
      --]]
      [c.method.connection.SECURE] = {
	 name = "secure"
      },
      [c.method.connection.SECURE_OK] = {
	 name = "secure_ok"
      },
      --[[
	 channel_max i16
	 frame_max i32
	 beartbeat i16
      --]]
      [c.method.connection.TUNE] = {
	 name = "tune",
	 r = function(b)
	    local f = {}
	    f.channel_max = b:get_i16()
	    f.frame_max = b:get_i32()
	    f.heartbeat = b:get_i16()
	    return f
	 end
      },
      --[[
	 channel_max i16
	 frame_max i32
	 heartbeat i16
      --]]
      [c.method.connection.TUNE_OK] = {
	 name = "tune_ok",
	 w = function(method)
	    local b = buffer.new()
	    b:put_i16(method.channel_max)
	    b:put_i32(method.frame_max)
	    b:put_i16(method.heartbeat or c.DEFAULT_HEARTBEAT)
	    return b:payload()
	 end
      },
      --[[
	 virtual_host short_string,
	 reserved-1(capabilities) octet
	 reserved-2 octet
      --]]
      [c.method.connection.OPEN] = {
	 name = "open",
	 w = function(method)
	    local b = buffer.new()
	    b:put_short_string(method.virtual_host)
	    b:put_i8(0) -- capabilities
	    b:put_i8(1) -- insist?
	    return b:payload()
	 end
      },
      --[[
	 reserved-1 short_string
      --]]
      [c.method.connection.OPEN_OK] = {
	 name = "open_ok",
	 r = function(b)
	    return { reserved1 = b:get_short_string() }
	 end
      },
      --[[
	 reply_code i16
	 reply_text short_string
	 class_id i16
	 method_id i16
      --]]
      [c.method.connection.CLOSE] = {
	 name = "close",
	 r = decode_close_reply,
	 w = encode_close_reply
      },
      --[[
	 
      --]]
      [c.method.connection.CLOSE_OK] = {
	 name = "close_ok",
	 r = nop,
	 w = nop
      },
      --[[
	 reason short_string
      --]]
      [c.method.connection.BLOCKED] = {
	 name = "blocked",
	 r = function(b)
	    return {reason = b:read_get_short_string()}
	 end,
	 w = function(method)
	    local b = buffer.new()
	    b:put_short_string(method.reason)
	    return b:payload()
	 end

      },
      --[[
	 
      --]]
      [c.method.connection.UNBLOCKED] = {
	 name = "unblocked",
	 r = function(b)
	    return nil
	 end,
	 w = function(method)
	    return nil
	 end
      }
   },
   [c.class.CHANNEL] = {
      name = "channel",
      [c.method.channel.OPEN] = {
	 name = "open",
	 w = function(method)
	    -- reserved?
	    return '\0'
	 end
      },
      [c.method.channel.OPEN_OK] = {
	 name = "open_ok",
	 r = function(b)
	    return {
	       reserved1 = b:get_long_string()
	    }
	 end
      },
      --[[
	 active bit
      --]]
      [c.method.channel.FLOW] = {
	 name = "flow",
	 r = function(b)
	    return { active = b:get_bool() }
	 end,
	 w = function(method)
	    local b = buffer.new()
	    b:put_bool(method.active)
	    return b:payload()
	 end

      },
      [c.method.channel.FLOW_OK] = {
	 name = "flow_ok",
	 r = function(b)
	    local bits = b:read_get_i8()
	    return { active = bit.band(bits,1) }
	 end,
	 w = function(method)
	    local b = buffer.new()
	    b:put_bool(method.active)
	    return b:payload()
	 end
      },
      --[[
	 reply_code i16
	 reply_text short_string
	 class_id i16
	 method_id i16
      --]]
      [c.method.channel.CLOSE] = {
	 name = "close",
	 r = decode_close_reply,
	 w = encode_close_reply
      },
      [c.method.channel.CLOSE_OK] = {
	 name = "close_ok",
	 r = nop,
	 w = nop
      },


   },
   [c.class.EXCHANGE] = {
      name = "exchange",
      --[[
	 reserved1 i16
	 exchange short_string
	 type short_string
	 passive bit
	 durable bit
	 auto_delete bit
	 internal bit
	 no_wait bit
	 arguments table
      --]]
      [c.method.exchange.DECLARE] = {
	 name = "declare",
	 w = function(method)
	    local bits = 0
	    local b = buffer.new()
	    b:put_i16(method.reserved1 or 0)
	    b:put_short_string(method.exchange)
	    b:put_short_string(method.typ)
	    b:put_i8(declare_exchange_flags(method))
	    b:put_field_table(method.arguments or {})
	    return b:payload()
	 end
      },
      [c.method.exchange.DECLARE_OK] = {
	 name = "declare_ok",
	 r = nop
      },
      --[[
	 reserved1 i16
	 destination short_string
	 source short_string
	 routing_key short_string
	 no_wait bit
	 arguments table
      --]]
      [c.method.exchange.BIND] = {
	 name = "bind",
	 w = function(method)
	    local b = buffer.new()
	    b:put_i16(method.reserved1 or 0)
	    b:put_short_string(method.destination)
	    b:put_short_string(method.source)
	    b:put_short_string(method.routing_key)
	    b:put_bool(method.no_wait)
	    b:put_field_table(method.arguments or {})
	    return b:payload()
	    
	 end
      },
      [c.method.exchange.BIND_OK] = {
	 name = "bind_ok",
	 r = nop
      },

      --[[

      --]]
      [c.method.exchange.DELETE] = {
	 name = "delete",
	 w = function(method)
	    local b = buffer.new()
	    b:put_i16(method.reserved1 or 0)
	    b:put_short_string(method.exchange)
	    local bits = 0
	    if method.if_unused then
	       bits = bit.bor(bits,1)
	    end
	    if method.no_wait then
	       bits = bit.bor(bits,2)
	    end
	    b:put_i8(bits)
	    return b:payload()
	 end
      },
      [c.method.exchange.DELETE_OK] = {
	 name = "delete_ok",
	 r = nop
      },
      --[[
	 
      --]]
      [c.method.exchange.UNBIND] = {
	 name = "unbind",
	 w = function(method)
	    local b = buffer.new()
	    b:put_i16(method.reserved1 or 0)
	    b:put_short_string(method.destination)
	    b:put_short_string(method.source)
	    b:put_short_string(method.routing_key)
	    b:put_bool(method.no_wait)
	    b:put_field_table(method.arguments or {})
	    return b:payload()
	    
	 end
      },
      [c.method.exchange.UNBIND_OK] = {
	 name = "unbind_ok",
	 r = nop
      },


   },
   [c.class.QUEUE] = {
      name = "queue",
      [c.method.queue.DECLARE] = {
	 name = "declare",
	 w = function(method)
	    local b = buffer.new()
	    b:put_i16(method.ticket or 0)
	    b:put_short_string(method.queue)
	    local bits = declare_queue_flags(method)
	    b:put_i8(bits)
	    b:put_field_table(method.arguments or {})
	    return b:payload()
	 end
      },
      [c.method.queue.DECLARE_OK] = {
	 name = "declare_ok",
	 r = function(b)
	    local f = {}
	    f.queue = b:get_short_string()
	    f.message_count = b:get_i32()
	    f.consumer_count = b:get_i32()
	    return f
	 end
      },
      [c.method.queue.BIND] = {
	 name = "bind",
	 w = function(method)
	    local b = buffer.new()
	    b:put_i16(method.reserved1 or 0)
	    b:put_short_string(method.queue)
	    b:put_short_string(method.exchange)
	    b:put_short_string(method.routing_key or "")
	    b:put_bool(method.no_wait)
	    b:put_field_table(method.arguments or {})
	    return b:payload()
	 end
      },
      [c.method.queue.BIND_OK] = {
	 name = "bind_ok",
	 r = nop
      },
      --[[
	 reserved1 i16
	 queue short_string
	 if_unused bit
	 if_empty bit
	 no_wait bit
      --]]
      [c.method.queue.DELETE] = {
	 name = "delete",
	 w = function(method)
	    local b = buffer.new()
	    b:put_i16(method.reserved1 or 0)
	    b:put_short_string(method.queue)
	    local bits = 0
	    if method.if_unused then
	       bits = bit.bor(bits,1)
	    end
	    if method.if_empty then
	       bits = bit.bor(bits,2)
	    end
	    if method.no_wait then
	       bits = bit.bor(bits, 4)
	    end
	    b:put_i8(bits)
	    return b:payload()
	    
	 end
      },
      --[[
	 message_count i32
      --]]
      [c.method.queue.DELETE_OK] = {
	 name = "delete_ok",
	 r = function(b)
	    return {
	       message_count = b:get_i32()
	    }

	 end
      },
      [c.method.queue.UNBIND] = {
	 name = "unbind",
	 w = function(method)
	    local b = buffer.new()
	    b:put_i16(method.reserved1 or 0)
	    b:put_short_string(method.queue)
	    b:put_short_string(method.exchange)
	    b:put_short_string(method.routing_key or "")
	    b:put_field_table(method.arguments or {})
	    return b:payload()
	 end
      },
      [c.method.queue.UNBIND_OK] = {
	 name = "unbind_ok",
	 r = nop
      },
      --[[
	 reserved1 i16
	 queue short_string
	 no_wait bit
      --]]
      [c.method.queue.PURGE] = {
	 name = "queue_purge",
	 w = function(method)
	    local b = buffer.new()
	    b:put_i16(method.reserved1 or 0)
	    b:put_short_string(method.queue)
	    local bits = 0
	    if method.no_wait then
	       bits = bit.bor(bits, 1)
	    end
	    b:put_i8(bits)
	    return b:payload()
	    
	 end
      },
      --[[
	 message_count i32
      --]]
      [c.method.queue.PURGE_OK] = {
	 name = "purge_ok",
	 r = function(b)
	    return {
	       message_count = b:get_i32()
	    }
	 end
      },


   },
   [c.class.BASIC] = {
      name = "basic",
      [c.method.basic.CONSUME] = {
	 name = "consume",
	 w = function(method)
	    local b = buffer.new()
	    b:put_i16(method.ticket or 0)
	    b:put_short_string(method.queue)
	    b:put_short_string(method.consumer_tag or "")
	    b:put_i8(basic_consume_flags(method))
	    b:put_field_table(method.arguments or {})
	    return b:payload()
	 end
      },
      [c.method.basic.CONSUME_OK] = {
	 name = "consume_ok",
	 r = function(b)
	    return {
	       consumer_tag = b:get_short_string()
	    }
	 end
      },
      --[[
	 consumer_tag short_string
	 delivery_tag short_string
	 redelivered bool
	 exchange short_string
	 routing_key short_string
      --]]
      [c.method.basic.DELIVER] = {
	 name = "deliver",
	 r = function(b)
	    local f = {}
	    f.consumer_tag = b:get_short_string()
	    f.delivery_tag = b:get_i64()
	    f.redelivered = b:get_i8()
	    f.exchange = b:get_short_string()
	    f.routing_key = b:get_short_string()
	    return f
	 end,
	 w = function(method)
	    local b = buffer.new()
	    b:put_short_string(method.consumer_tag)
	    b:put_short_string(method.delivery_tag)
	    b:put_bool(method.redelivered)
	    b:put_short_string(method.exchange)
	    b:put_short_string(method.routing_key)
	    return b:payload()
	 end
      },

      --[[
	 prefectch_size i32
	 prefetch_count i16
	 global bit
      --]]
      [c.method.basic.QOS] = {
	 name = "qos",
	 w = function(method)
	    local b = buffer.new()
	    b:put_i32(method.prefetch_size)
	    b:put_i16(method.prefetch_count)
	    b:put_bool(method.global)
	    return b:payload()
	 end
      },
      [c.method.basic.QOS_OK] = {
	 name = "qos_ok",
	 r = nop
      },
      --[[
	 consumer_tag short_string
	 no_wait bit
      --]]
      [c.method.basic.CANCEL] = {
	 name = "cancel",
	 w = function(method)
	    local b = buffer.new()
	    b:put_short_string(method.consumer_tag)
	    b:put_bool(method.no_wait)
	    return b:payload()
	 end
      },
      [c.method.basic.CANCEL_OK] = {
	 name = "cancel_ok",
	 r = function(b)
	    return {
	       consumer_tag = b:get_short_string()
	    }
	 end
      },
      --[[
	 reserved1 i16
	 queue short_string
	 no_ack bit
      --]]
      [c.method.basic.GET] = {
	 name = "get",
	 w = function(method)
	    local b = buffer.new()
	    b:put_i16(method.reserved1 or 0)
	    b:put_short_string(method.queue)
	    b:put_bool(method.no_ack)
	    return b:payload()
	 end
      },
      --[[
	 delivery_tag short_string
	 redelivered bool
	 exchange short_string
	 routing_key short_string
	 message_count i32
      --]]
      [c.method.basic.GET_OK] = {
	 name = "get_ok",
	 r = function(b)
	    local f = {}
	    f.delivery_tag = b:get_short_string()
	    f.redelivered = b:get_bool()
	    f.exchange = b:get_short_string()
	    f.routing_key = b:get_short_string()
	    f.message_count = b:get_i32()
	    return f
	 end
      },
      --[[
	 requeue bit
      --]]
      [c.method.basic.RECOVER] = {
	 name = "recover",
	 w = function(method)
	    local b = buffer.new()
	    b:put_bool(method.requeue)
	    return b:payload()
	 end
      },
      [c.method.basic.RECOVER_OK] = {
	 name = "recover_ok",
	 r = nop
      },
      [c.method.basic.RECOVER_ASYNC] = {
	 name = "recover_async",
	 w = function(b)
	    local b = buffer.new()
	    b:put_bool(method.requeue)
	    return b:payload()
	 end
      },
      --[[
	 reserved1 i16
	 exchange short_string 
	 routing_key short_string
	 mandatory bit
	 immediate bit
      --]]
      [c.method.basic.PUBLISH] = {
	 name = "publish",
	 w = function(method)
	    local b = buffer.new()
	    b:put_i16(method.reserved1 or 0)
	    b:put_short_string(method.exchange)
	    b:put_short_string(method.routing_key)
	    local bits = 0
	    if method.mandatory then
	       bits = bit.bor(bits,1)
	    end
	    if method.immediate then
	       bits = bit.bor(bits,2)
	    end
	    b:put_i8(bits)
	    return b:payload()
	 end
      },
      --[[
	 reply_code i16
	 reply_text short_string
	 exchange short_string
	 routing_key short_string
      --]]
      [c.method.basic.RETURN] = {
	 name = "return",
	 w = function(method)
	    local b = buffer.new()
	    b:put_i16(method.reply_code)
	    b:put_short_string(method.reply_text)
	    b:put_short_string(method.exchange)
	    b:wrie_short_string(method.routing_key)
	    return b:payload()
	 end
      },
      --[[
	 reserved1 i16
      --]]
      [c.method.basic.GET_EMPTY] = {
	 name = "get_emtpy",
	 r = function(b)
	    return {
	       reserved1 = b:get_i16()
	    }
	 end
      },
      --[[
	 delivery_tag short_string
	 multiple bit
      --]]
      [c.method.basic.ACK] = {
	 name = "ack",
	 r = function(b)
	    local method = {}
	    method.delivery_tag = b:get_short_string()
	    method.multiple = b:get_bool()
	    return method
	 end,
	 w = function(method)
	    local b = buffer.new()
	    b:put_short_string(method.delivery_tag)
	    b:put_bool(method.multiple)
	    return b:payload()
	 end
      },
      --[[
	 delivery_tag short_string
	 multiple bit
	 requeue bit
      --]]
      [c.method.basic.NACK] = {
	 name = "nack",
	 r = function(b)
	    local f = {}
	    f.delivery_tag = b:get_short_string()
	    local v = b:get_i8()
	    f.multiple = (bit.band(v,0x1) ~= 0)
	    f.requeue = (bit.band(v,0x2) ~= 0)
	    return f
	 end,
	 w = function(method)
	    local b = buffer.new()
	    b:put_short_string(method.delivery_tag)
	    local bits = 0
	    if method.multiple and method.multiple ~= 0 then
	       bits = bit.bor(bits, 1)
	    end
	    if method.requeue and method.requeue ~= 0 then
	       bits = bit.bor(bit, 2)
	    end
	    b:put_i16(bits)
	    return b:payload()
	 end
      },
      --[[
	 delivery_tag short_string
	 requeue bit
      --]]
      [c.method.basic.REJECT] = {
	 name = "reject",
	 r = function(b)
	    local f = {}
	    f.delivery_tag = b:get_short_string()
	    f.requeue = b:get_bool()
	    return f
	 end,
	 w = function(method)
	    local b = buffer.new()
	    b:put_short_string(method.delivery_tag)
	    b:put_bool(method.requeue)
	    return b:payload()
	 end
      },

   },
   [c.class.TX] = {
      name = "tx",
      [c.method.tx.SELECT] = {
	 name = "select",
	 w = nop
      },
      [c.method.tx.SELECT_OK] = {
	 name = "select_ok",
	 r = nop
      },
      [c.method.tx.COMMIT] = {
	 name = "commit",
	 w = nop
      },
      [c.method.tx.COMMIT_OK] = {
	 name = "commit_ok",
	 r = nop
      },
      [c.method.tx.ROLLBACK] = {
	 name = "rollback",
	 w = nop
      },
      [c.method.tx.ROLLBACK_OK] = {
	 name = "rollback_ok",
	 r = nop
      },

   },
   [c.class.CONFIRM] = {
      name = "confirm",
      --[[
	 no_wait bit
      --]]
      [c.method.confirm.SELECT] = {
	 name = "select",
	 w = function(method)
	    local b = buffer.new()
	    b:put_bool(method.no_wait)
	    return b:payload()
	 end
      },
      [c.method.confirm.SELECT_OK] = {
	 name = "select_ok",
	 r = nop
      },
   }
}

--
-- decoder
--

local function method_frame(ctx,channel,size)
   local frame = { channel = channel }
   local sock = ctx.sock
   local data, err = sock:receive(size)
   if not data then
      return nil,err
   end

   local b = buffer.new(data)
   -- debug
   logger.dbg("[method_frame]",b:hex_dump())
   local class_id = b:get_i16()
   local method_id = b:get_i16()
   frame.class_id = class_id
   frame.method_id = method_id
   logger.dbg("[method_frame] class_id:",class_id, "method_id:", method_id)
   local codec = methods_[class_id][method_id]
   if not codec then
      local err = "[method_frame]: no codec for class: " .. class_id .. " method: " .. method_id
      return nil,err
   end

   if not codec.r then
      local err = "[method_frame]: no decoder for class: " .. class_id .. " method: " .. method_id
      return nil, err
   end
   logger.dbg("[method_frame] class:",methods_[class_id].name, "method:", codec.name)
   frame.method = codec.r(b)
   return frame
end

local function header_frame(ctx,channel,size)
   local f = {
      channel = channel,
      properties = {}
   }

   local sock = ctx.sock
   local data,err = sock:receive(size)
   if not data then
      return nil,err
   end

   local b = buffer.new(data)
   logger.dbg("[header_frame]",b:hex_dump())
   
   f.class_id = b:get_i16()
   f.weight = b:get_i16()
   f.body_size = b:get_i64()
   local flag = b:get_i16()
   f.flag = flag
   if bit.band(flag,c.flag.CONTENT_TYPE) ~= 0 then
      f.properties.content_type= b:get_short_string()
   end

   if bit.band(flag,c.flag.CONTENT_ENCODING) ~= 0 then
      f.properties.content_encoding = b:get_short_string()
   end

   if bit.band(flag,c.flag.HEADERS) ~= 0 then
      f.properties.headers = b:get_field_table()
   end

   if bit.band(flag,c.flag.DELIVERY_MODE) ~= 0 then
      f.properties.delivery_mode = b:get_i8()
   end

   if bit.band(flag,c.flag.PRIORITY) ~= 0 then
      f.properties.priority = b:get_i8()
   end

   if bit.band(flag,c.flag.CORRELATION_ID) ~= 0 then
      f.properties.correlation_id = b:get_short_string()
   end
   
   if bit.band(flag,c.flag.REPLY_TO) ~= 0 then
      f.properties.reply_to = b:get_short_string()
   end
   
   if bit.band(flag,c.flag.EXPIRATION) ~= 0 then
      f.properties.expiration = b:get_short_string()
   end
   
   if bit.band(flag,c.flag.MESSAGE_ID) ~= 0 then
      f.properties.message_id = b:get_short_string()
   end
   
   if bit.band(flag,c.flag.TIMESTAMP) ~= 0 then
      f.properties.timestamp = b:get_timestamp()
   end
   
   if bit.band(flag,c.flag.TYPE) ~= 0 then
      f.properties.type, pos = b:get_short_string()
   end
   
   if bit.band(flag,c.flag.USER_ID) ~= 0 then
      f.properties.user_id = b:get_short_string()
   end

   if bit.band(flag,c.flag.APP_ID) ~= 0 then
      f.properties.app_id = b:get_short_string()
   end
   
   if bit.band(flag,c.flag.RESERVED1) ~= 0 then
      f.properties.resvered1 = b:get_short_string()
   end
   
   return f
end

local function body_frame(ctx,channel,size)
   local frame = { channel = channel }
   local sock = ctx.sock
   local data,err = sock:receive(size)
   if not data then
      return nil, err
   end
   local b = buffer.new(data)

   logger.dbg("[body_frame]",b:hex_dump())
   frame.body = b:payload()
   return frame
end


local function heartbeat_frame(ctx,channel,size)
   local frame = { channel = channel}
   if size > 0 then
      return nil
   end
   return frame
end

function _M.consume_frame(ctx)
   local sock = ctx.sock
   local data,err = sock:receive(7)
   if not data then
      return nil, err
   end
   
   local b = buffer.new(data)
   logger.dbg("[frame] 1st 7octets: ",b:hex_dump())
   local typ = b:get_i8()
   local channel = b:get_i16()
   local size = b:get_i32()
   local ok,fe,err
   if typ == c.frame.METHOD_FRAME then
      ok,fe,err = pcall(method_frame,ctx,channel,size)
   elseif typ == c.frame.HEADER_FRAME then
      ok,fe,err = pcall(header_frame,ctx,channel,size)
   elseif typ == c.frame.BODY_FRAME then
      ok,fe,err = pcall(body_frame,ctx,channel,size)
   elseif typ == c.frame.HEARTBEAT_FRAME then
      ok,fe,err = pcall(heartbeat_frame,ctx,channel,size)
   else
      ok = nil
      err = "invalid frame type"
   end

   -- THE END --
   local ok0,err0 = sock:receive(1)
   if not ok0 then
      return nil,err0
   end

   local tk = byte(ok0,1)
   if tk ~= c.frame.FRAME_END then
      return nil,"malformed frame: no frame_end."
   end

   -- err captured by pcall, most likely, due to malformed frames
   if not ok then
      return nil, fe
   end

   -- other errors
   if not fe then
      return nil, err
   end
   
   fe.type = typ
   return fe
end

--
-- encoder
--
local function encode_frame(typ,channel,payload)

   payload = payload or ""
   
   local size = #payload
   local b = buffer.new()
   b:put_i8(typ)
   b:put_i16(channel)
   b:put_i32(size)
   b:put_payload(payload)
   b:put_i8(c.frame.FRAME_END)
   return b:payload()
end


local function encode_method_frame(frame)
   local b = buffer.new()
   b:put_i16(frame.class_id)
   b:put_i16(frame.method_id)
   local payload = methods_[frame.class_id][frame.method_id].w(frame.method)
   if payload then
      b:put_payload(payload)
   end
   return encode_frame(c.frame.METHOD_FRAME,frame.channel,b:payload())
end

local function flags_mask(frame)
   local mask = 0

   if not frame.properties then
      return mask
   end
   if frame.properties.content_type ~= nil then
      mask = bit.bor(mask,c.flag.CONTENT_TYPE)
   end

   if frame.properties.content_encoding ~= nil then
      mask = bit.bor(mask,c.flag.CONTENT_ENCODING)
   end
   if frame.properties.headers ~= nil then
      mask = bit.bor(mask,c.flag.HEADERS)
   end
   if frame.properties.delivery_mode ~= nil then
      mask = bit.bor(mask,c.flag.DELIVERY_MODE)
   end
   if frame.properties.priority ~= nil then
      mask = bit.bor(mask,c.flag.PRIORITY)
   end
   if frame.properties.correlation_id ~= nil then
      mask = bit.bor(mask,c.flag.CORRELATION_ID)
   end
   if frame.properties.reply_to ~= nil then
      mask = bit.bor(mask,c.flag.REPLY_TO)
   end
   if frame.properties.expiration ~= nil then
      mask = bit.bor(mask,c.flag.EXPIRATION)
   end
   if frame.properties.timestamp ~= nil then
      mask = bit.bor(mask,c.flag.TIMESTAMP)
   end
   if frame.properties.type ~= nil then
      mask = bit.bor(mask,c.flag.TYPE)
   end
   if frame.properties.user_id ~= nil then
      mask = bit.bor(mask,c.flag.USER_ID)
   end
   if frame.properties.app_id ~= nil then
      mask = bit.bor(mask,c.flag.APP_ID)
   end

   return mask
end

local function encode_header_frame(frame)
   local b = buffer.new()
   b:put_i16(frame.class_id)
   b:put_i16(frame.weight)
   b:put_i64(frame.size)

   local flags = flags_mask(frame)
   b:put_i16(flags)
   if bit.band(flags,c.flag.CONTENT_TYPE) ~= 0 then
      b:put_short_string(f.properties.content_type)
   end

   if bit.band(flags,c.flag.CONTENT_ENCODING) ~= 0 then
      b:put_short_string(f.properties.content_encoding)
   end

   if bit.band(flags,c.flag.HEADERS) ~= 0 then
      b:put_field_table(f.properties.headers)
   end

   if bit.band(flags,c.flag.DELIVERY_MODE) ~= 0 then
      b:put_i8(f.properties.delivery_mode)
   end

   if bit.band(flags,c.flag.PRIORITY) ~= 0 then
      b:put_i8(f.properties.priority)
   end

   if bit.band(flags,c.flag.CORRELATION_ID) ~= 0 then
      b:put_short_string(f.properties.correlation_id)
   end
   
   if bit.band(flags,c.flag.REPLY_TO) ~= 0 then
      b:put_short_string(f.properties.reply_to)
   end
   
   if bit.band(flags,c.flag.EXPIRATION) ~= 0 then
      b:put_short_string(f.properties.expiration)
   end
   
   if bit.band(flags,c.flag.MESSAGE_ID) ~= 0 then
      b:put_short_string(f.properties.message_id)
   end
   
   if bit.band(flags,c.flag.TIMESTAMP) ~= 0 then
      b:put_time_stamp(f.properties.timestamp)
   end
   
   if bit.band(flags,c.flag.TYPE) ~= 0 then
      b:put_short_string(f.properties.type)
   end
   
   if bit.band(flags,c.flag.USER_ID) ~= 0 then
      b:put_short_string(f.properties.user_id)
   end

   if bit.band(flags,c.flag.APP_ID) ~= 0 then
      b:put_short_string(f.properties.app_id)
   end

   return encode_frame(c.frame.HEADER_FRAME,frame.channel,b:payload())
end

local function encode_body_frame(frame)
   return encode_frame(c.frame.BODY_FRAME,frame.channel,frame.body)
end

local function encode_heartbeat_frame(frame)
   return encode_frame(c.frame.HEARTBEAT_FRAME,frame.channel,nil)
end


local mt = { __index = _M }
--
-- new a frame
--
function _M.new(typ,channel)
   return setmetatable({
	 typ = typ,
	 channel = channel
		       }, mt)
end

function _M.new_method_frame(channel,class_id,method_id)
   local frame = _M.new(c.frame.METHOD_FRAME, channel)
   frame.class_id = class_id
   frame.method_id = method_id
   return frame
end

function _M:encode()
   local typ = self.typ
   if not typ then
      local err = "no frame type specified."
      logger.error("[frame.encode] " .. err)
      return nil,err
   end
   
   if typ == c.frame.METHOD_FRAME then
      return encode_method_frame(self)      
   elseif typ == c.frame.HEADER_FRAME then
      return encode_header_frame(self)
   elseif typ == c.frame.BODY_FRAME then
      return encode_body_frame(self)
   elseif typ == c.frame.HEARTBEAT_FRAME then
      return encode_heartbeat_frame(self)
   else
      local err = "invalid frame type" .. tostring(typ)
      logger.error("[frame.encode]" .. err)
      return nil, err
   end
end

--
-- protocol
--

function _M.wire_protocol_header(ctx)
   local sock = ctx.sock
   local bytes, err = sock:send("AMQP\0\0\9\1")
   if not bytes then
      return nil, err
   end
   
   return _M.consume_frame(ctx)
end

function _M.wire_heartbeat(ctx)
   
   local frame = _M.new(c.frame.HEARTBEAT_FRAME,c.DEFAULT_CHANNEL)

   local msg = frame:encode()
   local sock = ctx.sock
   local bytes, err = sock:send(msg)
   if not bytes then
      return nil,"[heartbeat]" .. err
   end
   return bytes
end

function _M.wire_header_frame(ctx,body_size)
   local frame = _M.new(c.frame.HEADER_FRAME,ctx.opts.channel or 1)
   frame.class_id = c.class.BASIC
   frame.weight = 0
   frame.size = body_size
   
   local msg = frame:encode()
   local sock = ctx.sock
   local bytes, err = sock:send(msg)
   if not bytes then
      return nil,"[wire_header_frame]" .. err
   end
   return bytes
end

function _M.wire_body_frame(ctx,payload, s, e)
   local frame = _M.new(c.frame.BODY_FRAME,ctx.opts.channel or 1)
   frame.body = payload:sub(s, e)
   local msg = frame:encode()
   local sock = ctx.sock
   local bytes, err = sock:send(msg)
   if not bytes then
      return nil,"[wire_body_frame]" .. err
   end
   return bytes
end


local function is_channel_close_received(frame)
   return frame ~= nil and frame.class_id == c.class.CHANNEL and frame.method_id == c.method.channel.CLOSE
end

local function is_connection_close_received(frame)
   return frame ~= nil and frame.class_id == c.class.CONNECTION and frame.method_id == c.method.connection.CLOSE 
end

local function ongoing(ctx,frame)
   ctx.ongoing = ctx.ongoing or {}
   ctx.ongoing.class_id = frame.class_id
   ctx.ongoing.method_id = frame.method_id
end

function _M.wire_method_frame(ctx,frame)
   
   local msg = frame:encode()
   local sock = ctx.sock
   local bytes,err = sock:send(msg)
   if not bytes then
      return nil,"[wire_method_frame]" .. err
   end

   logger.dbg("[wire_method_frame] wired a frame.", "[class_id]: ", frame.class_id, "[method_id]: ", frame.method_id)
   if frame.method ~= nil and not frame.method.no_wait then
      local f = _M.consume_frame(ctx)
      if f then
	 logger.dbg("[wire_method_frame] channel: ",f.channel)
	 if f.method then
	    logger.dbg("[wire_method_frame] method: ",f.method)
	 end
	 
	 if is_channel_close_received(f) then
	    ctx.channel_state = c.state.CLOSE_WAIT
	    ongoing(ctx,frame)
	    return nil, f.method.reply_code, f.method.reply_text
	 end

	 if is_connection_close_received(f) then
	    ctx.channel_state = c.state.CLOSED
	    ctx.connection_state = c.state.CLOSE_WAIT
	    ongoing(ctx,frame)
	    return nil, f.method.reply_code, f.method.reply_text
	 end
      end
      return f
   end
   return true
   
end

return _M
