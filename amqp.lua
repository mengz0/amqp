--
-- Copyright (C) 2016 Meng Zhang @ Yottaa,Inc
--

module("amqp",package.seeall)

local c = require "consts"
local frame = require "frame"
local logger = require "logger"

local bit = require "bit"
local band = bit.band
local bor = bit.bor
local lshift = bit.lshift
local rshift = bit.rshift
local tohex = bit.tohex

local format = string.format
local gmatch = string.gmatch
local max = math.max
local min = math.min

local socket
-- let ngx.socket take precedence to 
if _G.ngx and _G.ngx.socket then
   socket = ngx.socket
else
   socket = require("socket")
end
local tcp = socket.tcp


local amqp = {}
local mt = { __index = amqp }

-- to check whether we have valid parameters to setup
local function mandatory_options(opts)
   if not opts then
      error("no opts provided.")
   end

   if type(opts) ~= "table" then
      error("opts is not valid.")
   end

   if (opts.role == nil or opts.role == "consumer") and not opts.queue then
      error("as a consumer, queue is required.")
   end

   if not opts.exchange then
      error("no exchange configured.")
   end
   
end

--
-- initialize the context
--
function amqp.new(opts)
   
   mandatory_options(opts)

   local sock, err = tcp()
   if not sock then
      return nil, err
   end
   
   return setmetatable( { sock = sock,
			  opts = opts,
			  connection_state = c.state.CLOSED,
			  channel_state = c.state.CLOSED,

			  major = c.PROTOCOL_VERSION_MAJOR,
			  minor = c.PROTOCOL_VERSION_MINOR,
			  revision = c.PROTOCOL_VERSION_REVISION,

			  frame_max = c.DEFAULT_FRAME_SIZE,
			  channel_max = c.DEFAULT_MAX_CHANNELS,
			  mechanism = c.MECHANISM_PLAIN
			}, mt)
end

local function sslhandshake(ctx)
   
   local sock = ctx.sock
   if _G.ngx then
      local session, err = sock:sslhandshake()
      if not session then
	 logger.error("[amqp.connect] SSL handshake failed: ", err)
      end
      return session, err
   end

   local ssl = require("ssl")
   local params = {
      mode = "client",
      protocol = "sslv23",
      verify = "none",
      options = {"all", "no_sslv2","no_sslv3"}
   }

   ctx.sock = ssl.wrap(sock, params)
   local ok, msg = ctx.sock:dohandshake()
   if not ok then
      logger.error("[amqp.connect] SSL handshake failed: ", msg)
   else
      logger.dbg("[amqp.connect] SSL handshake.")    
   end
   return ok, msg
end
--
-- connect to the broker
--
function amqp.connect(this, ...)
   local sock = this.sock
   if not sock then
      return nil, "not initialized"
   end

   local ok, err = sock:connect(...)
   if not ok then
      logger.error("[amqp.connect] failed: ", err)
      return nil, err
   end
   
   if this.opts.ssl then
      return sslhandshake(this)
   end
   return true
end

--
-- to close the socket
--
function amqp.close(this)
   local sock = this.sock
   if not sock then
      return nil, "not initialized"
   end

   return sock:close()
end


local function platform()
   if jit and jit.os and jit.arch then
      return jit.os .. "_" .. jit.arch
   end
   return "posix"
end

--
-- connection and channel
--

local function connection_start_ok(ctx)

   local user = ctx.opts.user or "guest"
   local password = ctx.opts.password or "guest"
   local f = frame.new_method_frame(c.DEFAULT_CHANNEL,
				     c.class.CONNECTION,
				     c.method.connection.START_OK)
   f.method = {
      properties = {
	 product = c.PRODUCT,
	 version = c.VERSION,
	 platform = platform(),
	 copyright = c.COPYRIGHT,
	 capabilities = {
	    authentication_failure_close = true
	 }
      },
      mechanism = ctx.mechanism,
      response = format("\0%s\0%s",user,password),
      locale = c.LOCALE
   }

   return frame.wire_method_frame(ctx,f)
end

local function connection_tune_ok(ctx)

   local f = frame.new_method_frame(c.DEFAULT_CHANNEL,
				     c.class.CONNECTION,
				     c.method.connection.TUNE_OK)

   f.method = {
      channel_max = ctx.channel_max or c.DEFAULT_MAX_CHANNELS,
      frame_max = ctx.frame_max or c.DEFAULT_FRAME_SIZE,
      heartbeat = ctx.opts.heartbeat or c.DEFAULT_HEARTBEAT
   }

   local msg = f:encode()
   local sock = ctx.sock
   local bytes,err = sock:send(msg)
   if not bytes then
      return nil,"[connection_tune_ok]" .. err
   end
   logger.info("[connection_tune_ok] wired a frame.", "[class_id]: ", f.class_id, "[method_id]: ", f.method_id)
   return true
end

local function connection_open(ctx)
   local f = frame.new_method_frame(c.DEFAULT_CHANNEL,
				     c.class.CONNECTION,
				     c.method.connection.OPEN)
   f.method = {
      virtual_host = ctx.opts.virtual_host or "/"
   }

   return frame.wire_method_frame(ctx,f)
end


local function sanitize_close_reason(ctx,reason)

   reason = reason or {}
   local ongoing = ctx.ongoing or {}
   return {
      reply_code = reason.reply_code or c.err.CONNECTION_FORCED,
      reply_text = reason.reply_text or "",
      class_id = ongoing.class_id or 0,
      method_id = ongoing.method_id or 0
   }
end


local function connection_close(ctx, reason)
   
   local f = frame.new_method_frame(c.DEFAULT_CHANNEL,
				     c.class.CONNECTION,
				     c.method.connection.CLOSE)

   f.method = sanitize_close_reason(ctx,reason)
   return frame.wire_method_frame(ctx,f)
end


local function connection_close_ok(ctx)
   local f = frame.new_method_frame(ctx.channel or 1,
				     c.class.CONNECTION,
				     c.method.connection.CLOSE_OK)
   
   return frame.wire_method_frame(ctx,f)
end

local function channel_open(ctx)
   
   local f = frame.new_method_frame(ctx.opts.channel or 1,
				     c.class.CHANNEL,
				     c.method.channel.OPEN)
   local msg = f:encode()
   local sock = ctx.sock
   local bytes,err = sock:send(msg)
   if not bytes then
      return nil,"[channel_open]" .. err
   end

   logger.info("[channel_open] wired a frame.", "[class_id]: ", f.class_id, "[method_id]: ", f.method_id)
   local res = frame.consume_frame(ctx)
   if res then
      logger.dbg("[channel_open] channel: ",res.channel)
   end
   return res
end

local function channel_close(ctx, reason)
   
   local f = frame.new_method_frame(c.DEFAULT_CHANNEL,
				     c.class.CHANNEL,
				     c.method.channel.CLOSE)

   f.method = sanitize_close_reason(ctx,reason)
   return frame.wire_method_frame(ctx,f)
end

local function channel_close_ok(ctx)
   local f = frame.new_method_frame(ctx.channel or 1,
				     c.class.CHANNEL,
				     c.method.channel.CLOSE_OK)
   
   return frame.wire_method_frame(ctx,f)
end

local function is_version_acceptable(ctx,major,minor)
   return ctx.major == major and ctx.minor == minor
end

local function is_mechanism_acceptable(ctx,method)
   local mechanism = method.mechanism
   if not mechanism then
      return nil, "broker does not support any mechanism."
   end

   for me in gmatch(mechanism, "%S+") do
      if me == ctx.mechanism then
	 return true
      end
   end
   
   return nil, "mechanism does not match"
end

local function verify_capablities(ctx,method)

   if not is_version_acceptable(ctx,method.major,method.minor) then
      return nil, "protocol version does not match."
   end

   if not is_mechanism_acceptable(ctx,method) then
      return nil, "mechanism does not match."
   end
   return true
end


local function negotiate_connection_tune_params(ctx,method)
   if not method then
      return
   end

   if method.channel_max ~= nil and method.channel_max ~= 0 then
      -- 0 means no limit
      ctx.channel_max = min(ctx.channel_max, method.channel_max)
   end

   if method.frame_max ~= nil and method.frame_max ~= 0 then
      ctx.frame_max = min(ctx.frame_max, method.frame_max)
   end
   
end

local function set_state(ctx, channel_state, connection_state)
   ctx.channel_state = channel_state
   ctx.connection_state = connection_state
end

function amqp.setup(this)
   
   local sock = this.sock
   if not sock then
      return nil, "not initialized"
   end

   -- 30 seconds read timeout
   sock:settimeout(30)
   
   local res, err = frame.wire_protocol_header(this)
   if not res then
      logger.error("[amqp.setup] wire_protocol_header failed: " .. err)
      return nil, err
   end

   if res.method then
      logger.dbg("[amqp.setup] connection_start: ",res.method)
      local ok, err = verify_capablities(this,res.method)
      if not ok then
	 -- in order to close the socket without sending futher data
	 set_state(this,c.state.CLOSED, c.state.CLOSED)
	 return nil, err
      end
   end

   local res, err = connection_start_ok(this)
   if not res then
      logger.error("[amqp.setup] connection_start_ok failed: " .. err)
      return nil, err
   end

   negotiate_connection_tune_params(this,res.method)   

   local res, err = connection_tune_ok(this)
   if not res then
      logger.error("[amqp.setup] connection_tune_ok failed: " .. err)
      return nil, err
   end
   
   local res, err = connection_open(this)
   if not res then
      logger.error("[amqp.setup] connection_open failed: " .. err)
      return nil, err
   end
   
   this.connection_state = c.state.ESTABLISHED
   
   local res, err = channel_open(this)
   if not res then
      logger.error("[amqp.setup] channel_open failed: " .. err)
      return nil, err
   end
   this.channel_state = c.state.ESTABLISHED
   return true
end

--
-- close channel and connection if needed.
--
function amqp.teardown(this,reason)

   if this.channel_state == c.state.ESTABLISHED then
      local ok, err = channel_close(this,reason)
      if not ok then
	 logger.error("[channel_close] err: ",err)
      end
   elseif this.channel_state == c.state.CLOSE_WAIT then
      local ok, err = channel_close_ok(this)
      if not ok then
	 logger.error("[channel_close_ok] err: ",err)
      end
	 
   end

   if this.connection_state == c.state.ESTABLISHED then
      local ok, err = connection_close(this,reason)
      if not ok then
	 logger.error("[connection_close] err: ",err)
      end
   elseif this.connection_state == c.state.CLOSE_WAIT then
      local ok, err = connection_close_ok(this)
      if not ok then
	 logger.error("[connection_close_ok] err: ",err)
      end
   end

end

--
-- initialize the consumer
--
local function prepare_to_consume(ctx)

   if ctx.channel_state ~= c.state.ESTABLISHED then
      return nil, "[prepare_to_consume] channel is not open."
   end

   local res, err = amqp.queue_declare(ctx)
   if not res then
      logger.error("[prepare_to_consume] queue_declare failed: " .. err)
      return nil, err
   end

   local res, err = amqp.queue_bind(ctx)
   if not res then
      logger.error("[prepare_to_consume] queue_bind failed: " .. err)
      return nil, err
   end
   
   local res, err = amqp.basic_consume(ctx)
   if not res then
      logger.error("[prepare_to_consume] basic_consume failed: " .. err)
      return nil, err
   end

   return true
end

--
-- conclude a heartbeat timeout
-- if and only if we see ctx.threshold or more failure heartbeats in the recent heartbeats ctx.window
--

local function timedout(ctx, timeouts)
   local window = ctx.window or 5
   local threshold = ctx.threshold or 4
   local c = 0
   for i = 1, window do
      if band(rshift(timeouts,i-1),1) ~= 0 then
	 c = c + 1
      end
   end
   return c >= threshold
end

local function error_string(err)
   
   if err then
      return err
   end
   return "?"
end

      
--
-- consumer
--
function amqp.consume(this)

   local ok, err = this:setup()
   if not ok then
      this:teardown()
      return nil, err
   end

   local ok, err = prepare_to_consume(this)
   if not ok then
      this:teardown()
      return nil, err
   end

   local hb = {
      last = os.time(),
      timeouts = 0
   }
   
   while true do
--
      ::continue::
--
	 
      local f, err = frame.consume_frame(this)
      if not f then
	 logger.error("[amqp.consume]",error_string(err))

	 if err == "closed" then
	    set_state(this, c.state.CLOSED, c.state.CLOSED)
	    logger.error("[amqp.consume] socket closed.")
	    break
	 end

	 if err == "wantread" then
	    set_state(this, c.state.CLOSED, c.state.CLOSED)
	    logger.error("[amqp.consume] SSL socket needs to dohandshake again.")
	    break
	 end
	 
	 -- intented timeout?
	 local now = os.time()
	 if now - hb.last > c.DEFAULT_HEARTBEAT then
	    logger.info("[amqp.consume] timeouts inc. [ts]: ",now)
	    hb.timeouts = bor(lshift(hb.timeouts,1),1)
	    hb.last = now
	    local ok, err = frame.wire_heartbeat(this)
	    if not ok then
	       logger.info("[heartbeat]","pong error: " .. error_string(err) .. "[ts]: ", hb.last)
	    else
	       logger.info("[heartbeat]","pong sent. [ts]: ",hb.last)
	    end
	 end

	 if timedout(this,hb.timeouts) then
	    logger.error("[amqp.consume] timedout. [ts]: " .. now)
	    break
	 end

	 logger.info("[amqp.consume] continue consuming")
	 goto continue
      end
      
      if f.type == c.frame.METHOD_FRAME then

	 if f.class_id == c.class.CHANNEL then
	    if f.method_id == c.method.channel.CLOSE then
	       set_state(this, c.state.CLOSE_WAIT, this.connection_state)
	       logger.info("[channel close method]", f.method.reply_code, f.method.reply_text)
	       break
	    end
	 elseif f.class_id == c.class.CONNECTION then
	    if f.method_id == c.method.connection.CLOSE then
	       set_state(this, c.state.CLOSED, c.state.CLOSE_WAIT)
	       logger.info("[connection close method]", f.method.reply_code, f.method.reply_text)
	       break
	    end
	 elseif f.class_id == c.class.BASIC then
	    if f.method_id == c.method.basic.DELIVER then
	       if f.method ~= nil then
		  logger.info("[basic_deliver] ", f.method)
	       end
	    end
	 end
      elseif f.type == c.frame.HEADER_FRAME then
	 logger.info(format("[header] class_id: %d weight: %d, body_size: %d",
			    f.class_id, f.weight, f.body_size))
	 logger.info("[frame.properties]",f.properties)
      elseif f.type == c.frame.BODY_FRAME then
	 
	 if this.opts.callback then
	    this.opts.callback(f.body)
	 end
	 logger.info("[body]",f.body)
      elseif f.type == c.frame.HEARTBEAT_FRAME then
	 hb.last = os.time()
	 logger.info("[heartbeat]","ping received. [ts]: ",hb.last)
	 hb.timeouts = band(lshift(hb.timeouts,1),0)
	 local ok, err = frame.wire_heartbeat(this)
	 if not ok then
	    logger.info("[heartbeat]","pong error: " .. error_string(err) .. "[ts]: ", hb.last)
	 else
	    logger.info("[heartbeat]","pong sent. [ts]: ",hb.last)
	 end
      end
   end

   this:teardown()
   return nil
end

--
-- publisher
--

function amqp.publish(this,payload)

   local size = #payload
   local ok, err = amqp.basic_publish(this)
   if not ok then
      logger.error("[amqp.publish] failed: " .. err)
      return nil, err
   end

   local ok, err = frame.wire_header_frame(this,size)
   if not ok then
      logger.error("[amqp.publish] failed: " .. err)
      return nil, err
   end
   
   local ok, err = frame.wire_body_frame(this,payload)
   if not ok then
      logger.error("[amqp.publish] failed: " .. err)
      return nil, err
   end
   
   return true
end

--
-- queue
--
function amqp.queue_declare(ctx,opts)

   opts = opts or {}

   if not opts.queue and not ctx.opts.queue then
      return nil, "[queue_declare] queue is not specified."
   end
   
   local f = frame.new_method_frame(ctx.channel or 1,
				     c.class.QUEUE,
				     c.method.queue.DECLARE)

   f.method = {
      queue = opts.queue or ctx.opts.queue,
      passive = opts.passive or false,
      durable = opts.durable or false,
      exclusive = opts.exclusive or false,
      auto_delete = opts.auto_delete or true,
      no_wait = ctx.opts.no_wait or true
   }
   return frame.wire_method_frame(ctx,f)
end

function amqp.queue_bind(ctx,opts)

   opts = opts or {}

   if not opts.queue and not ctx.opts.queue then
      return nil, "[queue_bind] queue is not specified."
   end
   
   local f = frame.new_method_frame(ctx.channel or 1,
				     c.class.QUEUE,
				     c.method.queue.BIND)

   f.method = {
      queue = opts.queue or ctx.opts.queue,
      exchange = opts.exchange or ctx.opts.exchange,
      routing_key = opts.routing_key or "",
      no_wait = ctx.opts.no_wait or false
   }

   return frame.wire_method_frame(ctx,f)
end

function amqp.queue_unbind(ctx,opts)

   opts = opts or {}

   if not opts.queue and not ctx.opts.queue then
      return nil, "[queue_unbind] queue is not specified."
   end
   
   local f = frame.new_method_frame(ctx.channel or 1,
				     c.class.QUEUE,
				     c.method.queue.UNBIND)

   f.method = {
      queue = opts.queue or ctx.opts.queue,
      exchange = opts.exchange or ctx.opts.exchange,
      routing_key = opts.routing_key or "",
   }

   return frame.wire_method_frame(ctx,f)
end

function amqp.queue_delete(ctx,opts)

   opts = opts or {}

   if not opts.queue and not ctx.opts.queue then
      return nil, "[queue_delete] queue is not specified."
   end

   local f = frame.new_method_frame(ctx.channel or 1,
				     c.class.QUEUE,
				     c.method.queue.DELETE)

   f.method = {
      queue = opts.queue or ctx.opts.queue,
      if_unused = opts.if_unused or false,
      if_empty = opts.if_empty or false,
      no_wait = ctx.opts.no_wait or false
   }

   return frame.wire_method_frame(ctx,f)
end

--
-- exchange
--
function amqp.exchange_declare(ctx,opts)

   opts = opts or {}

   local f = frame.new_method_frame(ctx.channel or 1,
				     c.class.EXCHANGE,
				     c.method.exchange.DECLARE)


   
   f.method = {
      exchange = opts.exchange or ctx.opts.exchange,
      typ = opts.typ or "topic",
      passive = opts.passive or false,
      durable = opts.passive or false,
      auto_delete = opts.auto_delete or false,
      internal = opts.internal or false,
      no_wait = ctx.opts.no_wait or false
   }

   return frame.wire_method_frame(ctx,f)
end

function amqp.exchange_bind(ctx,opts)

   if not opts then
      return nil, "[exchange_bind] opts is required."
   end

   if not opts.source then
      return nil, "[exchange_bind] source is required."
   end

   if not opts.destination then
      return nil, "[exchange_bind] destination is required."
   end

   local f = frame.new_method_frame(ctx.channel or 1,
				     c.class.EXCHANGE,
				     c.method.exchange.BIND)
   
   f.method = {
      destination = opts.destination,
      source = opts.source,
      routing_key = opts.routing_key or "",
      no_wait = ctx.opts.no_wait or false
   }

   return frame.wire_method_frame(ctx,f)
end

function amqp.exchange_unbind(ctx,opts)

   if not opts then
      return nil, "[exchange_unbind] opts is required."
   end

   if not opts.source then
      return nil, "[exchange_unbind] source is required."
   end

   if not opts.destination then
      return nil, "[exchange_unbind] destination is required."
   end

   local f = frame.new_method_frame(ctx.channel or 1,
				     c.class.EXCHANGE,
				     c.method.exchange.UNBIND)
   
   f.method = {
      destination = opts.destination,
      source = opts.source,
      routing_key = opts.routing_key or "",
      no_wait = ctx.opts.no_wait or false
   }

   return frame.wire_method_frame(ctx,f)
end

function amqp.exchange_delete(ctx,opts)

   opts = opts or {}

   local f = frame.new_method_frame(ctx.channel or 1,
				     c.class.EXCHANGE,
				     c.method.exchange.DELETE)
   
   f.method = {
      exchange = opts.exchange or ctx.opts.exchange,
      if_unused = opts.is_unused or true,
      no_wait = ctx.opts.no_wait or false
   }

   return frame.wire_method_frame(ctx,f)
end

--
-- basic
--
function amqp.basic_consume(ctx,opts)

   opts = opts or {}

   if not opts.queue and not ctx.opts.queue then
      return nil, "[basic_consume] queue is not specified."
   end

   local f = frame.new_method_frame(ctx.channel or 1,
				     c.class.BASIC,
				     c.method.basic.CONSUME)

   f.method = {
      queue = opts.queue or ctx.opts.queue,
      no_local = opts.no_local or false,
      no_ack = opts.no_ack or true,
      exclusive = opts.exclusive or false,
      no_wait = ctx.opts.no_wait or false
   }

   return frame.wire_method_frame(ctx,f)
end


function amqp.basic_publish(ctx,opts)

   opts = opts or {}
   
   local f = frame.new_method_frame(ctx.channel or 1,
				     c.class.BASIC,
				     c.method.basic.PUBLISH)
   f.method = {
      exchange = opts.exchange or ctx.opts.exchange,
      routing_key = opts.routing_key or ctx.opts.routing_key or "",
      mandatory = opts.mandatory or false,
      immediate = opts.immediate or false
   }

   local msg = f:encode()
   local sock = ctx.sock
   local bytes,err = sock:send(msg)
   if not bytes then
      return nil,"[basic_publish]" .. err
   end
   return bytes
end

return amqp

