--
-- Copyright (C) 2016 Meng Zhang @ Yottaa,Inc
--

module("buffer",package.seeall)

local logger = require "logger"

-- Try to load bit library
local bit = require("bitopers")

local concat = table.concat
local sub = string.sub
local byte = string.byte
local char = string.char
local format = string.format

local ok, new_tab = pcall(require, "table.new")
if not ok or type(new_tab) ~= "function" then
   new_tab = function (narr, nrec) return {} end
end

local _M = {}

local mt = { __index = _M }

function _M.new(b,pos)
   return setmetatable( {
	 buffer_ = b or "",
	 pos_ = pos or 1
			},
      mt)
end

function _M:hex_dump()
    local len = #self.buffer_
    local bytes = new_tab(len, 0)
    for i = 1, len do
        bytes[i] = bit.tohex(byte(self.buffer_, i), 2)
    end
    return concat(bytes, " ")
end

function _M:get_i8()
   local b = byte(self.buffer_, self.pos_)
   self.pos_ = self.pos_ + 1
   return b
end

function _M:get_bool()
   local v = self:get_i8()
   return v and v ~= 0
end

function _M:get_i16()
    local a0, a1 = byte(self.buffer_, self.pos_, self.pos_ + 1)
    local r = bit.bor(a1, bit.lshift(a0, 8))
    self.pos_ = self.pos_ + 2
    return r
end


function _M:get_i24()
   local a0, a1, a2 = byte(self.buffer_, self.pos_, self.pos_ + 2)
   self.pos_ = self.pos_ + 3
   return bit.bor(a2,
	      bit.lshift(a1, 8),
	      bit.lshift(a0, 16))
end


function _M:get_i32()
   local a0, a1, a2, a3 = byte(self.buffer_, self.pos_, self.pos_ + 3)
   self.pos_ = self.pos_ + 4
   return bit.bor(a3,
	      bit.lshift(a2, 8),
	      bit.lshift(a1, 16),
	      bit.lshift(a0, 24))
end


function _M:get_i64()
    local a, b, c, d, e, f, g, h = byte(self.buffer_, self.pos_, self.pos_ + 7)
    self.pos_ = self.pos_ + 8

    local lo = bit.bor(h, bit.lshift(g, 8), bit.lshift(f, 16), bit.lshift(e, 24))
    local hi = bit.bor(d, bit.lshift(c, 8), bit.lshift(b, 16), bit.lshift(a, 24))
    return lo + hi * 4294967296
end

function _M:get_short_string()
   local length = self:get_i8()
   local tail = self.pos_+length-1
   local s = sub(self.buffer_,self.pos_,tail)
   self.pos_ = tail + 1
   return s
end

function _M:get_long_string()
   local length = self:get_i32()
   local tail = self.pos_+length-1
   local s = sub(self.buffer_,self.pos_,tail)
   self.pos_ = tail + 1
   return s
end

function _M:get_decimal()
   local scale = self:get_i8()
   local value = self:get_i32()
   local d = {scale = sacle, value = value}
   return d
end

function _M:get_f32()
   return self:get_i32()
end

function _M:get_f64()
   return self:get_i64()
end

function _M:get_timestamp()
   return self:get_i64()
end

function _M:get_field_array()
   local size = self:get_i32()
   logger.dbg("[array] size: " .. size)
   local a = {}
   local p = self.pos_
   while size > 0 do
      f = self:field_value()
      a[#a+1] = f
      size = size - (self.pos_ - p)
      p = self.pos_
   end
   return a
end


function _M:get_field_table()
   local size = self:get_i32()
   local r = {}
   local p = self.pos_
   local k,v
   while size > 0 do
      k = self:get_short_string()
--      logger.dbg("k",k)
      v = self:field_value()
      size = size - self.pos_ + p
      p = self.pos_
      r[k] = v
   end

   return r,pos_
end


function _M:put_i8(i)
   self.buffer_ = self.buffer_ .. char(bit.band(i,0x0ff))
end

function _M:put_bool(b)
   local v = 0
   if b and b ~= 0 then
      v = 1
   end
   self:put_i8(v)
end

function _M:put_i16(i)
   self.buffer_ = self.buffer_ ..
      char(bit.rshift(bit.band(i,0xff00),8)) ..
      char(bit.band(i,0x0ff))

end

function _M:put_i32(i)
   self.buffer_ = self.buffer_ ..
      char(bit.rshift(bit.band(i,0xff000000),24)) ..
      char(bit.rshift(bit.band(i, 0x00ff0000),16)) ..
      char(bit.rshift(bit.band(i, 0x0000ff00),8)) ..
      char(bit.band(i, 0x000000ff))
end

function _M:put_i64(i)

   -- rshift has not support for 64bit?
   -- side effect is that it will rotate for shifts bigger than 32bit
   local hi = bit.band(math.floor(i/4294967296),0x0ffffffff)
   local lo = bit.band(i, 0x0ffffffff)
   self:put_i32(hi)
   self:put_i32(lo)
end

function _M:put_f32(i)
   self:put_i32(i)
end

function _M:put_f64(i)
   self:put_i64(i)
end

function _M:put_timestamp(i)
   self:put_i64(i)
end

function _M:put_decimal(d)
   self:put_i8(d.scale)
   self:put_i32(d.value)
end

function _M:put_short_string(s)
   local len = #s
   self:put_i8(len)
   self.buffer_ = self.buffer_ .. s
end

function _M:put_long_string(s)
   local len = #s
   self:put_i32(len)
   self.buffer_ = self.buffer_ .. s
end

function _M:put_payload(payload)
   self.buffer_ = self.buffer_ .. payload
end

local function is_array(t)
   local i = 0
   for _ in pairs(t) do
      i = i + 1
      if t[i] == nil then return false end
   end
   return true
end

function _M:put_field_array(a)
   local b = _M.new()
   for i = 1, #a do
      b:put_field_value(a[i])
   end

   self:put_i32(#b.buffer_)
   self:put_payload(b.buffer_)
end


function _M:put_field_table(tab)
   local b = _M.new()
   for k,v in pairs(tab) do
      b:put_short_string(k)
      b:put_field_value(v)
   end

   self:put_i32(#b.buffer_)
   self:put_payload(b.buffer_)
end


local fields_ = {
   t = {
      r = function (self)
	 local b =  self:get_i8()
	 return b ~= 0
      end,
      w = function (self,val)
	 local b = 0
	 if val ~= 0 then
	    b = 1
	 end
	 self:put_i8(b)
      end
   },
   b = {
      r = _M.get_i8,
      w = _M.put_i8
   },
   B = {
      r = _M.get_i8,
      w = _M.put_i8
   },
   U = {
      r = _M.get_i16,
      w = _M.put_i16
   },
   u = {
      r = _M.get_i16,
      w = _M.put_i16
   },
   I = {
      r =  _M.get_i32,
      w = _M.put_i32
   },
   i = {
      r = _M.get_i32,
      w = _M.put_i32
   },
   L = {
      r = _M.get_i64,
      w = _M.put_i64
   },
   l = {
      r = _M.get_i64,
      w = _M.put_i64
   },
   f = {
      r = _M.get_f32,
      w = _M.put_f32
   },
   d = {
      r = _M.get_f64,
      w = _M.put_f64
   },
   D = {
      r = _M.get_decimal,
      w = _M.put_decimal
   },
   s = {
      r = _M.get_short_string,
      w = _M.put_short_string
   },
   S = {
      r = _M.get_long_string,
      w = _M.put_long_string
   },
   A = {
      r = _M.get_field_array,
      w = _M.put_field_array
   },
   T = {
      r = _M.get_timestamp,
      w = _M.put_timestamp
   },
   F = {
      r = _M.get_field_table,
      w = _M.put_field_table
   },
   V = {
      r = function(self)
	 return nil
      end,
      w = nil
   }
}

function _M:field_value()
   local typ = self:get_i8()
   local codec = fields_[char(typ)]
   if not codec then
      local err = format("codec[%d] not found.",typ)
      logger.error("[field_value] " .. err)
      return nil,err
   end
   return codec.r(self)
end


function _M:put_field_value(value)

   -- FIXME: to detect the type of the value
   local t = type(value)
   local typ = nil
   if t == 'number' then
      typ = 'I' -- assume to be i32
   elseif t == 'boolean' then
      typ = 't'
   elseif t == 'string' then
      typ = 'S'
   elseif t == 'table' then
      typ = 'F'
      if is_array(value) then
	 typ = 'A'
      end
   end

   -- wire the type
   self:put_i8(byte(typ))
   local codec = fields_[typ]
   if not codec then
      local err = format("codec[%d] not found.",typ)
      logger.error("[field_value] " .. err)
      return nil,err
   end
   codec.w(self,value)
end

function _M:payload()
   return self.buffer_
end

return _M
