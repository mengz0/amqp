--
-- Copyright (C) 2016 Meng Zhang @ Yottaa,Inc
--

module("buffer",package.seeall)

local logger = require "logger"
local bit = require "bit"
local band = bit.band
local bor = bit.bor
local lshift = bit.lshift
local rshift = bit.rshift
local tohex = bit.tohex

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

function _M.hex_dump(this)
    local len = #this.buffer_
    local bytes = new_tab(len, 0)
    for i = 1, len do
        bytes[i] = tohex(byte(this.buffer_, i), 2)
    end
    return concat(bytes, " ")
end

function _M.get_i8(this)
   local b = byte(this.buffer_, this.pos_)
   this.pos_ = this.pos_ + 1
   return b
end

function _M.get_bool(this)
   local v = this:get_i8()
   return v and v ~= 0
end

function _M.get_i16(this)
    local a0, a1 = byte(this.buffer_, this.pos_, this.pos_ + 1)
    local r = bor(a1, lshift(a0, 8))
    this.pos_ = this.pos_ + 2
    return r
end


function _M.get_i24(this)
   local a0, a1, a2 = byte(this.buffer_, this.pos_, this.pos_ + 2)
   this.pos_ = this.pos_ + 3
   return bor(a2,
	      lshift(a1, 8),
	      lshift(a0, 16))
end


function _M.get_i32(this)
   local a0, a1, a2, a3 = byte(this.buffer_, this.pos_, this.pos_ + 3)
   this.pos_ = this.pos_ + 4
   return bor(a3,
	      lshift(a2, 8),
	      lshift(a1, 16),
	      lshift(a0, 24))
end


function _M.get_i64(this)
    local a, b, c, d, e, f, g, h = byte(this.buffer_, this.pos_, this.pos_ + 7)
    this.pos_ = this.pos_ + 8

    local lo = bor(h, lshift(g, 8), lshift(f, 16), lshift(e, 24))
    local hi = bor(d, lshift(c, 8), lshift(b, 16), lshift(a, 24))
    return lo + hi * 4294967296
end

function _M.get_short_string(this)
   local length = this:get_i8()
   local tail = this.pos_+length-1
   local s = sub(this.buffer_,this.pos_,tail)
   this.pos_ = tail + 1
   return s
end

function _M.get_long_string(this)
   local length = this:get_i32()
   local tail = this.pos_+length-1
   local s = sub(this.buffer_,this.pos_,tail)
   this.pos_ = tail + 1
   return s
end

function _M.get_decimal(this)
   local scale = this:get_i8()
   local value = this:get_i32()
   local d = {scale = sacle, value = value}
   return d
end

function _M.get_f32(this)
   return this:get_i32()
end

function _M.get_f64(this)
   return this:get_i64()
end

function _M.get_timestamp(this)
   return this:get_i64()
end

function _M.get_field_array(this)
   local size = this:get_i32()
   logger.dbg("[array] size: " .. size)
   local a = {}
   local p = this.pos_
   while size > 0 do
      f = this:field_value()
      a[#a+1] = f
      size = size - (this.pos_ - p)
      p = this.pos_
   end
   return a
end


function _M.get_field_table(this)
   local size = this:get_i32()
   local r = {}
   local p = this.pos_
   local k,v
   while size > 0 do
      k = this:get_short_string()
--      logger.dbg("k",k)
      v = this:field_value()
      size = size - this.pos_ + p
      p = this.pos_
      r[k] = v
   end

   return r,pos_
end


function _M.put_i8(this,i)
   this.buffer_ = this.buffer_ .. char(band(i,0x0ff))
end

function _M.put_bool(this,b)
   local v = 0
   if b and b ~= 0 then
      v = 1
   end
   this:put_i8(v)
end

function _M.put_i16(this,i)
   this.buffer_ = this.buffer_ ..
      char(rshift(band(i,0xff00),8)) ..
      char(band(i,0x0ff))

end

function _M.put_i32(this,i)
   this.buffer_ = this.buffer_ ..
      char(rshift(band(i,0xff000000),24)) ..
      char(rshift(band(i, 0x00ff0000),16)) ..
      char(rshift(band(i, 0x0000ff00),8)) ..
      char(band(i, 0x000000ff))
end

function _M.put_i64(this,i)

   -- rshift has not support for 64bit?
   -- side effect is that it will rotate for shifts bigger than 32bit
   local hi = band(i/4294967296,0x0ffffffff)
   local lo = band(i, 0x0ffffffff)
   this:put_i32(hi)
   this:put_i32(lo)
end

function _M.put_f32(this,i)
   this:put_i32(i)
end

function _M.put_f64(this,i)
   this:put_i64(i)
end

function _M.put_timestamp(this,i)
   this:put_i64(i)
end

function _M.put_decimal(this,d)
   this:put_i8(d.scale)
   this:put_i32(d.value)
end

function _M.put_short_string(this,s)
   local len = #s
   this:put_i8(len)
   this.buffer_ = this.buffer_ .. s
end

function _M.put_long_string(this,s)
   local len = #s
   this:put_i32(len)
   this.buffer_ = this.buffer_ .. s
end

function _M.put_payload(this,payload)
   this.buffer_ = this.buffer_ .. payload
end

local function is_array(t)
   local i = 0
   for _ in pairs(t) do
      i = i + 1
      if t[i] == nil then return false end
   end
   return true
end

function _M.put_field_array(this,a)
   local b = _M.new()
   for i = 1, #a do
      b:put_field_value(a[i])
   end

   this:put_i32(#b.buffer_)
   this:put_payload(b.buffer_)
end


function _M.put_field_table(this,tab)
   local b = _M.new()
   for k,v in pairs(tab) do
      b:put_short_string(k)
      b:put_field_value(v)
   end

   this:put_i32(#b.buffer_)
   this:put_payload(b.buffer_)
end


local fields_ = {
   t = {
      r = function (this)
	 local b =  this:get_i8()
	 return b ~= 0
      end,
      w = function (this,val)
	 local b = 0
	 if val ~= 0 then
	    b = 1
	 end
	 this:put_i8(b)
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
      r = function(this)
	 return nil
      end,
      w = nil
   }
}

function _M.field_value(this)
   local typ = this:get_i8()
   local codec = fields_[char(typ)]
   if not codec then
      local err = format("codec[%d] not found.",typ)
      logger.error("[field_value] " .. err)
      return nil,err
   end
   return codec.r(this)
end


function _M.put_field_value(this,value)

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
   this:put_i8(byte(typ))
   local codec = fields_[typ]
   if not codec then
      local err = format("codec[%d] not found.",typ)
      logger.error("[field_value] " .. err)
      return nil,err
   end
   codec.w(this,value)
end

function _M.payload(this)
   return this.buffer_
end

return _M
