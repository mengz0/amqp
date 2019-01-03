
module("bitops",package.seeall)

-- Try to load bit library
local bit
local have_bit = pcall(function() bit = require "bit" end)

-- If we have the bit library, use that, otherwise, use built-in operators
-- (which are only available in Lua 5.3)
if have_bit then
  return {
    band = bit.band,
    bor = bit.bor,
    lshift = bit.lshift,
    rshift = bit.rshift,
    tohex = bit.tohex
  }
else
  return {
    band = function(a,b) return a & b end,
    bor = function(a,b) return a | b end,
    lshift = function(a,b) return a << b end,
    rshift = function(a,b) return a >> b end,
    tohex = function(a) return string.format("%x", a) end
  }
end

