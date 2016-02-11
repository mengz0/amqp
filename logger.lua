--
-- Copyright (C) 2016 Meng Zhang @ Yottaa,Inc
--
-- logger
--

module("logger",package.seeall)

local logger = {}


-- logging scaffold
local log = nil
if _G.ngx and _G.ngx.log then
   log = ngx.log
else
   log = print
end

-- logging level for print
local ERR   = 4
local INFO  = 7
local DEBUG = 8

-- ngx.log requires a number to indicate the logging level
if _G.ngx then
   ERR   = ngx.ERR
   INFO  = ngx.INFO
   DEBUG = ngx.DEBUG
end

local level_ = DEBUG

local function to_string(v)
   if v == nil then
      return ""
   end

   if type(v) ~= "table" then
      return tostring(v)
   end

   local s = "["
   for k,v in pairs(v) do
      if k ~= nil then
	 s = s .. to_string(k) .. ":"
      end
      if v ~= nil then
	 s = s .. to_string(v)
      end
      s = s .. " "
   end
   s = s .. "]"
   return s
end

local function va_table_to_string(tbl)
   local res = ""
   for k,v in pairs(tbl) do
      res = res .. to_string(v) .. "\t"
   end
   return res
end

function logger.set_level(level)
   level_ = level
end

function logger.error(...)
   log(ERR,va_table_to_string({...}))
end

function logger.info(...)
   if level_ < INFO then
      return
   end
   log(INFO,va_table_to_string({...}))
end

function logger.dbg(...)
   if level_ ~= DEBUG then
      return
   end

   log(DEBUG,va_table_to_string({...}))
end

return logger

