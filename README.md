# AMQP
Lua Client for AMQP 0.9.1

## Requirements
1. LuaJIT 2.1
2. busted 2.0 (Testing framework)

## Typical Use Cases

+ Consumer

```lua
local amqp = require "amqp"
local ctx = amqp.new({role = "consumer", queue = "mengz0", exchange = "amq.topic", ssl = false, user = "guest", password = "guest"})
ctx:connect("127.0.0.1",5672)
local ok, err = ctx:consume()
```

+ Producer

```lua
local amqp = require "amqp"
local ctx = amqp.new({role = "publisher", exchange = "amq.topic", ssl = false, user = "guest", password = "guest"})
ctx:connect("127.0.0.1",5672)
ctx:setup()
local ok, err = ctx:publish("Hello world!")
```