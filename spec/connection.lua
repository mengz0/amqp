require 'busted.runner'()

describe("[lua-amqp]", function()
	    describe("client", function()
			local logger = require("logger")
			logger.set_level(7)
			
			it("should be rejected with wrong user name or password", function()

			      local amqp = require("amqp")
			      local ctx = amqp.new({
				    role = "consumer",
				    queue = "mengz0",
				    exchange = "amq.topic",
				    ssl = false,
				    user = "guest",
				    password = "guest0"})
			      
			      local ok, err = ctx:connect("127.0.0.1",5672)
			      assert.truthy(ok)
			      local ok, err = ctx:setup()
			      -- expects access denied
			      assert.is_equal(403,err)
			      assert.falsy(ok)
			      ctx:teardown()
			      ctx:close()
			end)

	    end)	    

end)
