require 'busted.runner'()

describe("[lua-amqp]", function()
	    describe("client", function()
			local logger = require("logger")
			logger.set_level(7)

			it("should be able to publish messages", function()
			      
			      local amqp = require("amqp")
			      local ctx = amqp.new({
				    role = "producer",
				    exchange = "amq.topic",
				    ssl = false,
				    user = "guest",
				    password = "guest"})
			      
			      local ok, err = ctx:connect("127.0.0.1",5672)
			      assert.truthy(ok)
			      local ok, err = ctx:setup()
			      assert.truthy(ok)
			      local ok, err = ctx:publish("Hello world!")
			      assert.truthy(ok)
			      ctx:teardown()
			      ctx:close()
			end)

	    end)	    

end)
