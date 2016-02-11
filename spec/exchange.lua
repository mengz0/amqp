require 'busted.runner'()

describe("[lua-amqp]", function()
	    describe("client", function()
			local logger = require("logger")
			logger.set_level(7)

			local options = {
				    role = "producer",
				    exchange = "amq.topic",
				    ssl = false,
				    user = "guest",
				    password = "guest"
			}

			it("should be able to declare exchange", function()
			      local amqp = require("amqp")
			      local ctx = amqp.new(options)
			      local ok, err = ctx:connect("127.0.0.1",5672)
			      assert.truthy(ok)
			      local ok, err = ctx:setup()
			      assert.truthy(ok)
			      local ok, err = amqp.exchange_declare(ctx,{
								       exchange = "topic.mengz",
								       passive = false,
								       durable = true,
								       internal = false,
								       auto_delete = true
			      })
			      assert.truthy(ok)
			      ctx:teardown()
			      ctx:close()
			end)

			it("should be able to bind exchange", function()
			      local amqp = require("amqp")
			      local ctx = amqp.new(options)
			      local ok, err = ctx:connect("127.0.0.1",5672)
			      assert.truthy(ok)
			      local ok, err = ctx:setup()
			      assert.truthy(ok)

			      local ok, err = amqp.exchange_bind(ctx,{
								    source = "amq.topic",
								    destination = "topic.mengz",
								    routing_key = "Kiwi"
			      })
			      assert.truthy(ok)
			      ctx:teardown()
			      ctx:close()

			end)

			it("should be able to unbind exchange", function()

			      local amqp = require("amqp")
			      local ctx = amqp.new(options)
			      local ok, err = ctx:connect("127.0.0.1",5672)
			      assert.truthy(ok)
			      local ok, err = ctx:setup()
			      assert.truthy(ok)


			      local ok, err = amqp.exchange_unbind(ctx,{
								      source = "amq.topic",
								      destination = "topic.mengz",
								      routing_key = "Kiwi"
			      })

			      assert.truthy(ok)
			      ctx:teardown()
			      ctx:close()

			end)



			it("should be able to delete exchange", function()

			      local amqp = require("amqp")
			      local ctx = amqp.new(options)
			      local ok, err = ctx:connect("127.0.0.1",5672)
			      assert.truthy(ok)
			      local ok, err = ctx:setup()
			      assert.truthy(ok)

			      local ok, err = amqp.exchange_delete(ctx,{
								      exchange = "topic.mengz"
								      
			      })
			      assert.truthy(ok)

			      ctx:teardown()
			      ctx:close()
			
			end)

			it("should succeed to delete non existing exchanges", function()

			      local amqp = require("amqp")
			      local ctx = amqp.new(options)
			      local ok, err = ctx:connect("127.0.0.1",5672)
			      assert.truthy(ok)
			      local ok, err = ctx:setup()
			      assert.truthy(ok)

			      local ok, err = amqp.exchange_delete(ctx,{
								      exchange = "topic.mengz"
								      
			      })
			      assert.truthy(ok)

			      ctx:teardown()
			      ctx:close()
			
			end)

		
	    end)

end)
