require "zhelpers"
local zmq = require "lzmq"
local zpoller = require "lzmq.poller"

local context = zmq.context()

-- Socket to receive messages on
local receiver, err = context:socket(zmq.PULL, { connect = "tcp://localhost:5557" })
zassert(receiver, err)

-- Socket to send messages to
local sender, err = context:socket(zmq.PUSH, { connect = "tcp://localhost:5558" })
zassert(sender, err)

-- Socket to send messages to
local controller, err = context:socket(zmq.SUB, {
  subscribe = "";
  connect   = "tcp://localhost:5559"
})
zassert(controller, err)

local poller = zpoller.new(2)

poller:add(receiver, zmq.POLLIN, function()
  local message = receiver:recv()
  printf("%s.", message) -- Show progress
  io.flush()
  s_sleep(tonumber(message)) -- Do the work
  sender:send("") -- Send results to sink
end)

poller:add(controller, zmq.POLLIN, function()
  poller:stop()
end)

poller:start()
