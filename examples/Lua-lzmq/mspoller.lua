require "zhelpers"
local zmq = require "lzmq"
local zpoller = require "lzmq.poller"

local context = zmq.context()

local receiver, err = context:socket(zmq.PULL,{connect = "tcp://localhost:5557"})
zassert(receiver, err)

local subscriber, err = context:socket(zmq.SUB, {
  subscribe = "10001 ";
  connect   = "tcp://localhost:5556";
})
zassert(subscriber, err)

local poller = zpoller.new(2)

poller:add(receiver, zmq.POLLIN, function()
  local msg = receiver:recv()
  -- Process task
end)

poller:add(subscriber, zmq.POLLIN, function()
  local msg = subscriber:recv()
  -- Process weather update
end)

-- Process messages from both sockets
poller:start()
