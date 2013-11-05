-- Simple request-reply broker

require "zhelpers"
local zmq     = require "lzmq"
local zpoller = require "lzmq.poller"

-- Prepare our context and sockets
local context = zmq.context()

local frontend, err = context:socket{zmq.ROUTER, bind = "tcp://*:5559"}
zassert(frontend, err)

local backend, err = context:socket{zmq.DEALER, bind = "tcp://*:5560"}
zassert(backend, err)

local poller = zpoller.new(2)

poller:add(frontend, zmq.POLLIN, function()
  while true do
    -- Process all parts of the message
    local message, more = frontend:recv()
    backend:send(message, more and zmq.SNDMORE or 0)
    if not more then break end
  end
end)

poller:add(backend, zmq.POLLIN, function()
  -- Process all parts of the message (using lua table)
  local message = backend:recv_all()
  frontend:send_all(message)
end)

-- Process messages from both sockets
poller:start()
