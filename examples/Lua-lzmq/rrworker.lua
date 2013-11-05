-- Hello World worker
-- Connects REP socket to tcp://*:5560
-- Expects "Hello" from client, replies with "World"

require "zhelpers"
local zmq     = require "lzmq"
local zpoller = require "lzmq.poller"

local context = zmq.context()

-- Socket to talk to clients
local responder, err = context:socket{zmq.REP, connect = "tcp://localhost:5560"}
zassert(responder, err)

while true do
  -- Wait for next request from client
  local message = responder:recv()
  printf ("Received request: [%s]\n", message)

  -- Do some 'work'
  sleep(1)

  -- Send reply back to client
  responder:send("World")
end
