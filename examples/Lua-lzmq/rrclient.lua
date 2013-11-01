require "zhelpers"
local zmq = require "lzmq"
local zpoller = require "lzmq.poller"

local context = zmq.context()

-- Socket to talk to server
local requester, err = context:socket(zmq.REQ,{connect = "tcp://localhost:5559"})
zassert(requester, err)

for request_nbr = 1, 10 do
  requester:send("Hello")
  local message = requester:recv()
  printf ("Received reply %d [%s]\n", request_nbr, message)
end