-- Hello World client

require "zhelpers"
local zmq = require "lzmq"

print("Connecting to hello world server ...")
local context = zmq.context()
local requester, err = context:socket{zmq.REQ, 
  connect = "tcp://localhost:5555"
}
zassert(requester, err)

for request_nbr = 0, 9 do
  print ("Sending Hello " .. request_nbr .. "...")
  requester:send("Hello")
  local buffer = requester:recv()
  print("Received World " .. request_nbr)
end
