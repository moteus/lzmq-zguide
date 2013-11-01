require "zhelpers"
local zmq = require "lzmq"

-- Prepare our context and publisher
local context = zmq.context()
local subscriber, err = context:socket(zmq.SUB, {
  subscribe = "B";
  connect   = "tcp://localhost:5563";
})
zassert(subscriber, err)

while true do
  -- Read envelope with address
  local address = subscriber:recv()
  -- Read message contents
  local contents = subscriber:recv()
  printf ("[%s] %s\n", address, contents);
end
