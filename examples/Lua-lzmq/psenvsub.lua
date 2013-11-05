-- Pubsub envelope subscriber

require "zhelpers"
local zmq = require "lzmq"

-- Prepare our context and publisher
local context = zmq.context()
local subscriber, err = context:socket{zmq.SUB,
  subscribe = "B";
  connect   = "tcp://localhost:5563";
}
zassert(subscriber, err)

while true do
  -- Read envelope with address and message contents
  local address, contents = subscriber:recvx()
  printf ("[%s] %s\n", address, contents);
end
