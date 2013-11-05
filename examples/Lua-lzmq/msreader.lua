-- Reading from multiple sockets
-- This version uses a simple recv loop

require "zhelpers"
local zmq = require "lzmq"

local context = zmq.context()

-- Connect to task ventilator
local receiver, err = context:socket{zmq.PULL,connect = "tcp://localhost:5557"}
zassert(receiver, err)

-- Connect to weather server
local subscriber, err = context:socket{zmq.SUB,
  subscribe = "10001 ";
  connect   = "tcp://localhost:5556";
}
zassert(subscriber, err)

-- Process messages from both sockets
-- We prioritize traffic from the task ventilator
while true do
  while true do
    local msg = receiver:recv(zmq.DONTWAIT)
    if not msg then break end
    -- Process task
  end

  while true do
    local msg = subscriber:recv(zmq.DONTWAIT)
    if not msg then break end
    -- Process weather update
  end

  -- No activity, so sleep for 1 msec
  s_sleep (1);
end
