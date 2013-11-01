require "zhelpers"
local zmq = require "lzmq"

local context = zmq.context()

local receiver, err = context:socket(zmq.PULL,{connect = "tcp://localhost:5557"})
zassert(receiver, err)

local subscriber, err = context:socket(zmq.SUB, {
  subscribe = "10001 ";
  connect   = "tcp://localhost:5556";
})
zassert(subscriber, err)

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

  s_sleep (1);
end
