-- Task worker
-- Connects PULL socket to tcp://localhost:5557
-- Collects workloads from ventilator via that socket
-- Connects PUSH socket to tcp://localhost:5558
-- Sends results to sink via that socket

require "zhelpers"
local zmq = require "lzmq"

local context = zmq.context()

-- Socket to receive messages on
local receiver, err = context:socket{zmq.PULL, connect = "tcp://localhost:5557"}
zassert(receiver, err)

-- Socket to send messages to
local sender, err = context:socket{zmq.PUSH, connect = "tcp://localhost:5558"}
zassert(sender, err)

-- Process tasks forever
while true do
  local message = receiver:recv()
  printf("%s.", message) -- Show progress
  io.flush()
  s_sleep(tonumber(message)) -- Do the work
  sender:send("") -- Send results to sink
end
