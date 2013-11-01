require "zhelpers"
local zmq = require "lzmq"

local context = zmq.context()
local receiver, err = context:socket(zmq.PULL, { connect = "tcp://localhost:5557" })
zassert(receiver, err)

local sender, err = context:socket(zmq.PUSH, { connect = "tcp://localhost:5558" })
zassert(sender, err)

while true do
  local message = receiver:recv()
  printf("%s.", message) -- Show progress
  io.flush()
  s_sleep(tonumber(message)) -- Do the work
  sender:send("") -- Send results to sink
end
