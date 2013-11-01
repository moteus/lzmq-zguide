require "zhelpers"
local zmq = require "lzmq"

local SUBSCRIBERS_EXPECTED = 10 -- We wait for 10 subscribers

local context = zmq.context()

-- Socket to talk to clients
local subscriber, err = context:socket(zmq.SUB, {
  subscribe = "";
  connect   = "tcp://localhost:5561";
})
zassert(subscriber, err)

-- 0MQ is so fast, we need to wait a while
sleep (1);

-- Second, synchronize with publisher
local syncclient = context:socket(zmq.REQ, {connect = "tcp://localhost:5562"})
zassert(syncclient, err)

syncclient:send("") -- send a synchronization request
syncclient:recv()   -- wait for synchronization reply

local update_nbr = 0
while true do
  local message = subscriber:recv()
  if message == "END" then break end
  update_nbr = update_nbr + 1
end

printf ("Received %d updates\n", update_nbr)
