require "zhelpers"
local zmq = require "lzmq"

local SUBSCRIBERS_EXPECTED = 10 -- We wait for 10 subscribers

local context = zmq.context()

-- Socket to talk to clients
local publisher, err = context:socket(zmq.PUB, {
  sndhwm = 1100000;
  bind   = "tcp://*:5561";
})
zassert(publisher, err)

-- Socket to receive signals
local syncservice, err = context:socket(zmq.REP, { bind = "tcp://*:5562" })
zassert(syncservice, err)

-- Get synchronization from subscribers
printf ("Waiting for subscribers\n");
local subscribers = 0;
while subscribers < SUBSCRIBERS_EXPECTED do
  syncservice:recv()   -- wait for synchronization request
  syncservice:send("") -- send synchronization reply
  subscribers = subscribers + 1
end

-- Now broadcast exactly 1M updates followed by END
printf ("Broadcasting messages\n");
for update_nbr = 1, 1000000 do
  publisher:send("Rhubarb")
end
publisher:send("END")
