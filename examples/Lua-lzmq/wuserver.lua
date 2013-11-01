require "zhelpers"
local zmq = require "lzmq"

local context = zmq.context()
local publisher, err = context:socket(zmq.PUB,{ bind = "tcp://*:5556" })
zassert(publisher, err)
publisher:bind("ipc://weather.ipc")

while true do
  -- Get values that will fool the boss
  local zipcode     = randof(100000)
  local temperature = randof (215) - 80
  local relhumidity = randof (50) + 10

  -- Send message to all subscribers
  local update = sprintf("%05d %d %d", zipcode, temperature, relhumidity)
  publisher:send(update)
end
