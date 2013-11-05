-- Weather update client
-- Connects SUB socket to tcp://localhost:5556
-- Collects weather updates and finds avg temp in zipcode

require "zhelpers"
local zmq = require "lzmq"

-- Subscribe to zipcode, default is NYC, 10001
local filter = arg[1] or "10001"

printf("Collecting updates from weather server ...\n")

local context = zmq.context()

-- Socket to talk to server
local subscriber, err = context:socket{zmq.SUB,
  subscribe = filter .. " ";
  connect   = "tcp://localhost:5556";
}
zassert(subscriber, err)

-- Process 100 updates
local update_nbr, total_temp = 100, 0
for i = 1, update_nbr do
  local message = subscriber:recv()
  local zipcode, temperature, relhumidity =
    string.match(message, "([%d]*)%s+([-]?[%d-]*)%s+([-]?[%d-]*)")
  assert(zipcode == filter)
  total_temp = total_temp + tonumber(temperature)
end

printf ("Average temperature for zipcode '%s' was %dF\n",
  filter, (total_temp / update_nbr))

