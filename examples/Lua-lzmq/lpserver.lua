-- Lazy Pirate server
-- Binds REQ socket to tcp://*:5555
-- Like hwserver except:
-- - echoes request as-is
-- - randomly runs slowly, or exits to simulate a crash.

require "zhelpers"
local zmq = require "lzmq"

local ctx = zmq.context()

local server, err = ctx:socket{zmq.REP, bind = "tcp://*:5555"}
zassert(server, err)

math.randomseed(os.time())

local cycles = 0
while true do
  local request = server:recv()
  cycles = cycles + 1

  -- Simulate various problems, after a few cycles
  if (cycles > 3) and (randof(3) == 0) then
    printf("I: simulating a crash\n")
    break
  elseif (cycles > 3) and (randof(3) == 0) then
    printf("I: simulating CPU overload\n")
    sleep(2)
  end
  printf("I: normal request (%s)\n", request)
  sleep(1) -- Do some heavy work
  server:send(request)
end
