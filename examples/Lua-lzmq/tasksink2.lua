-- Task sink - design 2
-- Adds pub-sub flow to send kill signal to workers

require "zhelpers"
local zmq    = require "lzmq"
local ztimer = require "lzmq.timer"

local context = zmq.context()

-- Socket to receive messages on
local receiver, err = context:socket{zmq.PULL, bind = "tcp://*:5558"}
zassert(receiver, err)

-- Socket for worker control
local controller = context:socket{zmq.PUB, bind = "tcp://*:5559"}
zassert(receiver, err)

-- Wait for start of batch
receiver:recv()

-- Start our clock now
local timer = ztimer.monotonic():start()

-- Process 100 confirmations
for task_nbr = 1, 100 do
  local message = receiver:recv();
  if task_nbr % 10 == 0 then
    printf (":")
  else
    printf (".")
  end
  io.flush()
end

-- Calculate and report duration of batch
printf ("Total elapsed time: %d msec\n", timer:stop());

-- Send kill signal to workers
controller:send("KILL")
