-- Task sink
-- Binds PULL socket to tcp://localhost:5558
-- Collects results from workers via that socket

require "zhelpers"
local zmq    = require "lzmq"
local ztimer = require "lzmq.timer"

-- Prepare our context and socket
local context = zmq.context()
local receiver, err = context:socket{zmq.PULL, bind = "tcp://*:5558"}
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

