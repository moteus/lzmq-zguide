-- Task ventilator
-- Binds PUSH socket to tcp://localhost:5557
-- Sends batch of tasks to workers via that socket

require "zhelpers"
local zmq = require "lzmq"

local context = zmq.context()

-- Socket to send messages on
local sender, err = context:socket{zmq.PUSH, bind = "tcp://*:5557"}
zassert(sender, err)

-- Socket to send start of batch message on
local sink, err = context:socket{zmq.PUSH, connect = "tcp://localhost:5558"}
zassert(sink, err)

printf("Press Enter when the workers are ready: ")
getchar()
printf("Sending tasks to workers...\n")

-- The first message is "0" and signals start of batch
sink:send("0")

-- Send 100 tasks
local total_msec = 0; -- Total expected cost in msecs
for task_nbr = 1, 100 do
  --  Random workload from 1 to 100msecs
  local workload = randof (100) + 1;
  local total_msec = total_msec + workload;
  sender:send(workload)
end

printf ("\nTotal expected cost: %d msec\n", total_msec)
