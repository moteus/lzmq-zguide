require "zhelpers"
local zmq      = require "lzmq"
local zthreads = require "lzmq.threads"
local ztimer   = require "lzmq.timer"

local NBR_WORKERS = 10

-- While this example runs in a single process, that is only to make
-- it easier to start and stop the example. Each thread has its own
-- context and conceptually acts as a separate process.

local worker_routine = [[
  require "zhelpers"
  local zmq = require "lzmq"

  local context = zmq.context()

  local worker, err = context:socket(zmq.REQ,{connect = "tcp://localhost:5671"})
  zassert(worker, err)

  math.randomseed(worker:fd())

  local total = 0
  while true do
    -- Tell the broker we're ready for work
    worker:send("Hi Boss")

    -- Get workload from broker, until finished
    local workload = worker:recv()
    if workload == "Fired!" then
      printf ("Completed: %d tasks\n", total)
      break
    end
    total = total + 1
    
    -- Do some random work
    s_sleep (randof (500) + 1)
  end
]]

local context = zmq.context()

local broker, err = context:socket(zmq.ROUTER, {bind = "tcp://*:5671"})
zassert(broker, err)

sleep(1)

-- Launch pool of worker threads
for thread_nbr = 1, NBR_WORKERS do
  zthreads.run(context, worker_routine):start(true)
end

-- Run for five seconds and then tell workers to end
local timer = ztimer.monotonic(5000):start()

local workers_fired = 0;
while true do
  -- Next message gives us least recently used worker
  local identity = broker:recv()
  broker:send_more(identity)
  broker:recv() -- Envelope delimiter
  broker:recv() -- Response from worker
  broker:send_more("")
  -- Encourage workers until it's time to fire them
  if timer:rest() > 0 then
    broker:send("Work harder")
  else
    broker:send("Fired!")
    workers_fired = workers_fired + 1
    if workers_fired == NBR_WORKERS then break end
  end
end