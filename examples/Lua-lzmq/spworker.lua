-- Simple Pirate worker
-- Connects REQ socket to tcp://*:5556
-- Implements worker part of load-balancing

require "zhelpers"
local zmq      = require "lzmq"
local zpoller  = require "lzmq.poller"

local WORKER_READY = "\001" -- Signals worker is ready

math.randomseed(os.time())

local ctx = zmq.context()

local identity = sprintf("%04X-%04X", randof(0x10000), randof(0x10000));
local worker, err = ctx:socket{zmq.REQ,
  identity = identity;
  connect  = "tcp://localhost:5556";
}

-- Tell broker we're ready for work
printf("I: (%s) worker ready\n", identity)
worker:send(WORKER_READY)

local cycles = 0
while true do
  local msg = worker:recv_all()
  if not msg then break end -- Interrupted

  -- Simulate various problems, after a few cycles
  cycles = cycles + 1
  if (cycles > 3) and (randof(5) == 0) then
    printf("I: (%s) simulating a crash\n", identity)
    break;
  elseif (cycles > 3) and (randof(5) == 0) then
    printf("I: (%s) simulating CPU overload\n", identity)
    sleep(3)
  end
  printf("I: (%s) normal reply\n", identity)
  sleep(1) -- Do some heavy work
  worker:send_all(msg)
end
