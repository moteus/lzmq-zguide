-- Paranoid Pirate worker

require "zhelpers"
local zmq      = require "lzmq"
local ztimer   = require "lzmq.timer"
local zloop    = require "lzmq.loop"

local HEARTBEAT_LIVENESS = 3     -- 3-5 is reasonable
local HEARTBEAT_INTERVAL = 1000  -- msecs
local INTERVAL_INIT      = 1000  -- Initial reconnect
local INTERVAL_MAX       = 32000 -- After exponential backoff

-- Paranoid Pirate Protocol constants
local PPP_READY     = "\001" -- Signals worker is ready
local PPP_HEARTBEAT = "\002" -- Signals worker heartbeat

-- Helper function that returns a new configured socket
-- connected to the Paranoid Pirate queue
function s_worker_socket(ctx)
  local worker, err = ctx:socket{zmq.DEALER,
    linger  = 0;
    connect = "tcp://localhost:5556";
  }
  zassert(worker, err)
  printf("I: worker ready\n")
  worker:send(PPP_READY)
  return worker;
end

-- We have a single task that implements the worker side of the
-- Paranoid Pirate Protocol (PPP). The interesting parts here are
-- the heartbeating, which lets the worker detect if the queue has
-- died, and vice versa:

local ctx = zmq.context()
local worker = s_worker_socket(ctx)
local interval = INTERVAL_INIT

local loop = zloop.new(1, ctx)

local reconnect_event
local cycles   = 0

-- Get message
-- - 3-part envelope + content -> request
-- - 1-part HEARTBEAT -> heartbeat
function worker_cb(worker)
  local msg = worker:recv_all()
  if not msg then return loop:interrupt() end

  -- To test the robustness of the queue implementation we
  -- simulate various typical problems, such as the worker
  -- crashing or running very slowly. We do this after a few
  -- cycles so that the architecture can get up and running
  -- first:
  if #msg == 3 then
    cycles = cycles + 1
    if (cycles > 3) and (randof(5) == 0) then
      printf("I: simulating a crash\n")
      return loop:interrupt()
    elseif (cycles > 3) and (randof(5) == 0) then
      printf("I: simulating CPU overload\n")
      sleep(3) -- Do some heavy work
    end
    printf ("I: normal reply\n")
    worker:send_all(msg)
    reconnect_event:restart()
  elseif (#msg == 1) and (msg[1] == PPP_HEARTBEAT) then
    -- When we get a heartbeat message from the queue, it means the
    -- queue was (recently) alive, so we must reset our liveness
    -- indicator:
    reconnect_event:restart()
  else
    printf("E: invalid message\n")
    s_dump_msg(msg)
  end
end

-- If the queue hasn't sent us heartbeats in a while, destroy the
-- socket and reconnect. This is the simplest most brutal way of
-- discarding any messages we might have sent in the meantime://
function reconnect_cb(ev)
  printf ("W: heartbeat failure, can't reach queue\n");
  printf ("W: reconnecting in %d msec...\n", interval);

  loop:remove_socket(worker)
  worker:close()
  worker = nil
  ev:reset()

  loop:add_once(interval, function()
    if interval < INTERVAL_MAX then
      interval = interval * 2
    end
    worker = s_worker_socket(ctx)
    loop:add_socket(worker, worker_cb)
    

    reconnect_event = loop:add_interval(
      HEARTBEAT_LIVENESS * HEARTBEAT_INTERVAL,
      reconnect_cb
    )
  end)
end

loop:add_socket(worker, worker_cb)

reconnect_event = loop:add_interval(
  HEARTBEAT_LIVENESS * HEARTBEAT_INTERVAL,
  reconnect_cb
)

loop:add_interval(HEARTBEAT_INTERVAL, function()
  -- Send heartbeat to queue if it's time
  if not worker then return end
  printf ("I: worker heartbeat\n");
  worker:send(PPP_HEARTBEAT)
end)

loop:start()