-- Paranoid Pirate queue

require "zhelpers"
local zmq      = require "lzmq"
local ztimer   = require "lzmq.timer"
local zloop    = require "lzmq.loop"

local HEARTBEAT_LIVENESS = 3    -- 3-5 is reasonable
local HEARTBEAT_INTERVAL = 1000 -- msecs

-- Paranoid Pirate Protocol constants
local PPP_READY     = "\001" -- Signals worker is ready
local PPP_HEARTBEAT = "\002" -- Signals worker heartbeat

local ctx = zmq.context()

-- For clients
local frontend, err = ctx:socket{zmq.ROUTER, bind = "tcp://*:5555"}
zassert(frontend, err)

-- For workers
local backend, err = ctx:socket{zmq.ROUTER, bind = "tcp://*:5556"}
zassert(backend, err)

-- Queue of available workers
local workers = {}

-- The main task is a load-balancer with heartbeating on workers so we
-- can detect crashed or blocked worker tasks:

local loop = zloop.new(2, ctx)

-- Handle worker activity on backend
loop:add_socket(backend, function()
  -- Use worker identity for load-balancing
  local msg = backend:recv_all()
  -- Interrupted
  if not msg then return poller.stop() end

  -- Any sign of life from worker means it's ready
  local identity = s_unwrap(msg)
  local worker
  for i, w in ipairs(workers) do
    if w.identity == identity then
      table.remove(workers, i)
      w.expiry:start()
      worker = w
      break
    end
  end

  if (not worker) and (#workers == 0) then
    loop:add_socket(frontend, frontend_cb)
  end

  worker = worker or {
    identity = identity;
    expiry   = ztimer.monotonic(HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS):start()
  }
  table.insert(workers, worker)

  -- Validate control message, or return reply to client
  if #msg == 1 then
    if (msg[1] ~= PPP_READY) and (msg[1] ~= PPP_HEARTBEAT) then
      printf("E: invalid message from worker")
      s_dump_msg(msg)
    end
  else
    frontend:send_all(msg)
  end

end)

function frontend_cb(sok)
  -- Now get next client request, route to next worker
  local msg = frontend:recv_all()
  if msg then
    assert(#workers > 0)
    local worker = table.remove(workers, 1)
    backend:send_more(worker.identity)
    backend:send_all(msg)
    if #workers == 0 then
      loop:remove_socket(frontend)
    end
  end
end

-- Handle heartbeating. First, we send heartbeats 
-- to any idle workers if it's time. Then, we purge any
-- dead workers
loop:add_interval(HEARTBEAT_INTERVAL, function()
  for _, worker in ipairs(workers) do
    backend:sendx(worker.identity, PPP_HEARTBEAT)
  end

  -- The purge method looks for and kills expired workers. We hold workers
  -- from oldest to most recent, so we stop at the first alive worker:
  local worker = workers[1]
  while worker do
    if worker.expiry:rest() > 0 then break end
    table.remove(workers, 1)
    worker = workers[1]
  end

  if #workers == 0 then
    loop:remove_socket(frontend)
  end
end)

loop:add_socket(frontend, frontend_cb)

loop:start()
