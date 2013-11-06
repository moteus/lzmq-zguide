-- Simple Pirate broker
-- This is identical to load-balancing pattern, with no reliability
-- mechanisms. It depends on the client for recovery. Runs forever.

require "zhelpers"
local zmq      = require "lzmq"
local zpoller  = require "lzmq.poller"

local ctx = zmq.context()

-- For clients
local frontend, err = ctx:socket{zmq.ROUTER, bind = "tcp://*:5555"}
zassert(frontend, err)

-- For workers
local backend, err = ctx:socket{zmq.ROUTER, bind = "tcp://*:5556"}
zassert(backend, err)

-- Queue of available workers
local workers = {}

local poller = zpoller.new(2)

poller:add(backend, zmq.POLLIN, function()
  -- Use worker identity for load-balancing
  local msg = backend:recv_all()
  -- Interrupted
  if not msg then return poller.stop() end

  local identity = s_unwrap(msg)
  table.insert(workers, identity)

  -- Forward message to client if it's not a READY
  if msg[1] ~= WORKER_READY then
    frontend:send_all(msg)
  end

  if #workers == 1 then
    poller:add(frontend, zmq.POLLIN, frontend_cb)
  end
end)

function frontend_cb()
  -- Get client request, route to first available worker
  local msg = frontend:recv_all()
  if msg then
    assert(#workers > 0)
    local worker = table.remove(workers, 1)
    s_wrap(msg, worker)
    backend:send_all(msg)
    if #workers == 0 then
      poller:remove(frontend)
    end
  end
end

poller:start()