-- Load-balancing broker
-- Clients and workers are shown here in-process

require "zhelpers"
local zmq      = require "lzmq"
local zthreads = require "lzmq.threads"
local ztimer   = require "lzmq.timer"
local zpoller  = require "lzmq.poller"

local NBR_CLIENTS = 10
local NBR_WORKERS = 10

local FRONTEND = "ipc://frontend.ipc"
local BACKEND  = "ipc://backend.ipc"

if IS_WINDOWS then
  FRONTEND = "tcp://127.0.0.1:5671"
  BACKEND  = "tcp://127.0.0.1:5672"
end

local tremove = table.remove
local tinsert = table.insert

local init_thread = [[
  require "zhelpers"
  local zmq = require "lzmq"
  local context = zmq.context()
  local FRONTEND = ]] .. ("%q"):format(FRONTEND) .. [[
  local BACKEND = ]]  .. ("%q"):format(BACKEND)  .. [[
]]

-- Basic request-reply client using REQ socket
--
local client_task = init_thread .. [[
  local client, err = context:socket{zmq.REQ, connect = FRONTEND}
  zassert(client, err)
  -- Send request, get reply
  client:send("HELLO")
  local reply = client:recv()
  printf ("Client: %s\n", reply)
]]

-- While this example runs in a single process, that is just to make
-- it easier to start and stop the example. Each thread has its own
-- context and conceptually acts as a separate process.
-- This is the worker task, using a REQ socket to do load-balancing.
local worker_task = init_thread .. [[
  local worker, err = context:socket{zmq.REQ,connect = BACKEND}
  zassert(worker, err)

  -- Tell broker we're ready for work
  worker:send("READY")

  while true do
    -- Read and save all frames until we get an empty frame
    -- In this example there is only 1, but there could be more
    local identity, empty, request = worker:recvx()
    assert(empty == "")

    printf ("Worker: %s\n", request)

    worker:sendx(identity, "", "OK")
  end
]]

-- This is the main task. It starts the clients and workers, and then
-- routes requests between the two layers. Workers signal READY when
-- they start; after that we treat them as ready when they reply with
-- a response back to a client. The load-balancing data structure is
-- just a queue of next available workers.


local context = zmq.context()

local frontend, err = context:socket{zmq.ROUTER, bind = FRONTEND}
zassert(frontend, err)
local backend,  err = context:socket{zmq.ROUTER, bind = BACKEND}
zassert(backend, err)

-- Launch pool of client threads
for thread_nbr = 1, NBR_WORKERS do
  zthreads.run(context, worker_task):start(true)
end

-- Launch pool of worker threads
for thread_nbr = 1, NBR_CLIENTS do
  zthreads.run(context, client_task):start(true)
end

local client_nbr = NBR_CLIENTS

-- Here is the main loop for the least-recently-used queue. It has two
-- sockets; a frontend for clients and a backend for workers. It polls
-- the backend in all cases, and polls the frontend only when there are
-- one or more workers ready. This is a neat way to use 0MQ's own queues
-- to hold messages we're not ready to process yet. When we get a client
-- reply, we pop the next available worker and send the request to it,
-- including the originating client identity. When a worker replies, we
-- requeue that worker and forward the reply to the original client
-- using the reply envelope.

local worker_queue = {}

local poller = zpoller.new(2)

function frontend_cb()
  assert (#worker_queue > 0)

  -- Now get next client request, route to last-used worker
  -- Client request is [identity][empty][request]
  local msg = frontend:recv_all()
  assert(msg[2] == "")

  local worker_id = tremove(worker_queue, 1)
  backend:sendx_more(worker_id, "")
  backend:send_all(msg)
end

-- Handle worker activity on backend
function backend_cb()
  assert (#worker_queue < NBR_WORKERS)

  -- Queue worker identity for load-balancing
  local worker_id = backend:recv()
  tinsert(worker_queue, worker_id)

  -- Second frame is empty
  local empty = backend:recv()
  assert(empty == "")

  -- Third frame is READY or else a client reply identity
  local client_id = backend:recv()

  -- If client reply, send rest back to frontend
  if client_id ~= "READY" then
    empty = backend:recv()
    assert(empty == "")

    local reply = backend:recv()
    frontend:send_all{client_id, "", reply}

    client_nbr = client_nbr - 1
    if client_nbr == 0 then
      poller:stop() -- Exit after N messages
    end
  end
end

poller:add(backend, zmq.POLLIN, function()
  local n = #worker_queue
  backend_cb()
  if (n == 0) and (#worker_queue > 0) then
    poller:add(frontend, zmq.POLLIN, function()
      frontend_cb()
      if #worker_queue == 0 then poller:remove(frontend) end
    end)
  end
end)

poller:start()
