-- Broker peering simulation (part 2)
-- Prototypes the request-reply flow

require "zhelpers"
require "peercfg"
local zmq      = require "lzmq"
local zthreads = require "lzmq.threads"
local ztimer   = require "lzmq.timer"
local zloop    = require "lzmq.loop"

local NBR_CLIENTS  = 10
local NBR_WORKERS  = 3
local WORKER_READY = "\001" -- Signals worker is ready

local include = [[
  require "zhelpers"
  local zmq      = require "lzmq"
  local zthreads = require "lzmq.threads"
  local ztimer   = require "lzmq.timer"
  local zpoller  = require "lzmq.poller"
  local config   = require "peercfg"
  local NBR_CLIENTS  = ]] .. ("%q"):format(NBR_CLIENTS)  .. [[
  local NBR_WORKERS  = ]] .. ("%q"):format(NBR_WORKERS)  .. [[
  local WORKER_READY = ]] .. ("%q"):format(WORKER_READY) .. [[
]]

local init_task = include .. [[
  local ctx = zmq.context()
]]

local init_thread = include .. [[
  local ctx = zthreads.get_parent_ctx()
]]

-- The client task does a request-reply dialog using a standard
-- synchronous REQ socket:
local client_task = init_task .. [[
  local self = ...
  local client, err = ctx:socket{zmq.REQ, connect=LOCALFE(self)}
  zassert(client, err)

  sleep (1) -- wait all brokers bind
  while true do
    -- Send request, get reply
    client:send("HELLO")
    local reply = client:recv()
    if not reply then break end -- Interrupted
    printf ("Client: %s\n", reply)
    sleep (1)
  end
]]

-- The worker task plugs into the load-balancer using a REQ
-- socket:
local worker_task = init_task .. [[
  local self = ...
  local worker, err = ctx:socket{zmq.REQ, connect=LOCALBE(self)}

  -- Tell broker we're ready for work
  worker:send(WORKER_READY)

  -- Process messages as they arrive
  while true do
    local msg = worker:recv_all()
    if not msg then break end -- Interrupted
    msg[#msg] = "OK"
    print("Worker : OK")
    worker:send_all(msg)
  end
]]

-- The main task begins by setting-up its frontend and backend sockets
-- and then starting its client and worker tasks:


-- First argument is this broker's name
-- Other arguments are our peers' names
local argc = select("#", ...)

if argc < 2 then
  printf("syntax: peering2 me {you}...\n")
  return 0
end

local ctx = zmq.context()

local self = arg[1]

-- Bind cloud frontend to endpoint
local cloudfe, err = ctx:socket{zmq.ROUTER, 
  identity = self;
  bind = CLOUD(self);
}

printf("I: My name is '%s'('%s')\n", self, CLOUD(self))

-- Connect cloud backend to all peers
local cloudbe, err = ctx:socket{zmq.ROUTER, identity = self}
zassert(cloudbe, err)

for i = 2, argc do
  local peer = CLOUD(arg[i])
  printf ("I: connecting to cloud frontend at '%s'('%s')\n", arg[i], peer)
  zassert(cloudbe:connect(peer))
end

math.randomseed(cloudfe:fd())

-- Prepare local frontend and backend
local localfe, err = ctx:socket{zmq.ROUTER, bind = LOCALFE(self)}
zassert(localfe, err)
local localbe, err = ctx:socket{zmq.ROUTER, bind = LOCALBE(self)}
zassert(localbe, err)

-- Get user to tell us when we can start…
-- printf ("Press Enter when all brokers are started: ");
-- getchar ();

-- Start local workers
for worker_nbr = 1, NBR_WORKERS do
  local thread = zthreads.run(ctx, worker_task, self)
  thread:start(true)
end

-- Start local clients
for client_nbr = 1, NBR_CLIENTS do
  local thread = zthreads.run(ctx, client_task, self)
  thread:start(true)
end

-- Least recently used queue of available workers
local workers = {}

-- loop to proceed client requests
local loopfe = zloop.new(2, ctx)

local localfe_ready, cloudfe_ready

loopfe:add_socket(localfe, function() localfe_ready = true end)

loopfe:add_socket(cloudfe, function() cloudfe_ready = true end)

-- proceed clients requests
function proceed_frontend(msg, reroutable)
  -- If reroutable, send to cloud 20% of the time
  -- Here we'd normally use cloud status information

  if reroutable and (argc > 1) and (randof (5) == 0) then
    -- Route to random broker peer
    local peer = randof(argc - 1) + 2
    table.insert(msg, 1, arg[peer])
    cloudbe:send_all(msg)
  else
    local frame = table.remove(workers, 1)
    s_wrap(msg, frame)
    localbe:send_all(msg)
  end
end

-- poll clients requests
function poll_frontend()
  -- Now we route as many client requests as we have worker capacity
  -- for. We may reroute requests from our local frontend, but not from
  -- the cloud frontend. We reroute randomly now, just to test things
  -- out. In the next version, we'll do this properly by calculating
  -- cloud capacity:

  localfe_ready, cloudfe_ready = false
  while (#workers>0) and (loopfe:poll(0)>0) do
    assert(cloudfe_ready or localfe_ready)
    -- We'll do peer brokers first, to prevent starvation
    if cloudfe_ready then
      proceed_frontend(cloudfe:recv_all(), false)
    elseif(localfe_ready) then
      proceed_frontend(localfe:recv_all(), true)
    end
    localfe_ready, cloudfe_ready = false
  end
end

-- Here, we handle the request-reply flow. We're using load-balancing
-- to poll workers at all times, and clients only when there are one
-- or more workers available.

local loopbe = zloop.new(2, ctx)

function proceed_backend(msg)
  if msg[1] ~= WORKER_READY then
    -- Route reply to cloud if it's addressed to a broker
    local data = msg[1]
    for argn = 2, argc do
      if data == arg[ argn ] then
        cloudfe:send_all( msg )
        msg = nil
        break
      end
    end
    if msg then
      -- Route reply to client if we still need to
      localfe:send_all(msg)
    end
  end

  poll_frontend()
end

-- First, route any waiting replies from workers
-- Handle reply from local worker
loopbe:add_socket(localbe, function(sok)
  local msg = sok:recv_all()
  if not msg then return loopbe:interrupt() end

  local identity = s_unwrap(msg)
  table.insert(workers, identity)

  proceed_backend(msg)
end)

loopbe:add_socket(cloudbe, function(sok)
  local msg = sok:recv_all()
  if not msg then return loopbe:interrupt() end

  -- We don't use peer broker identity for anything
  local identity = s_unwrap(msg)

  proceed_backend(msg)
end)

loopbe:add_interval(1000, poll_frontend)

loopbe:start()
