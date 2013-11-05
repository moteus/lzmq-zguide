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


-- This is the client task. It issues a burst of requests and then
-- sleeps for a few seconds. This simulates sporadic activity; when
-- a number of clients are active at once, the local workers should
-- be overloaded. The client uses a REQ socket for requests and also
-- pushes statistics to the monitor socket:
local client_task = init_task .. [[
  local self = ...
  local client, err = ctx:socket{zmq.REQ, 
    rcvtimeo = 10000; -- Wait max ten seconds for a reply, then complain
    connect  = LOCALFE(self);
  }
  zassert(client, err)
  local monitor, err = ctx:socket{zmq.PUSH, connect = MONITOR(self)}
  zassert(monitor, err)

  sleep (1) -- wait all brokers bind
  while true do
    sleep(randof(5))
    local burst = randof(15)

    while burst > 0 do
      burst = burst - 1
      local task_id = sprintf("%04X", randof (0x10000))

      client:send(task_id) -- Send request with random hex ID
      local reply, err = client:recv()
      if reply then
        assert(reply == task_id)
        monitor:send(reply)
      elseif err:mnemo() == 'EINTR' then
        return
      else
        monitor:send ("E: CLIENT EXIT - lost task " ..task_id)
        return
      end
    end
  end
]]

-- This is the worker task, which uses a REQ socket to plug into the
-- load-balancer. It's the same stub worker task that you've seen in
-- other examples:
local worker_task = init_task .. [[
  local self = ...
  local worker, err = ctx:socket{zmq.REQ, connect=LOCALBE(self)}

  -- Tell broker we're ready for work
  worker:send(WORKER_READY)

  -- Process messages as they arrive
  while true do
    local msg = worker:recv_all()
    if not msg then break end -- Interrupted
    -- Workers are busy for 0/1 seconds
    sleep(randof(2))
    worker:send_all(msg)
  end
]]




-- The main task begins by setting up all its sockets. The local frontend
-- talks to clients, and our local backend talks to workers. The cloud
-- frontend talks to peer brokers as if they were clients, and the cloud
-- backend talks to peer brokers as if they were workers. The state
-- backend publishes regular state messages, and the state frontend
-- subscribes to all state backends to collect these messages. Finally,
-- we use a PULL monitor socket to collect printable messages from tasks:



-- First argument is this broker's name
-- Other arguments are our peers' names
local argc = select("#", ...)

-- local arg = {"DC1", "DC2"}
-- local argc = #arg

if argc < 2 then
  printf("syntax: peering3 me {you}...\n")
  return 0
end

local ctx = zmq.context()

local self = arg[1]

printf ("I: preparing broker at %s ...\n", self);


-- Prepare local frontend and backend
local localfe, err = ctx:socket{zmq.ROUTER, bind = LOCALFE(self)}
zassert(localfe, err)

local localbe, err = ctx:socket{zmq.ROUTER, bind = LOCALBE(self)}
zassert(localbe, err)

-- Bind cloud frontend to endpoint
local cloudfe, err = ctx:socket{zmq.ROUTER, 
  identity = self;
  bind = CLOUD(self);
}

-- list of all cloud backend
local beendpoints = {}
for i = 2, argc do
  local peer = CLOUD(arg[i])
  printf ("I: connecting to cloud frontend at '%s'('%s')\n", arg[i], peer)
  table.insert(beendpoints, peer)
end

-- Connect cloud backend to all peers
local cloudbe, err = ctx:socket{zmq.ROUTER,
  identity = self;
  connect = beendpoints;
}
zassert(cloudbe, err)

-- Bind state backend to endpoint
local statebe, err = ctx:socket{zmq.PUB, bind = STATE(self)}
zassert(statebe, err)

-- list of all frontend endpoints
local feendpoints = {}
for i = 2, argc do
  local peer = STATE(arg[i])
  printf ("I: connecting to state backend at '%s'('%s')\n", arg[i], peer)
  table.insert(feendpoints, peer)
end

-- Connect statefe to all peers
local statefe, err = ctx:socket{zmq.SUB,
  subscribe = "";
  connect   = feendpoints;
}
zassert(statefe, err)

-- Prepare monitor socket
local monitor, err = ctx:socket{zmq.PULL, bind = MONITOR(self)}
zassert(monitor, err)

math.randomseed(cloudfe:fd())

-- After binding and connecting all our sockets, we start our child
-- tasks - workers and clients:

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

local workers = {}
local cloud_capacity = 0
local previous = 0

-- The main loop has two parts. First, we poll workers and our two service
-- sockets (statefe and monitor), in any case. If we have no ready workers,
-- then there's no point in looking at incoming requests. These can remain
-- on their internal 0MQ queues:


-- loop to proceed client requests
local loopfe = zloop.new(2, ctx)

local localfe_ready, cloudfe_ready

loopfe:add_socket(localfe, function() localfe_ready = true end)

loopfe:add_socket(cloudfe, function() cloudfe_ready = true end)

-- proceed clients requests
function proceed_frontend(msg, reroutable)
  if #workers == 0 then
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
  -- Now route as many clients requests as we can handle. If we have
  -- local capacity, we poll both localfe and cloudfe. If we have cloud
  -- capacity only, we poll just localfe. We route any request locally
  -- if we can, else we route to the cloud.

  localfe_ready, cloudfe_ready = false
  while((cloud_capacity+#workers)>0)and(loopfe:poll(0)>0)do
    assert(cloudfe_ready or localfe_ready)

    if #workers == 0 then -- ignore cloud
      cloudfe_ready = false
    end

    if localfe_ready then
      proceed_frontend(localfe:recv_all(), true)
    elseif cloudfe_ready then
      proceed_frontend(cloudfe:recv_all(), false)
    else
      break
    end

    localfe_ready, cloudfe_ready = false
  end

  if previous ~= #workers then
    statebe:sendx(self, tostring(#workers))
    previous = #workers
  end
end

-- Here, we handle the request-reply flow. We're using load-balancing
-- to poll workers at all times, and clients only when there are one
-- or more workers available.

-- Least recently used queue of available workers
local loopbe = zloop.new(2, ctx)

function proceed_backend(msg)
  if msg[1] ~= WORKER_READY then
    -- Route reply to cloud if it's addressed to a broker
    local data = msg[1]
    for argn = 2, argc do
      if data == arg[ argn ] then
        return cloudfe:send_all( msg )
      end
    end
    -- Route reply to client if we still need to
    localfe:send_all(msg)
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
  previous = #workers

  local msg = sok:recv_all()
  if not msg then return loopbe:interrupt() end

  -- We don't use peer broker identity for anything
  local identity = s_unwrap(msg)

  proceed_backend(msg)

  if previous ~= #workers then
    statebe:sendx(self, tostring(#workers))
  end
end)

loopbe:add_socket(statefe, function(sok)
  local peer, status = sok:recvx()
  cloud_capacity = tonumber(status)
end)

loopbe:add_socket(monitor, function(sok)
  local msg = sok:recvx()
  print(msg)
end)

loopbe:add_interval(1000, poll_frontend)

loopbe:start()
