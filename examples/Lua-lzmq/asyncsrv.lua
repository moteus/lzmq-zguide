require "zhelpers"
local zmq      = require "lzmq"
local zthreads = require "lzmq.threads"
local ztimer   = require "lzmq.timer"
local zpoller  = require "lzmq.poller"

local include = [[
  require "zhelpers"
  local zmq      = require "lzmq"
  local zthreads = require "lzmq.threads"
  local ztimer   = require "lzmq.timer"
  local zpoller  = require "lzmq.poller"
]]

local init_task = include .. [[
  local ctx = zmq.context()
]]

local init_thread = include .. [[
  local ctx = zthreads.get_parent_ctx()
]]

-- This is our client task
-- It connects to the server, and then sends a request once per second
-- It collects responses as they arrive, and it prints them out. We will
-- run several client tasks in parallel, each with a different random ID.
local client_task = init_task .. [[
  local client, err = ctx:socket(zmq.DEALER, {connect = "tcp://localhost:5570"})
  zassert(client, err)
  local timer = ztimer.monotonic(1000)
  local poller = zpoller.new(1)

  poller:add(client, zmq.POLLIN, function()
    local msg = client:recv_all()
    printf("%04X: %s\n", client:fd(), msg[#msg])
  end)

  local request_nbr = 0
  while true do
    timer:start()
    while timer:rest() > 0 do
      poller:poll(10)
    end
    request_nbr = request_nbr + 1
    local msg = string.format("request #%d", request_nbr)
    client:send(msg)
  end
]]

-- Each worker task works on one request at a time and sends a random number
-- of replies back, with random delays between replies:
local server_worker = init_thread .. [[
  local worker, err = ctx:socket(zmq.DEALER, {connect = "inproc://backend"})
  zassert(worker, err)

  math.randomseed(worker:fd())

  while true do
    -- The DEALER socket gives us the reply envelope and message
    local msg = worker:recv_all()
    local identity, content = msg[1], msg[2]
    assert(identity and content)

    -- Send 0..4 replies back
    local replies = randof(5)
    for reply = 1, replies do
      -- Sleep for some fraction of a second
      ztimer.sleep(randof(1000) + 1)
      worker:send_all{identity, content}
    end
  end
]]

-- This is our server task.
-- It uses the multithreaded server model to deal requests out to a pool
-- of workers and route replies back to clients. One worker can handle
-- one request at a time but one client can talk to multiple workers at
-- once.
local server_task = init_task .. [[
  -- Frontend socket talks to clients over TCP
  local frontend, err = ctx:socket(zmq.ROUTER,{bind = "tcp://*:5570"})
  zassert(frontend, err)

  -- Backend socket talks to workers over inproc
  local backend, err = ctx:socket(zmq.DEALER,{bind = "inproc://backend"})
  zassert(backend, err)

  -- Launch pool of worker threads, precise number is not critical
  for thread_nbr = 1, 5 do
    zthreads.run(ctx, ]] .. string.format("%q", server_worker) .. [[):start(true)
  end

  zmq.proxy(frontend, backend)
]]


-- The main thread simply starts several clients and a server, and then
-- waits for the server to finish.

zthreads.run(nil, client_task):start(true)
zthreads.run(nil, client_task):start(true)
zthreads.run(nil, client_task):start(true)
zthreads.run(nil, server_task):start(true)
ztimer.sleep (5 * 1000) -- Run for 5 seconds then quit


