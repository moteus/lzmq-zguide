require "zhelpers"
local zmq = require "lzmq"
local zthreads = require "lzmq.threads"

local worker_routine = [[
  require "zhelpers"
  local zmq = require "lzmq"
  local zthreads = require "lzmq.threads"
  local context = zthreads.get_parent_ctx()

  local receiver, err = context:socket(zmq.REP,{connect = "inproc://workers"})
  zassert(receiver, err)
  while true do
    local message = receiver:recv()
    printf ("Received request: [%s]\n", message)

    -- Do some 'work'
    sleep (1);

    -- Send reply back to client
    receiver:send("World")
  end
]]

local context = zmq.context()

-- Socket to talk to clients
local clients, err = context:socket(zmq.ROUTER, {bind = "tcp://*:5555"})
zassert(clients, err)

-- Socket to talk to clients
local workers, err = context:socket(zmq.DEALER, {bind = "inproc://workers"})
zassert(workers, err)

-- Launch pool of worker threads
for thread_nbr = 1, 5 do
  zthreads.run(context, worker_routine):start(true)
end

-- Connect work threads to client threads via a queue proxy
zmq.proxy(clients, workers);
