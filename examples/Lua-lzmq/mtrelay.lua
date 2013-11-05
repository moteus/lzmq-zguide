-- Multithreaded relay

require "zhelpers"
local zmq      = require "lzmq"
local zthreads = require "lzmq.threads"

local init_thread = [[
  require "zhelpers"
  local zmq      = require "lzmq"
  local zthreads = require "lzmq.threads"
  local context = zthreads.get_parent_ctx()
]]

local step1_routine = init_thread .. [[
  -- Connect to step2 and tell it we're ready
  local xmitter, err = context:socket{zmq.PAIR, connect = "inproc://step2"}
  zassert(xmitter, err)
  printf ("Step 1 ready, signaling step 2\n")
  xmitter:send("READY")
]]

local step2_routine = init_thread .. [[
  -- Bind inproc socket before starting step1
  local receiver, err = context:socket{zmq.PAIR, bind = "inproc://step2"}
  zassert(receiver, err)
  zthreads.run(context, ]] .. string.format("%q", step1_routine) .. [[):start(true)
  receiver:recv()
  
  -- Connect to step3 and tell it we're ready
  local xmitter, err = context:socket{zmq.PAIR, connect = "inproc://step3"}
  zassert(xmitter, err)
  printf ("Step 2 ready, signaling step 3\n")
  xmitter:send("READY")
]]

local context = zmq.context()

-- Bind inproc socket before starting step2
local receiver, err = context:socket{zmq.PAIR, bind = "inproc://step3"}
zassert(receiver, err)

zthreads.run(context, step2_routine):start(true)

-- Wait for signal
receiver:recv()

printf ("Test successful!\n")