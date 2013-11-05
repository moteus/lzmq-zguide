require "zhelpers"
local zmq      = require "lzmq"
local zthreads = require "lzmq.threads"

local init_thread = [[
  require "zhelpers"
  local zthreads = require "lzmq.threads"
  local context = zthreads.get_parent_ctx()
]]

local step1_routine = init_thread .. [[
  -- pipe in first arg
  local xmitter = ...
  printf ("Step 1 ready, signaling step 2\n")
  xmitter:send("READY")
]]

local step2_routine = init_thread .. [[
  local thread, receiver = zthreads.fork(context, ]] 
    .. string.format("%q", step1_routine) .. 
  [[)
  thread:start()
  receiver:recv()
  
  -- pipe in first arg
  local xmitter = ...
  printf ("Step 2 ready, signaling step 3\n")
  xmitter:send("READY")
]]

local thread, receiver = zthreads.fork(zmq.context(), step2_routine)
thread:start()

-- Wait for signal
receiver:recv()

printf ("Test successful!\n")