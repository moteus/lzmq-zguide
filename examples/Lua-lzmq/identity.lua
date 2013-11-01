require "zhelpers"
local zmq = require "lzmq"

-- Prepare our context and publisher
local context = zmq.context()

local sink, err = context:socket(zmq.ROUTER,{ bind = "inproc://example" })
zassert(sink, err)

-- First allow 0MQ to set the identity
local anonymous, err = context:socket(zmq.REQ, {connect = "inproc://example"})
zassert(anonymous, err)
anonymous:send("ROUTER uses a generated UUID")

s_dump(sink)

-- Then set the identity ourselves
local identified, err = context:socket(zmq.REQ, {
  identity = "PEER2";
  connect  = "inproc://example";
})
zassert(identified, err)
identified:send ("ROUTER socket uses REQ's socket identity");

s_dump (sink);
