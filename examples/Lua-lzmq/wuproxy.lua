-- Weather proxy device

require "zhelpers"
local zmq     = require "lzmq"
local zpoller = require "lzmq.poller"

local context = zmq.context()

-- This is where the weather server sits
local frontend, err = context:socket{zmq.XSUB, connect = "tcp://192.168.55.210:5556"}
zassert(frontend, err)

-- This is our public endpoint for subscribers
local backend, err = context:socket{zmq.XPUB, bind = "tcp://10.1.1.0:8100"}
zassert(backend, err)

-- Run the proxy until the user interrupts us
zmq.proxy(frontend, backend)
