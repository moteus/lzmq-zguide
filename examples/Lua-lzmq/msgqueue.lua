require "zhelpers"
local zmq = require "lzmq"
local zpoller = require "lzmq.poller"

local context = zmq.context()

local frontend, err = context:socket(zmq.ROUTER,{bind = "tcp://*:5559"})
zassert(frontend, err)

local backend, err = context:socket(zmq.DEALER,{bind = "tcp://*:5560"})
zassert(backend, err)

zmq.proxy(frontend, backend)
