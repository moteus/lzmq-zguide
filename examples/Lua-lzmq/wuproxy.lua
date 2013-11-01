require "zhelpers"
local zmq = require "lzmq"
local zpoller = require "lzmq.poller"

local context = zmq.context()

local frontend, err = context:socket(zmq.XSUB,{connect = "tcp://192.168.55.210:5556"})
zassert(frontend, err)

local backend, err = context:socket(zmq.XPUB,{bind = "tcp://10.1.1.0:8100"})
zassert(backend, err)

zmq.proxy(frontend, backend)
