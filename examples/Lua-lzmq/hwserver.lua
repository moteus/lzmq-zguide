-- Hello World server

require "zhelpers"
local zmq = require "lzmq"

local context = zmq.context()
local responder, err = context:socket{zmq.REP, bind = "tcp://*:5556"}
responder:bind("ipc://weather.ipc")
zassert(responder, err)
while true do
  local buffer = zassert(responder:recv())
  print("Received " .. buffer)
  sleep (1) -- Do some 'work'
  zassert(responder:send("World"))
end
