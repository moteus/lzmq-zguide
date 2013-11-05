-- Pubsub envelope publisher

require "zhelpers"
local zmq = require "lzmq"

-- Prepare our context and publisher
local context = zmq.context()
local publisher, err = context:socket{zmq.PUB, bind = "tcp://*:5563"}
zassert(publisher, err)

while true do
  -- Write two messages, each with an envelope and content
  publisher:sendx("A", "We don't want to see this")
  publisher:sendx("B", "We would like to see this")
  sleep (1);
end

