-- Broker peering simulation (part 1)
-- Prototypes the state flow

require "zhelpers"
require "peercfg"
local zmq      = require "lzmq"
local zthreads = require "lzmq.threads"
local ztimer   = require "lzmq.timer"
local zloop    = require "lzmq.loop"

-- First argument is this broker's name
-- Other arguments are our peers' names
local argc = select("#", ...)

if argc < 2 then
  printf("syntax: peering1 me {you}...\n")
  return 0
end

local ctx = zmq.context()

local self = STATE(arg[1])

-- Bind state backend to endpoint
local statebe, err = ctx:socket{zmq.PUB, bind = self}
zassert(statebe, err)

-- Connect statefe to all peers
local statefe, err = ctx:socket{zmq.SUB, subscribe = ""}
zassert(statefe, err)

for i = 2, argc do
  local peer = STATE(arg[i])
  printf ("I: connecting to state backend at '%s'('%s')\n", arg[i], peer)
  zassert(statefe:connect(peer))
end

math.randomseed(statebe:fd())

-- The main loop sends out status messages to peers, and collects
-- status messages back from peers. The zmq_poll timeout defines
-- our own heartbeat:

local loop = zloop.new(1, ctx)

-- Handle incoming status messages
loop:add_socket(statefe, function(sok)
  local peer_name, available = sok:recvx()
  printf ("%s - %s workers free\n", peer_name, available)
end)

-- Send random values for worker availability
loop:add_interval(1000, function()
  statebe:sendx(self, tostring(randof(10)));
end)

loop:start()
