-- Lazy Pirate client
-- Use zmq_poll to do a safe request-reply
-- To run, start lpserver and then randomly kill/restart it

require "zhelpers"
local zmq = require "lzmq"

local REQUEST_TIMEOUT = 2500 -- msecs, (> 1000!)
local REQUEST_RETRIES = 3    -- Before we abandon
local SERVER_ENDPOINT = "tcp://localhost:5555"

local ctx = zmq.context()

local SOCKET_OPTION = {zmq.REQ,
  linger   = 0;
  connect  = SERVER_ENDPOINT;
  rcvtimeo = REQUEST_TIMEOUT;
}

printf("I: connecting to server...\n")
local client, err = ctx:socket(SOCKET_OPTION)
zassert(client, err)

local sequence = 0;
local retries_left = REQUEST_RETRIES
while retries_left > 0 do
  -- We send a request, then we work to get a reply
  sequence = sequence + 1
  local request = tostring(sequence)
  client:send(request)
  
  local expect_reply = true
  while expect_reply do
    local reply, err = client:recv()

    -- Here we process a server reply and exit our loop if the
    -- reply is valid. If we didn't a reply we close the client
    -- socket and resend the request. We try a number of times
    -- before finally abandoning:

    if reply then
      -- We got a reply from the server, must match sequence
      if tonumber(reply) == sequence then
        printf ("I: server replied OK (%s)\n", reply)
        retries_left = REQUEST_RETRIES
        expect_reply = false
      else
        printf ("E: malformed reply from server: %s\n", reply)
      end
    elseif err:no() == zmq.EINTR then
      retries_left = 0
      break
    else
      assert(err:no() == zmq.EAGAIN)
      retries_left = retries_left - 1
      if retries_left == 0 then
        printf("E: server seems to be offline, abandoning\n")
        break
      else
        printf("W: no response from server, retrying...\n")
        -- Old socket is confused; close it and open a new one
        client:close()
        printf ("I: reconnecting to server...\n")
        client, err = ctx:socket(SOCKET_OPTION)
        zassert(client, err)
        -- Send request again, on new socket
        client:send(request)
      end
    end
  end
end
