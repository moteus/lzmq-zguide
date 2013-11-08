-- mdcliapi class - Majordomo Protocol Client API
-- Implements the MDP/Worker spec at http://rfc.zeromq.org/spec:7.

local mdp    = require "mdp"
local utils  = require "mdutils"
local zmq    = require "lzmq"
local zloop  = require "lzmq.loop"
local ztimer = require "lzmq.timer"

local MDPC_CLIENT = mdp.MDPC_CLIENT

local zassert = zmq.assert

local push_front, printf, dump_msg = utils.push_front, utils.printf, utils.dump_msg

local function connect_to_broker(self)
  if self._private.client then
    self._private.client:close()
    self._private.client = nil
  end
  local err
  self._private.client, err = self._private.context:socket{zmq.REQ,
    linger   = 0;
    rcvtimeo = self._private.timeout;
    connect  = self._private.broker;
  }
  zassert(self._private.client, err)
  self:log("I: connecting to broker at %s...\n", self._private.broker)
end

local mdcli = {} do

mdcli.__index = mdcli

function mdcli:new(broker, verbose)
  local o = setmetatable({
    _private = {
      broker  = broker;
      verbose = verbose;
      timeout = 2500;
      retries = 3;
      context = zmq.context();
      logger  = printf;
    }
  }, self)
  connect_to_broker(o)
  return o
end

function mdcli:log(...)
  if self._private.verbose then
    self._private.logger(...)
  end
end

function mdcli:log_msg(msg, ...)
  if self._private.verbose then
    self._private.logger(...)
    dump_msg(msg) --@todo use logger to dump
  end
end

function mdcli:destroy()
  if not self._private.context then
    return
  end

  if self._private.client then
    self._private.client:close()
    self._private.client = nil
  end

  self._private.context:destroy()
  self._private.context = nil
end

mdcli.__gc = mdcli.destroy

function mdcli:send(service, request)
  assert(request)

  local retries_left = self._private.retries

  if type(request) == "string" then
    request = {MDPC_CLIENT, service, request}
  else
    push_front(request, service)
    push_front(request, MDPC_CLIENT)
  end

  self:log_msg(request, "I: send request to '%s' service:\n", service)

  while retries_left > 0 do
    self._private.client:send_all(request)
    local reply, err = self._private.client:recv_all()

    if reply then -- If we got a reply, process it

      self:log_msg(reply, "I: received reply:\n")

      -- We would handle malformed replies better in real code
      assert(#reply >= 3)

      local header = table.remove(reply, 1)
      assert(header == MDPC_CLIENT)

      local reply_service = table.remove(reply, 1)
      assert(reply_service == service)

      return reply -- Success
    end

    -- On any blocking call, libzmq will return -1 if there was
    -- an error; we could in theory check for different error codes,
    -- but in practice it's OK to assume it was EINTR (Ctrl-C):
    if err:no() == zmq.EINTR then break end

    -- here we expect timeout only

    zassert(err:no() == zmq.EAGAIN, err)

    retries_left = retries_left - 1
    if retries_left > 0 then
      self:log("W: no reply, reconnecting...\n")
      connect_to_broker(self)
    else
      self:log("W: permanent error, abandoning\n")
      connect_to_broker(self)
      break -- Give up
    end
  end
end

-- These are the class methods. We can set the request timeout and number
-- of retry attempts before sending requests:

--Request timeout

function mdcli:set_timeout(timeout)
  self._private.timeout = timeout
  if self._private.client then
    self._private.client.set_rcvtimeo(timeout)
  end
end

function mdcli:timeout()
  return self._private.timeout
end

-- Request retries

function mdcli:set_retries(retries)
  self._private.retries = retries
end

function mdcli:retries()
  return self._private.retries
end

end

local cli = mdcli:new("tcp://127.0.0.1:5555")
local msg = cli:send("ECHO", "HELLO")

return {
  new = function(...) return mdcli:new(...) end
}
