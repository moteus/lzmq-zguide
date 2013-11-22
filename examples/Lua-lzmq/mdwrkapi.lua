-- mdwrkapi class - Majordomo Protocol Worker API
-- Implements the MDP/Worker spec at http://rfc.zeromq.org/spec:7.
----------------------------------------------------------------------------

-- usage
-- local echo = mdwrk:new("tcp://127.0.0.1:5555", "ECHO", true)
-- local msg while true do msg = echo:recv(msg) end
----------------------------------------------------------------------------

local mdp    = require "mdp"
local utils  = require "mdutils"
local zmq    = require "lzmq"
local zloop  = require "lzmq.loop"
local ztimer = require "lzmq.timer"

local zassert, unpack = zmq.assert, unpack or table.unpack

local MDPW_REPLY, MDPW_WORKER, MDPW_READY, MDPW_HEARTBEAT, MDPW_REQUEST, MDPW_DISCONNECT = 
  mdp.MDPW_REPLY, mdp.MDPW_WORKER, mdp.MDPW_READY, mdp.MDPW_HEARTBEAT, mdp.MDPW_REQUEST, mdp.MDPW_DISCONNECT

local printf, dump_msg = utils.printf, utils.dump_msg

local READY_MSG      = {"", MDPW_WORKER, MDPW_READY, "SERVICE"    }
local REPLY_MSG      = {"", MDPW_WORKER, MDPW_REPLY, "", ""       }
local HEARTBEAT_MSG  = {"", MDPW_WORKER, MDPW_HEARTBEAT           }
local DISCONNECT_MSG = {"", MDPW_WORKER, MDPW_DISCONNECT          }

-- Connect or reconnect to broker
local function connect_to_broker(self)
  if self._private.worker then
    self._private.worker:close()
    self._private.worker = nil
  end  
  self._private.worker = self._private.context:socket{zmq.DEALER,
    linger  = 0;
    connect = self._private.broker;
  };
  self:log("I: connecting to broker at %s...\n", self._private.broker)

  READY_MSG[#READY_MSG] = self._private.service

  -- Register service with broker
  self._private.worker:send_all(READY_MSG)

  self._private.heartbeat:start()
  self._private.heartbeat_at:start()
end

local mdwrk = {} do
mdwrk.__index = mdwrk

function mdwrk:new(broker, service, verbose)
  assert(broker)
  assert(service)

  local o = setmetatable({
    _private = {
      context   = zmq.context();
      broker    = broker;
      service   = service;
      verbose   = verbose;
      reconnect = 2500; -- msecs
      retries   = 3;
      logger    = printf;
    }
  }, self)

  local heartbeat = 2500; -- msecs

  -- wait heartbeat from broker
  o._private.heartbeat = ztimer.monotonic(
    heartbeat * o._private.retries
  ):start()

  -- send heartbeat to broker
  o._private.heartbeat_at = ztimer.monotonic(
    heartbeat
  ):start()

  connect_to_broker(o)
  return o
end

function mdwrk:destroy(send_disconnect)
  if not self._private.context then
    return
  end

  if self._private.worker then
    if send_disconnect then
      self._private.worker:send_all(DISCONNECT_MSG)
    end
    self._private.worker:close()
    self._private.worker = nil
  end

  self._private.context:destroy()
  self._private.context = nil
end

mdwrk.__gc = mdwrk.destroy

function mdwrk:log(...)
  if self._private.verbose then
    self._private.logger(...)
  end
end

function mdwrk:log_msg(msg, ...)
  if self._private.verbose then
    self._private.logger(...)
    dump_msg(msg) --@todo use logger to dump
  end
end

-- We provide two methods to configure the worker API. You can set the
-- heartbeat interval and retries to match the expected network performance.

-- Set heartbeat delay
function mdwrk:set_heartbeat(interval)
  self._private.heartbeat:set(interval)
end

-- Set reconnect delay
function mdwrk:set_reconnect(reconnect)
  self._private.reconnect = reconnect
end

-- This is the recv method; it's a little misnamed because it first sends
-- any reply and then waits for a new request. If you have a better name
-- for this, let me know.
-- Send reply, if any, to broker and wait for next request.
function mdwrk:recv(reply)
  assert(self._private.expect_reply or not reply)

  local worker       = self._private.worker
  -- wait heartbeat from broker
  local heartbeat    = self._private.heartbeat
  -- send heartbeat to broker
  local heartbeat_at = self._private.heartbeat_at

  if reply then
    assert(self._private.reply_to)

    REPLY_MSG[#REPLY_MSG - 1] = self._private.reply_to
    worker:send_all(REPLY_MSG, zmq.SNDMORE)
    if type(reply) == "string" then
      worker:send(reply)
    else 
      worker:send_all(reply)
    end

    self._private.reply_to = nil
  end

  self._private.expect_reply = true

  while true do
    local timeout = math.min(heartbeat:rest(), heartbeat_at:rest())
    worker:set_rcvtimeo(timeout)
    local msg, err = worker:recv_all()

    if (not msg) and (err:no() == zmq.EINTR) then break end

    -- we can send HEARTBEAT more rapidly
    if heartbeat_at:rest() < 5 then
      self._private.worker:send_all(HEARTBEAT_MSG)
      heartbeat_at:start()
    end

    if msg then
      self:log_msg(msg, "I: received message from broker:\n")
      heartbeat:start()

      -- Don't try to handle errors, just assert noisily
      assert(#msg >= 3)

      local empty = table.remove(msg, 1)
      assert(empty == "")

      local header = table.remove(msg, 1)
      assert(header == MDPW_WORKER)

      local command = table.remove(msg, 1)
      if command == MDPW_REQUEST then
        -- We should pop and save as many addresses as there are
        -- up to a null part, but for now, just save one…
        self._private.reply_to = table.remove(msg, 1)
        if msg[1] == "" then table.remove(msg, 1) end

        -- Here is where we actually have a message to process; we
        -- return it to the caller application:

        return msg -- We have a request to process
      end

      if command == MDPW_HEARTBEAT then
        -- Do nothing for heartbeats
      elseif command == MDPW_DISCONNECT then
        connect_to_broker(self)
        worker = self._private.worker
      else
        self:log_msg(msg, "E: invalid input message\n")
      end
    else
      if heartbeat:rest() == 0 then
        self:log("W: disconnected from broker - retrying...\n")
        ztimer.sleep(self._private.reconnect)
        connect_to_broker(self)
        worker = self._private.worker
      end
    end
  end
end

end

return {
  new = function(...) return mdwrk:new(...) end
}
