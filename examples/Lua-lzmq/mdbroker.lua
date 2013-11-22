-- Majordomo Protocol broker
-- A minimal Lua implementation of the Majordomo Protocol as defined in
-- http://rfc.zeromq.org/spec:7 and http://rfc.zeromq.org/spec:8.

----------------------------------------------------------------------------

-- usage
-- local SERVER_ENDPOINT    = "tcp://*:5555"
-- Broker:new(SERVER_ENDPOINT, true):start()
----------------------------------------------------------------------------

local HEARTBEAT_LIVENESS = 3    -- 3-5 is reasonable
local HEARTBEAT_INTERVAL = 2500 -- msecs
local HEARTBEAT_EXPIRY   = HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS

----------------------------------------------------------------------------

local mdp    = require "mdp"
local utils  = require "mdutils"
local zmq    = require "lzmq"
local zloop  = require "lzmq.loop"
local ztimer = require "lzmq.timer"

local MDPW_WORKER, MDPC_CLIENT = mdp.MDPW_WORKER, mdp.MDPC_CLIENT

local MDPW_REPLY, MDPW_READY, MDPW_HEARTBEAT, MDPW_REQUEST, MDPW_DISCONNECT = 
  mdp.MDPW_REPLY, mdp.MDPW_READY, mdp.MDPW_HEARTBEAT, mdp.MDPW_REQUEST, mdp.MDPW_DISCONNECT

local List, push_front, pop_front, zmsg_unwrap, zmsg_wrap, hex, printf, dump_msg = 
  utils.List, utils.push_front, utils.pop_front, utils.zmsg_unwrap, 
  utils.zmsg_wrap, utils.hex, utils.printf, utils.dump_msg

local HEARTBEAT_MSG  = {nil, "", MDPW_WORKER, MDPW_HEARTBEAT  }
local DISCONNECT_MSG = {nil, "", MDPW_WORKER, MDPW_DISCONNECT }

local Service = {} do

Service.__index = Service

function Service:new(broker, name)
  local o = setmetatable({
    broker   = broker;
    name     = name;
    workers  = List:new();
    requests = List:new();
  }, self)

  return o
end

function Service:dispatch(msg)
  if msg then-- Queue message if any
    self.requests:push_back(msg)
  end

  self.broker:purge() -- Remove dead workers

  while (not self.workers:empty()) and (not self.requests:empty()) do
    local worker = self.workers:pop_front()
    local msg    = self.requests:pop_front()
    self.broker:send_all(
      {worker.identity, "", MDPW_WORKER, MDPW_REQUEST},
      zmq.SNDMORE
    )
    self.broker:send_all(msg)
  end
end

function Service:heartbeat()
  for _, worker in self.workers:ipairs() do
    HEARTBEAT_MSG[1] = worker.identity
    self.broker:send_all(HEARTBEAT_MSG)
  end
end

function Service:purge()
  local rem
  for i, worker in self.workers:ipairs() do
    if worker:expire() then
      rem = rem or {}
      table.insert(rem, i)
      worker.service = nil
      worker:destroy()
      self.broker:log("I: deleting expired worker: %s\n", worker.name)
    end
  end
  if rem then
    self.workers:remove_multi(rem)
  end
end

function Service:add_worker(worker)
  self.workers:push_back(worker)
  assert((worker.service == self) or (worker.service == nil))
  worker.service = self
end

function Service:remove_worker(worker)
  for i, w in self.workers:ipairs() do
    if w == worker then
      self.workers:remove(i)
      assert(worker.service == self)
      worker.service = nil
      return worker
    end
  end
end

end

local Worker = {} do
Worker.__index = Worker

function Worker:new(broker, identity)
  local o = setmetatable({
    broker   = broker;
    hbtimer  = ztimer.monotonic(HEARTBEAT_EXPIRY):start();
    identity = identity;
    name     = hex(identity);
  }, self)
  broker.workers[identity] = o

  return o
end

function Worker:expire()
  return self.hbtimer:rest() == 0
end

function Worker:destroy(send_disconnect)
  self.hbtimer:close()
  self.broker.workers[self.identity] = nil

  if self.service then
    self.service:remove_worker(self)
    self.service = nil
  end

  if send_disconnect then
    DISCONNECT_MSG[1] = self.identity
    self.broker:send_all(DISCONNECT_MSG)
  end

  self.identity = nil
  self.broker   = nil
end

function Worker:ready()
  assert(self.service)
  self.service:add_worker(self)
  self.hbtimer:start()
end

function Worker:restart()
  self.hbtimer:start()
end

end

local Services = {} do

Services.__index = Services

function Services:new(broker)
  local o = setmetatable({
    broker   = broker;
    services = {};
  }, self)

  return o
end

function Services:require(name)
  local service = self.services[name]
  if service then return service end
  service = Service:new(self.broker, name)
  self.services[name] = service

  self.broker:log("I: added service: %s\n", name)

  return service
end

function Services:heartbeat()
  for _, service in pairs(self.services) do
    service:heartbeat()
  end
end

function Services:purge()
  for _, service in pairs(self.services) do
    service:purge()
  end
end

function Services:exists(name)
  return self.services[name]
end

end

local Broker = {} do

Broker.__index = Broker

function Broker:new(endpoint, verbose)
  local context = zmq.context()
  local socket  = context:socket{zmq.ROUTER, bind = endpoint}
  local o = setmetatable({
    context  = context;
    socket   = socket;
    endpoint = endpoint;
    workers  = {};
    verbose  = verbose;
  }, self)
  o.services = Services:new(o)

  o:log("I: MDP broker/0.2.0 is active at %s\n", endpoint)

  return o
end

function Broker:log(...)
  if self.verbose then
    printf(...)
  end
end

function Broker:log_msg(msg, ...)
  if self.verbose then
    printf(...)
    dump_msg(msg)
  end
end

function Broker:send_all(...)
  return self.socket:send_all(...)
end

function Broker:sendx(...)
  return self.socket:sendx(...)
end

function Broker:heartbeat()
  self.services:heartbeat()
end

function Broker:purge()
  self.services:purge()
end

function Broker:worker(identity)
  local worker = self.workers[identity]
  if worker then return worker end

  worker = Worker:new(self, identity)

  self:log("I: registering new worker: %s\n", worker.name)

  return worker, true
end

function Broker:service(identity)
  return self.services:require(identity)
end

function Broker:on_client(sender, msg)
  assert(#msg >= 2) -- Service name + body
  local service_name = pop_front(msg)

  -- If we got a MMI service request, process that internally
  if service_name:sub(1,4) == "mmi." then
    local return_code
    if service_name == "mmi.service" then
      local name = msg[#msg]
      return_code = self.services:exists(name) and "200" or "404"
    else
      return_code = "501"
    end
    msg[#msg] = return_code

    -- insert the protocol header and service name
    push_front(msg, service_name)
    push_front(msg, MDPC_CLIENT)
    zmsg_wrap (msg, sender)

    return self:send_all(msg)
  end

  -- Else dispatch the message to the requested service
  -- Set reply return identity to client sender
  local service = self:service(service_name)
  service:dispatch(zmsg_wrap(msg, sender))
end

function Broker:on_worker(sender, msg)
  assert(#msg >= 1) -- At least, command

  local worker, new = self:worker(sender)
  local worker_ready = not new
  local command = pop_front(msg)

  if command == MDPW_READY then
    if not new then -- Not first command in session
      return worker:destroy(true)
    end

    -- Reserved service name
    if sender:sub(1, 4) == "mmi." then
      return worker:destroy(true)
    end

    -- Attach worker to service and mark as idle
    local service_frame = pop_front(msg)
    assert(service_frame)
    local service = self:service(service_frame)
    return service:add_worker(worker)
  end

  if command == MDPW_REPLY then
    if new then -- first message coul not be REPLY
      return worker:destroy(true)
    end

    -- Remove and save client return envelope and insert the
    -- protocol header and service name, then rewrap envelope.
    local client = zmsg_unwrap(msg)
    push_front(msg, worker.service.name)
    push_front(msg, MDPC_CLIENT)
    zmsg_wrap (msg, client)
    self:send_all(msg)
    return worker:ready()
  end

  if command == MDPW_HEARTBEAT then
    if new then -- first message coul not be HB
      return worker:destroy(true)
    end

    return worker:restart()
  end

  if command == MDPW_DISCONNECT then
    return worker:destroy()
  end

  self:log_msg(msg, "E: invalid input message\n");
end

function Broker:start()
  local loop = zloop.new(1, self.context)
  local self = self

  loop:add_socket(self.socket, function(sok)
    local msg = sok:recv_all()
    self:log_msg(msg, "I: received message:\n")

    local sender = pop_front(msg)
    local empty  = pop_front(msg)
    local header = pop_front(msg)

    if header == MDPC_CLIENT then
      self:on_client(sender, msg)
    elseif header == MDPW_WORKER then
      self:on_worker(sender, msg)
    else 
      self:log_msg(msg, "E: invalid message:\n")
    end
  end)

  loop:add_interval(HEARTBEAT_INTERVAL, function()
    self:purge()
    self:heartbeat()
  end)

  loop:start()

  -- loop:remove_socket(self.socket)
end

end
