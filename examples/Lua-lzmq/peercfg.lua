-- config file for peeringX.lua

if not IS_WINDOWS then

function CLOUD(name)
  return sprintf("ipc://%s.cloud.ipc", name)
end

function STATE(name)
  return sprintf("ipc://%s.state.ipc", name)
end

function LOCALBE(name)
  return sprintf("ipc://%s.localbe.ipc", name)
end

function LOCALFE(name)
  return sprintf("ipc://%s.localfe.ipc", name)
end

function MONITOR(name)
  return sprintf("ipc://%s.monitor.ipc", name)
end

return

end

-- ZMQ does not support ipc for Windows

local BASE_PORT = 5555
local PEERS = {"DC1","DC2","DC3"}

local cur_port = BASE_PORT

local function next_port()
  cur_port = cur_port + 1
  return cur_port - 1
end

local function next_addr(name)
  return "tcp://127.0.0.1:" .. next_port()
end

local function gen_addresses(peer)
  local result = {}
  for _, peer in ipairs(peer) do
    result[peer] = next_addr(peer)
  end
  return result
end

local addresses = {
  state   = gen_addresses(PEERS);
  cloud   = gen_addresses(PEERS);
  localfe = gen_addresses(PEERS);
  localbe = gen_addresses(PEERS);
  monitor = gen_addresses(PEERS);
}

function CLOUD(name)
  return assert(addresses.cloud[name])
end

function STATE(name)
  return assert(addresses.state[name])
end

function LOCALBE(name)
  return assert(addresses.localbe[name])
end

function LOCALFE(name)
  return assert(addresses.localfe[name])
end

function MONITOR(name)
  return assert(addresses.monitor[name])
end
