local _M = {}

local List = {} do
List.__index = List

function List:new()
  return setmetatable({first_ = 0;last_ = -1}, self)
end

function List:size()
  local n = self.last_ - self.first_ + 1
  if n < 0 then return 0 end
  return n
end

List.__len = List.size

function List:empty()
  return self:size() == 0
end

function List:push_front(value)
  self.first_ = self.first_ - 1
  self[self.first_] = value
end

function List:push_back(value)
  self.last_ = self.last_ + 1
  self[self.last_] = value
end

function List:pop_front()
  if self:empty() then return end
  local value = self[self.first_]
  self[self.first_] = nil
  self.first_ = self.first_ + 1
  return value
end

function List:pop_back()
  if self:empty() then return end
  local value = self[self.last_]
  self[self.last_] = nil
  self.last_ = self.last_ - 1
  return value
end

function List:first()
  if self:empty() then return end
  return self[self.first_]
end

function List:last()
  if self:empty() then return end
  return self[self.last_]
end

function List:next(i)
  i = i and i + 1 or 1
  local pos = self.first_ + i - 1
  if pos > self.last_ then return end
  return i, self[self.first_ + i - 1]
end

function List:ipairs()
  return self.next, self
end

function List:remove(i)
  assert((i > 0) and (i <= self:size()))
  local elem = self[self.first_ + i - 1]
  for i = self.first_ + i - 1, self.last_ - 1 do
    self[i] = self[i + 1]
  end
  self[self.last_] = nil
  self.last_ = self.last_ - 1
  return elem
end

function List:index_(i)
  return self.first_ + i - 1
end

function List:remove_multi(t)
  for i = #t, 1, -1 do
    self:remove(t[i])
  end
end

-- function List:__tostring()
--   return  "List: {" .. table.concat(self, ",", self.first_, self.last_) .. "}"
-- end

end

_M.List = List

function _M.push_front(t, value)
  return table.insert(t, 1, value)
end

function _M.push_back(t, value)
  return table.insert(t, value)
end

function _M.pop_front(t)
  return table.remove(t, 1)
end

function _M.pop_back(t)
  return table.remove(t)
end

function _M.zmsg_unwrap(msg)
  local frame = _M.pop_front(msg)
  if msg[1] == ""  then _M.pop_front(msg) end
  return frame
end

function _M.zmsg_wrap(msg, frame)
  _M.push_front(msg, "")
  _M.push_front(msg, frame)
  return msg
end

function _M.hex(str)
  local result = ""
  for i=1, #str do
    result = result .. string.format("%02X", str:byte(i))
  end
  return result
end

function _M.printf(fmt, ...)
  return io.write((string.format(fmt, ...)))
end

local function dump_str(str)
  --  Dump the message as text or binary
  local function is_text(data)
    for i=1, #data do
      local c = data:byte(i)
      if (c < 32 or c > 127) then
        return false
      end
    end
    return true
  end

  local result = string.format("[%03d] ", #str)

  if is_text(str) then
    result = result .. str
  else
    for i=1, #str do
      result = result .. string.format("%02X", str:byte(i))
    end
  end

  return result
end

function _M.dump_msg(msg)
  io.write("----------------------------------------\n")
  for _, data in ipairs(msg) do
    io.write(dump_str(data), "\n")
  end
end

return _M

