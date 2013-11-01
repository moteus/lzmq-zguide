local zmq     = require "lzmq"
local ztimer  = require "lzmq.timer"

IS_WINDOWS = package.config:sub(1,1) == "\\"

zassert = zmq.assert

function sleep(sec)
  ztimer.sleep(sec * 1000)
end

function s_sleep(msec)
  ztimer.sleep(msec)
end

function printf(fmt, ...)
  return io.write((string.format(fmt, ...)))
end

function fprintf(file, fmt, ...)
  return file:write((string.format(fmt, ...)))
end

function sprintf(fmt, ...)
  return (string.format(fmt, ...))
end

function randof(n)
  if type(n) == 'number' then
    return math.random(1, n) - 1
  end
  return n[math.random(1, #n)]
end

function getchar()
  io.read(1)
end

function s_dump(socket)
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

  print("----------------------------------------")
  --  Process all parts of the message
  local msg = socket:recv_all()
  for _, data in ipairs(msg) do
    printf("[%03d] ", #data)
    if is_text(data) then
      io.write(data)
    else
      for i=1, #data do
        printf("%02X", data:byte(i))
      end
    end
    printf("\n")
  end
end

