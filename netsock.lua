local component = require("component")
local event = require("event")
local uuid = require("uuid")
local serial = require("serialization")

local modem = component.modem

local MAX_PACKET_LENGTH = 8

local net = { version="1.0", timeout=5 }

local requests = {
    create=1,
    ping=2,
    destroy=3,
    write=4,
    ok=5,
    err=6
}

-- Possible request statuses and actions
net.statuses = {}
for k, v in pairs(requests) do net.statuses[k] = v end

local function txframe_new(request, port, kvparams, length)
    return serial.serialize({
        proto="netsock",
        version=net.version,
        settings=kvparams,
        request=request,
        uid=uuid.next(),
        length=length or 0,
        target=modem.address,
        port=port
    })
end

local function receive(addr, port, timeout)
    local function filter(e, _, sender, fport)
        return e == "modem_message" and sender == addr and fport == port
    end
    return event.pullFiltered(timeout, filter)
end

-- Creates a connection (or a server listener if addr == null). Adds an event listener. Returns an error if the connection cannot be established
-- Returns: streamlike (rw) || nil, err
function net.create(addr, port, kvparams)
    checkArg(1, addr, "string", "nil")
    checkArg(2, port, "number")

    local conn = {
        status="alive",
        addr=addr,
        port=port,
        internal={
            kvparams=kvparams,
            opened = modem.open(port),
            buffer={},
            listeners={}
        }
    }

    if conn.addr then
        local txframe = txframe_new(requests.create, conn.port, kvparams)
        modem.send(addr, port, txframe)
        local success, _, _, _, _, rxframe = receive(addr, port, net.timeout)
        if not success then
            if conn.internal.opened then modem.close(port) end
            return nil, "no reply"
        end
        rxframe = serial.unserialize(rxframe)
        if rxframe.request ~= requests.ok then
            return nil, "connection refused or failed: "..rxframe.request
        end
    else
        conn.connections = {}
    end

    -- Find out if internal buffer has messages
    -- Returns: bool
    function conn:can_read()
        return #self.internal.buffer > 0
    end

    -- Read first frame from the buffer or `count` frames (or less if buffer is not long enough)
    -- Returns: frame || table<frame> || nil, err?
    function conn:read(count)
        if not count then
            if not self:can_read() then return nil end
            return table.remove(self.internal.buffer, 1)
        elseif checkArg(1, count, "number") then
            if count < 1 then return nil, "count cannot be less than 1" end
            local tbl = {}
            for i = 1, count do
                table.insert(tbl, self:read())
            end
            return tbl
        end
    end

    -- Writes up to MAX_PACKET_LENGTH - 1 variables to the socket. Returns whether the message was sent or not (not 'received', 'sent')
    -- Returns: bool || nil, err
    function conn:write(...)
        local data = {...}
        if #data > (MAX_PACKET_LENGTH - 1) then
            return nil, "max packet size exceeded; packing is not implemented yet"
        end
        local frame = txframe_new(requests.write, self.internal.kvparams, #data)
        if self.addr then
            return modem.send(self.addr, self.port, frame, table.unpack(data))
        else
            return modem.broadcast(self.port, frame, table.unpack(data))
        end
    end

    -- Use this on server side. Allows a server to reply to a specific host
    -- Writes up to MAX_PACKET_LENGTH - 1 variables to the socket. Returns whether the message was sent or not (not 'received', 'sent')
    -- Returns: bool || nil, err
    function conn:send(frame, ...)
        local data = {...}
        if #data > (MAX_PACKET_LENGTH - 1) then
            return nil, "max packet size exceeded; packing is not implemented yet"
        end
        local tf = txframe_new(requests.write, self.port, self.internal.kvparams, #data)
        return modem.send(frame.target, self.port, tf, table.unpack(data))
    end

    -- Use this on server side. Allows a server to reply to a specific host
    -- Writes up to MAX_PACKET_LENGTH - 1 variables to the socket. Returns whether the message was sent or not (not 'received', 'sent')
    -- Returns: bool || nil, err
    function conn:status_message(frame, status, err)
        local tf = txframe_new(status, self.port, self.internal.kvparams)
        return modem.send(frame.target, self.port, tf, err)
    end

    -- Closes the connection and unregisters the event listener. Closes the port if it was opened
    function conn:close()
        if self.addr then
            modem.send(self.addr, self.port, txframe_new(requests.destroy, self.internal.kvparams))
        else
            return modem.broadcast(self.port, txframe_new(requests.destroy, self.internal.kvparams))
        end
        event.ignore("modem_message", self.internal.mhwrapper)
        if self.internal.opened then modem.close(self.port) end
        self.status = "dead"
    end

    -- Calls `func` every time a new message appears. Returns true if a callback was added successfully
    -- Returns: bool
    function conn:listen(func)
        checkArg(1, func, "function")
        for _, v in ipairs(self.internal.listeners) do
            if v == func then return false end
        end
        table.insert(self.internal.listeners, func)
        return true
    end

    -- Removes `func` from message event listeners. Returns true if a callback was removed successfully
    -- Returns: bool
    function conn:ignore(func)
        checkArg(1, func, "function")
        for i, v in ipairs(self.internal.listeners) do
            if v == func then
                table.remove(self.internal.listeners, i)
                return true
            end
        end
        return false
    end

    function conn:set_connection_handler(func)
        checkArg(1, func, "function", "nil")
        self.internal.conn_handler = func
    end

    local function findconn(c, h, p)
        for k, v in pairs(c.connections) do
            if v[1] == h and v[2] == p then return k end
        end
        return nil
    end

    local function try(func, ...)
        if not func then return nil end
        return func(...)
    end

    local function message_handler(c, _, _, sender, pt, _, frame, ...)
        if c.status == "dead" then return false end
        frame = serial.unserialize(frame)

        -- We check if the library is set to server mode (no address set)
        -- If so, we then check if this connection is allowed. If it's not -
        -- we call the connection handler. If it returns true, we allow the connection
        -- and let the client know about it.
        -- If the user requests a write operation, we add the data to the buffer.

        if not frame then return end

        if c.addr and pt == c.port then
            if sender == c.addr then
                if frame.request == requests.create then
                    c:status_message(frame, requests.err, "already connected")
                    return
                end
                table.insert(c.internal.buffer, {frame, ...})
                for _, v in ipairs(c.internal.listeners) do v(frame, ...) end
            end
        else
            if frame.request == requests.create then
                local index = findconn(c, sender, pt)
                if index then
                    c:status_message(frame, requests.err, "already connected")
                    return
                end
                local ok, err = try(c.internal.conn_handler)
                if ok == false then
                    c:status_message(frame, requests.err, err)
                else
                    table.insert(c.connections, {sender, pt})
                    c:status_message(frame, requests.ok)
                end
            elseif frame.request == requests.destroy then
                local index = findconn(c, sender, pt)
                if index then
                    try(c.internal.conn_handler)
                    table.remove(c.connections, index)
                end
            elseif frame.request == requests.ping then
                c:status_message(frame, requests.ok)
            elseif findconn(c, sender, pt) then
                table.insert(c.internal.buffer, {frame, ...})
                for _, v in ipairs(c.internal.listeners) do v(frame, ...) end
            end
        end

    end

    function conn.internal.mhwrapper(...) return message_handler(conn, ...) end

    event.listen("modem_message", conn.internal.mhwrapper)

    return conn
end

return net
