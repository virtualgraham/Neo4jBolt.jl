#=

JuliaBolt is ported from Neobolt: Neo4j Bolt connector for Python
https://github.com/neo4j-drivers/neobolt

Original notices:

# Copyright (c) 2002-2018 "Neo4j,"
# Neo4j Sweden AB [http://neo4j.com]
#
# This file is part of Neo4j.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

=#

using Sockets

#####################################
#           Message Frame           #
#####################################

const empty_view = UInt8[]

mutable struct MessageFrame
   data_view::SubArray{UInt8}
   current_pane::Integer # index of current pane in panes
   current_offset::Integer # index within the current pane
   panes::Union{Array{Tuple{Integer,Integer}}, Nothing}

   MessageFrame(data_view) = new(data_view, -1, -1, nothing)
   MessageFrame(data_view, panes) = new(data_view, 0, 0, panes)
end

# Message Frame -------------------- #

function close(m::MessageFrame)
    # what would setting data_view to nothing accomplish other than putting the object in an unusable and invalid state? 
    # perhaps freeing up memory for the garbage collector?
    return
end

# Message Frame -------------------- #

function next_pane(m::MessageFrame)
    m.current_pane += 1
    if m.current_pane < length(m.panes)
        m.current_offset = 0
    else
        m.current_pane = -1
        m.current_offset = -1
    end
end

# Message Frame -------------------- #

function read_int(m::MessageFrame)
    if m.current_pane == -1
        return -1
    end
    (p, q) = m.panes[m.current_pane + 1] #index
    size = q - p
    value = m.data_view[p + m.current_offset + 1] #index
    m.current_offset += 1
    if m.current_offset == size
        next_pane(m)
    end
    return value
end

# Message Frame -------------------- #

function read(m::MessageFrame, n::Integer)
    if n == 0 || m.current_pane == -1
        return empty_view
    end
    value::Union{AbstractArray, Nothing} = nothing
    is_view = false
    offset = 0

    to_read = n
    while to_read > 0 && m.current_pane >= 0
        (p, q) = m.panes[m.current_pane + 1] #index
        size = q - p
        remaining = size - m.current_offset
        _start = p + m.current_offset
        if to_read <= remaining
            _end = _start + to_read
            if to_read < remaining
                m.current_offset += to_read
            else
                next_pane(m)
            end
        else
            _end = q
            next_pane(m)
        end

        read = _end - _start

        if value !== nothing
            if is_view
                new_value = zeros(UInt8, n)
                new_value[1:offset] = value[1:offset] #index
                value = new_value
                is_view = false
            end
            value[(offset+1):(offset+read)] = m.data_view[(_start+1):_end] #index
        else
            value = Base.view(m.data_view, (_start+1):_end) #index
            is_view = true
        end
        offset += read
        to_read -= read
    end
    return Base.view(value, :)
end

# Message Frame ------------------- #
# # # # # # # # # # # # # # # # # # #



#####################################
#        Chunked Input Buffer       #
#####################################

# TODO: Fix indexing to 1 based

mutable struct ChunkedInputBuffer
    data::Array{UInt8}
    data_view::SubArray{UInt8}
    extent::Integer
    origin::Integer
    limit::Integer
    frame::Union{MessageFrame, Nothing}

    function ChunkedInputBuffer(;capacity::Integer=524288) 
        data = zeros(UInt8, capacity)
        return new(data, Base.view(data, :), 0, 0, -1, nothing) 
    end
end

# Chunked Input Buffer ------------ #

function capacity(c::ChunkedInputBuffer)
    return length(c.data_view)
end

# Chunked Input Buffer ------------ #

function Base.view(c::ChunkedInputBuffer)
    return Base.view(c.data_view, 1:c.extent) #index
end

# Chunked Input Buffer ------------ #

function load(c::ChunkedInputBuffer, b::AbstractArray{UInt8})
    n = length(b)
    new_extent = c.extent + n
    overflow = new_extent - length(c.data)
    if overflow > 0 # if new data does not fit in data array
        if recycle(c)
            return load(c, b)
        end
        # c.data_view = nothing
        new_extent = c.extent + n

        if new_extent > length(c.data)
            resize!(c.data, new_extent)
        end
        
        c.data[(c.extent+1):new_extent] = b #index
        c.data_view = Base.view(c.data, :)
    else
        c.data_view[(c.extent + 1):new_extent] = b #index
    end
    c.extent = new_extent
end

function read_some(s::IO, limit=0)
    b = []
    try
        while true
            t = Base.read(s, UInt8)
            #println("> ", Char(t))
            append!(b, t)
            if t == 0x00 || (limit > 0 && length(b) == limit)
                break
            end
        end
    catch EOFError
        println(">EOFError")
    end
    return b
end 

function read_some_into(s::IO, b::AbstractArray, limit=length(b))
    try
        i = 1
        while true
            if i > length(b) || (limit != length(b) && i > limit)
                return i - 1
            end
            t = Base.read(s, UInt8)
            #println("> ", Char(t))
            b[i] = t
            i += 1
            if t == 0x00
                return i - 1
            end
        end
    catch EOFError
        println(">EOFError")
    end
end 

function receive(c::ChunkedInputBuffer, s::IO, n::Integer)
    try
        new_extent = c.extent + n
        overflow = new_extent - length(c.data)
        if overflow > 0
            if recycle(c)
                return receive(c, s, n)
            end
            data = read_some(s, n)
            data_size = length(data)
            new_extent = c.extent + data_size

            if new_extent > length(c.data)
                resize!(c.data, new_extent)
            end
        
            c.data[(c.extent+1):new_extent] = data #index
            c.data_view = Base.view(c.data, :)
        else
            data_size = read_some_into(s, Base.view(c.data_view, (c.extent+1):new_extent)) #index
            new_extent = c.extent + data_size
        end
        c.extent = new_extent
        return data_size
    catch e
        println("receive error")
        return -1
    end
end

# Chunked Input Buffer ------------ #

function receive_message(c::ChunkedInputBuffer, s::IO, n::Integer)
    while !frame_message(c)
        received = receive(c, s, n)
        if received <= 0
            return received
        end
    end
    return 1
end

# Chunked Input Buffer ------------ #

function recycle(c::ChunkedInputBuffer)
    origin = c.origin
    if origin == 0
        return false
    end
    available = c.extent - origin 
    ### here ###
    c.data[1:available] = c.data[(origin+1):c.extent] #index
    c.extent = available
    c.origin = 0
    return true
end

# Chunked Input Buffer ------------ #

function frame_message(c::ChunkedInputBuffer)
    if c.frame !== nothing
        discard_message(c)
    end
    panes = Tuple{Integer, Integer}[]
    p = origin = c.origin
    extent = c.extent
    while p < extent
        available = extent - p
        if available < 2
            break
        end

        # manually unpack
        # chunk_size, = struct_unpack(">H", c.view[p:(p+2)])
        _chunk_size = reinterpret(UInt16, c.data_view[(p+1):(p+2)]) #index
        chunk_size = ntoh(_chunk_size[1])
          
        p += 2
        if chunk_size == 0
            c.limit = p
            c.frame = MessageFrame(Base.view(c.data_view, (origin+1):c.limit), panes) #index
            return true
        end
        q = p + chunk_size
        push!(panes, (p - origin, q - origin))
        p = q
    end
    return false
end

# Chunked Input Buffer ------------ #

function discard_message(c::ChunkedInputBuffer)
    if c.frame !== nothing
        close(c.frame)
        c.origin = c.limit
        c.limit = -1
        c.frame = nothing
    end
end

# Chunked Input Buffer ------------ #

function Base.show(io::IO, m::ChunkedInputBuffer)
  Base.show(io, m.data)
end

# Chunked Input Buffer ------------ #
# # # # # # # # # # # # # # # # # # #


#####################################
#       Chunked Output Buffer       #
#####################################

mutable struct ChunkedOutputBuffer
    max_chunk_size::Integer
    header::Integer
    _start::Integer
    _end::Integer
    data::Array{UInt8}
    
    ChunkedOutputBuffer(;capacity::Integer=1048576, max_chunk_size::Integer=16384) = new(max_chunk_size, 0, 2, 2, zeros(UInt8, capacity))
end

# Chunked Output Buffer ----------- #

function clear(c::ChunkedOutputBuffer)
    c.header = 0
    c._start = 2
    c._end = 2
    c.data[1:2] = [0x00, 0x00] #index
end

# Chunked Output Buffer ----------- #

function Base.write(c::ChunkedOutputBuffer, b::AbstractArray{UInt8})
    to_write = length(b)
    max_chunk_size = c.max_chunk_size
    pos = 0
    while to_write > 0
        chunk_size = c._end - c._start
        remaining = max_chunk_size - chunk_size
        if remaining == 0 || remaining < to_write <= max_chunk_size
            chunk(c)
        else
            wrote = min(to_write, remaining)
            new_end = c._end + wrote

            if new_end > length(c.data)
                resize!(c.data, new_end)
            end
        
            c.data[(c._end+1):new_end] = b[(pos+1):(pos+wrote)] #index
            c._end = new_end
            pos += wrote

            new_chunk_size = UInt16(c._end - c._start)
            # c.data[c.header:(c.header + 2)] = struct_pack(">H", new_chunk_size)
            _new_chunk_size = hton(new_chunk_size)
            c.data[(c.header+1):(c.header + 2)] = reinterpret(UInt8, [_new_chunk_size]) # index
           
            to_write -= wrote
        end
    end
end

# Chunked Output Buffer ----------- #

function chunk(c::ChunkedOutputBuffer)
    c.header = c._end
    c._start = c.header + 2
    c._end = c._start

    if c._start > length(c.data)
        resize!(c.data, c._start)
    end
    c.data[(c.header+1):c._start] = [0x00,0x00] #index
end

# Chunked Output Buffer ----------- #

function Base.view(c::ChunkedOutputBuffer)
    _end = c._end
    chunk_size = _end - c._start
    if chunk_size == 0
        return Base.view(c.data, 1:c.header) #index
    else
        return Base.view(c.data, 1:_end) #index
    end
end

# Chunked Output Buffer ----------- #
# # # # # # # # # # # # # # # # # # #