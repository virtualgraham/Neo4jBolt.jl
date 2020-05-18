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

#####################################
#             Unpacker              #
#####################################

mutable struct Unpacker
    source::Union{MessageFrame, Nothing}

    Unpacker() = new(nothing)
end

# Unpacker ------------------------ #

read(u::Unpacker, n::Integer=1) = read(u.source, n)

# Unpacker ------------------------ #

read_int(u::Unpacker) = read_int(u.source)

# Unpacker ------------------------ #

function unpack(u::Unpacker)
    marker = read_int(u)

    if marker == -1
        throw(ErrorException("Nothing to unpack"))
    end
    
    # Tiny Integer
    if 0x00 <= marker <= 0x7F
        return marker
    elseif 0xF0 <= marker <= 0xFF
        return Int8(marker - 256)
    
    # Null
    elseif marker == 0xC0
        return nothing
    
    # Float
    elseif marker == 0xC1
        return struct_unpack(Float64, read(u, 8))
        
    # Boolean
    elseif marker == 0xC2
        return false
    elseif marker == 0xC3
        return true

    # Integer
    elseif marker == 0xC8
        return struct_unpack(Int8, read(u, 1))
    elseif marker == 0xC9
        return struct_unpack(Int16, read(u, 2))
    elseif marker == 0xCA
        return struct_unpack(Int32, read(u, 4))
    elseif marker == 0xCB
        return struct_unpack(Int64, read(u, 8))
        

    # Bytes
    elseif marker == 0xCC
        size_uint8 = struct_unpack(UInt8, read(u, 1))
        return read(u, size_uint8)
    elseif marker == 0xCD
        size_uint16 = struct_unpack(UInt16, read(u, 2))
        return read(u, size_uint16)
    elseif marker == 0xCE
        size_uint32 = struct_unpack(UInt32, read(u, 4))
        return read(u, size_uint32)

    else
        marker_high = marker & 0xF0
        # String
        if marker_high == 0x80  # TINY_STRING
            return String(read(u, marker & 0x0F))
        elseif marker == 0xD0  # STRING_8
            size_uint8 = struct_unpack(UInt8, read(u, 1))
            return String(read(u, size_uint8))
        elseif marker == 0xD1  # STRING_16
            size_uint16 = struct_unpack(UInt16, read(u, 2))
            return String(read(u, size_uint16))
        elseif marker == 0xD2  # STRING_32
            size_uint32 = struct_unpack(UInt32, read(u, 4))
            return String(read(u, size_uint32))

        # List
        elseif 0x90 <= marker <= 0x9F || 0xD4 <= marker <= 0xD7
            return unpack_list(u, marker)

        # Map
        elseif 0xA0 <= marker <= 0xAF || 0xD8 <= marker <= 0xDB
            return unpack_map(u, marker)

        # Structure
        elseif 0xB0 <= marker <= 0xBF || 0xDC <= marker <= 0xDD
            (size, tag) = unpack_structure_header(u, marker)
            value = Structure(tag, Array{Any}(nothing, size))
            for i in 1:length(value.fields)
                value.fields[i] = unpack(u)
            end
            return value

        elseif marker == 0xDF  # END_OF_STREAM
            return :EndOfStream

        else
            throw(ErrorException("Unknown PackStream marker $(marker)"))

        end
    end
end

# Unpacker ------------------------ #

function unpack_list(u::Unpacker)
    marker = read_int(u)
    return unpack_list(u, marker)
end

# Unpacker ------------------------ #

function unpack_list(u::Unpacker, marker::UInt8)
    marker_high = marker & 0xF0
    if marker_high ==  0x90
        size_ = marker & 0x0F
        if size_ == 0
            return []
        elseif size_ == 1
            return [unpack(u)]
        else
            return [unpack(u) for _ in 1:size_]
        end
    elseif marker == 0xD4  # LIST_8
        size_uint8 = struct_unpack(UInt8, read(u, 1))
        return [unpack(u) for _ in 1:size_uint8]
    elseif marker == 0xD5  # LIST_16
        size_uint16 = struct_unpack(UInt16, read(u, 2))
        return [unpack(u) for _ in 1:size_uint16]
    elseif marker == 0xD6  # LIST_32
        size_uint32 = struct_unpack(UInt32, read(u, 4))
        return [unpack(u) for _ in 1:size_uint32]
    elseif marker == 0xD7  # LIST_STREAM
        value = []
        item = nothing
        while item != :EndOfStream
            item = unpack(u)
            if item != :EndOfStream
                push!(value, item)
            end
        end
        return value
    else
        return nothing
    end
end

# Unpacker ------------------------ #

function unpack_map(u::Unpacker)
    marker = read_int(u)
    if 0 <= marker <= 255
        return unpack_map(u, marker)
    else
        return nothing
    end
end

# Unpacker ------------------------ #

function unpack_map(u::Unpacker, marker::UInt8)
    marker_high = marker & 0xF0
    if marker_high == 0xA0
        size = marker & 0x0F
        value = Dict()
        for _ in 1:size
            key = unpack(u)
            value[key] = unpack(u)
        end
        return value
    elseif marker == 0xD8  # MAP_8
        size = struct_unpack(UInt8, read(u, 1))
        value = Dict()
        for _ in 1:size
            key = unpack(u)
            value[key] = unpack(u)
        end
        return value
    elseif marker == 0xD9  # MAP_16
        size, = struct_unpack(UInt16, read(u, 2))
        value = Dict()
        for _ in 1:size
            key = unpack(u)
            value[key] = unpack(u)
        end
        return value
    elseif marker == 0xDA  # MAP_32
        size, = struct_unpack(UInt32, read(u, 4))
        value = Dict()
        for _ in 1:size
            key = unpack(u)
            value[key] = unpack(u)
        end
        return value
    elseif marker == 0xDB  # MAP_STREAM
        value = Dict()
        key = nothing
        while key != :EndOfStream
            key = unpack(u)
            if key != :EndOfStream
                value[key] = unpack(u)
            end
        end
        return value
    else
        return nothing
    end
end

# Unpacker ------------------------ #

function unpack_structure_header(u::Unpacker)
    marker = read_int(u)
    if marker == -1
        return nothing, nothing
    else
        return unpack_structure_header(u, marker)
    end
end

# Unpacker ------------------------ #

function unpack_structure_header(u::Unpacker, marker::UInt8)
    marker_high = marker & 0xF0
    if marker_high == 0xB0  # TINY_STRUCT
        signature = read(u, 1)[1]
        return marker & 0x0F, signature
    elseif marker == 0xDC  # STRUCT_8
        size = struct_unpack(UInt8, read(u, 1))
        signature = read(u, 1)[1]
        return size, signature
    elseif marker == 0xDD  # STRUCT_16
        size = struct_unpack(UInt16, read(u, 2))
        signature = read(u, 1)[1]
        return size, signature
    else
        throw(ErrorException("Expected structure, found marker $(marker)"))
    end
end

# Unpacker ------------------------ #
# # # # # # # # # # # # # # # # # # #