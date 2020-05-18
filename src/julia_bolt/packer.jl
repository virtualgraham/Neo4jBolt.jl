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

const NULL_ = 0xC0
const FALSE = 0xC2
const TRUE = 0xC3

const PACKED_UINT_8 = [struct_pack(value) for value in 0x00:0xff]
const PACKED_UINT_16 = [struct_pack(value) for value in 0x0000:0xffff]

const INT64_HI = (2^63)-1
const INT64_LO = -(2^63)


#####################################
#               Packer              #
#####################################

mutable struct Packer
    stream
    supports_bytes::Bool

    Packer(stream) = new(stream, false)
end

# Packer -------------------------- #

function pack_raw(p::Packer, data::AbstractArray{UInt8})
    write(p, data)
end

# Packer -------------------------- #

function pack(p::Packer, value::Nothing)
    write(p, [0xC0]) # NULL
end

# Packer -------------------------- #

function pack(p::Packer, value::Bool)
    if value
        write(p, [0xC3]) # TRUE
    else
        write(p, [0xC2]) # FALSE
    end
end

# Packer -------------------------- #

function pack(p::Packer, value::Float64)
    write(p, [0xC1])
    write(p, struct_pack(value))
end

# Packer -------------------------- #

function pack(p::Packer, value::Integer)
    if -16 <= value < 128
        write(p, PACKED_UINT_8[mod(value, 256)+1]) #index
    elseif -128 <= value < -16
        write(p, [0xC8])
        write(p, PACKED_UINT_8[mod(value, 256)+1]) #index
    elseif -32768 <= value < 32768
        write(p, [0xC9])
        write(p, PACKED_UINT_16[mod(value, 65536)+1]) #index
    elseif -2147483648 <= value < 2147483648
        write(p, [0xCA])
        write(p, struct_pack(Int32(value)))
    elseif INT64_LO <= value < INT64_HI
        write(p, [0xCB])
        write(p, struct_pack(Int64(value)))
    else
        throw(ErrorException("Integer $(value) out of range"))
    end
end

# Packer -------------------------- #

function pack(p::Packer, value::String)
    data = Array{UInt8}(value)
    pack_string_header(p, length(data))
    pack_raw(p, data)
end

# Packer -------------------------- #

function pack(p::Packer, value::Array{UInt8})
    pack_bytes_header(p, length(value))
    pack_raw(p, value)
end

# Packer -------------------------- #

function pack(p::Packer, value::AbstractArray)
    pack_list_header(p, length(value))
    for item in value
        pack(p, item)
    end
end

# Packer -------------------------- #

function pack(p::Packer, value::AbstractDict)
    pack_map_header(p, length(value))
    for (k, v) in value
        pack(p, k)
        pack(p, v)
    end
end

# Packer -------------------------- #

function pack(p::Packer, value::Structure)
    pack_struct(p, value.tag, value.fields)
end

# Packer -------------------------- #

function pack_bytes_header(p::Packer, size::Integer)
    if !p.supports_bytes
        throw(ErrorException("This PackSteam channel does not support BYTES (consider upgrading to Neo4j 3.2+)"))
    end

    if size < 0x100
        write(p, [0xCC])
        write(p, PACKED_UINT_8[size+1]) #index
    elseif size < 0x10000
        write(p, [0xCD])
        write(p, PACKED_UINT_16[size+1])  #index
    elseif size < 0x100000000
        write(p, [0xCE])
        write(p, struct_pack(UInt32(size)))
    else
        throw(ErrorException("Bytes header size out of range"))
    end
end

# Packer -------------------------- #

function pack_string_header(p::Packer, size::Integer)
    if size == 0x00
        write(p, [0x80])
    elseif size == 0x01
        write(p, [0x81])
    elseif size == 0x02
        write(p, [0x82])
    elseif size == 0x03
        write(p, [0x83])
    elseif size == 0x04
        write(p, [0x84])
    elseif size == 0x05
        write(p, [0x85])
    elseif size == 0x06
        write(p, [0x86])
    elseif size == 0x07
        write(p, [0x87])
    elseif size == 0x08
        write(p, [0x88])
    elseif size == 0x09
        write(p, [0x89])
    elseif size == 0x0A
        write(p, [0x8A])
    elseif size == 0x0B
        write(p, [0x8B])
    elseif size == 0x0C
        write(p, [0x8C])
    elseif size == 0x0D
        write(p, [0x8D])
    elseif size == 0x0E
        write(p, [0x8E])
    elseif size == 0x0F
        write(p, [0x8F])
    elseif size < 0x100
        write(p, [0xD0])
        write(p, PACKED_UINT_8[size+1]) #index
    elseif size < 0x10000
        write(p, [0xD1])
        write(p, PACKED_UINT_16[size+1]) #index
    elseif size < 0x100000000
        write(p, [0xD2])
        write(p, struct_pack(UInt32(size)))
    else
        throw(ErrorException("String header size out of range"))
    end
end

# Packer -------------------------- #

function pack_list_header(p::Packer, size::Integer)
    if size == 0x00
        write(p, [0x90])
    elseif size == 0x01
        write(p, [0x91])
    elseif size == 0x02
        write(p, [0x92])
    elseif size == 0x03
        write(p, [0x93])
    elseif size == 0x04
        write(p, [0x94])
    elseif size == 0x05
        write(p, [0x95])
    elseif size == 0x06
        write(p, [0x96])
    elseif size == 0x07
        write(p, [0x97])
    elseif size == 0x08
        write(p, [0x98])
    elseif size == 0x09
        write(p, [0x99])
    elseif size == 0x0A
        write(p, [0x9A])
    elseif size == 0x0B
        write(p, [0x9B])
    elseif size == 0x0C
        write(p, [0x9C])
    elseif size == 0x0D
        write(p, [0x9D])
    elseif size == 0x0E
        write(p, [0x9E])
    elseif size == 0x0F
        write(p, [0x9F])
    elseif size < 0x100
        write(p, [0xD4])
        write(p, PACKED_UINT_8[size+1]) #index
    elseif size < 0x10000
        write(p, [0xD5])
        write(p, PACKED_UINT_16[size+1]) #index
    elseif size < 0x100000000
        write(p, [0xD6])
        write(p, struct_pack(UInt32(size)))
    else
        throw(ErrorException("List header size out of range"))
    end
end

# Packer -------------------------- #

function pack_list_stream_header(p::Packer)
    write(p, [0xD7])
end

# Packer -------------------------- #

function pack_map_header(p::Packer, size::Integer)
    if size == 0x00
        write(p, [0xA0])
    elseif size == 0x01
        write(p, [0xA1])
    elseif size == 0x02
        write(p, [0xA2])
    elseif size == 0x03
        write(p, [0xA3])
    elseif size == 0x04
        write(p, [0xA4])
    elseif size == 0x05
        write(p, [0xA5])
    elseif size == 0x06
        write(p, [0xA6])
    elseif size == 0x07
        write(p, [0xA7])
    elseif size == 0x08
        write(p, [0xA8])
    elseif size == 0x09
        write(p, [0xA9])
    elseif size == 0x0A
        write(p, [0xAA])
    elseif size == 0x0B
        write(p, [0xAB])
    elseif size == 0x0C
        write(p, [0xAC])
    elseif size == 0x0D
        write(p, [0xAD])
    elseif size == 0x0E
        write(p, [0xAE])
    elseif size == 0x0F
        write(p, [0xAF])
    elseif size < 0x100
        write(p, [0xD8])
        write(p, PACKED_UINT_8[size+1]) #index
    elseif size < 0x10000
        write(p, [0xD9])
        write(p, PACKED_UINT_16[size+1]) #index
    elseif size < 0x100000000
        write(p, [0xDA])
        write(p, struct_pack(UInt32(size)))
    else
        throw(ErrorException("Map header size out of range"))
    end
end

# Packer -------------------------- #

function pack_map_stream_header(p::Packer)
    write(p, [0xDB])
end

# Packer -------------------------- #

function pack_struct(p::Packer, signiture::UInt8, fields::Array)
    size = length(fields)
    if size == 0x00
        write(p, [0xB0])
    elseif size == 0x01
        write(p, [0xB1])
    elseif size == 0x02
        write(p, [0xB2])
    elseif size == 0x03
        write(p, [0xB3])
    elseif size == 0x04
        write(p, [0xB4])
    elseif size == 0x05
        write(p, [0xB5])
    elseif size == 0x06
        write(p, [0xB6])
    elseif size == 0x07
        write(p, [0xB7])
    elseif size == 0x08
        write(p, [0xB8])
    elseif size == 0x09
        write(p, [0xB9])
    elseif size == 0x0A
        write(p, [0xBA])
    elseif size == 0x0B
        write(p, [0xBB])
    elseif size == 0x0C
        write(p, [0xBC])
    elseif size == 0x0D
        write(p, [0xBD])
    elseif size == 0x0E
        write(p, [0xBE])
    elseif size == 0x0F
        write(p, [0xBF])
    elseif size < 0x100
        write(p, [0xDC])
        write(p, PACKED_UINT_8[size+1]) #index
    elseif size < 0x10000
        write(p, [0xDD])
        write(p, PACKED_UINT_16[size+1]) #index
    else
        throw(ErrorException("String header size out of range"))
    end

    write(p, [signiture])
    
    for field in fields
        pack(p, field)
    end
end

# Packer -------------------------- #

function pack_end_of_stream(p::Packer)
    write(p, [0xDF])
end

# Packer -------------------------- #

function write(p::Packer, b::AbstractArray{UInt8})
    Base.write(p.stream, b)
end

# Packer -------------------------- #
# # # # # # # # # # # # # # # # # # #