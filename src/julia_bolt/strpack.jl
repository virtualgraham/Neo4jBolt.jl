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
#             Structure             #
#####################################

mutable struct Structure
    tag::UInt8
    fields::Array{Any}

    Structure(tag, fields) = new(tag, fields)
    Structure(tag) = new(tag, [])
end

function Base.getindex(structure::Structure, i::Int)
    return Base.getindex(structure.fields, i)
end

function Base.setindex(structure::Structure, v, i::Int)
    return Base.setindex(structure.fields, v, i)
end

function Base.firstindex(structure::Structure)
    return Base.firstindex(structure.fields)
end

function Base.lastindex(structure::Structure)
    return Base.lastindex(structure.fields)
end
 
# Structure ----------------------- #

Base.:(==)(x::Structure, y::Structure) = x.tag == y.tag && x.fields == y.fields

# Structure ----------------------- #
# # # # # # # # # # # # # # # # # # #


#####################################
#              strpack              #
#####################################

function struct_pack(value::UInt8)
    return [hton(value)]
end

# strpack ------------------------- #

function struct_pack(value::Union{UInt16, UInt32, Int16, Int32, Int64, Float64})
    return copy(reinterpret(UInt8, [hton(value)]))
end

# strpack ------------------------- #

function struct_unpack(t::DataType, v::AbstractArray{UInt8})
    _value = reinterpret(t, v)
    return ntoh(_value[1])
end

# strpack ------------------------- #
# # # # # # # # # # # # # # # # # # #


include("packer.jl")
include("unpacker.jl")