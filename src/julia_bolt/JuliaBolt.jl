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

module JuliaBolt

using Logging

using Sockets
using BufferedStreams

export bolt_connect, bolt_run, bolt_sync, bolt_begin, bolt_commit, bolt_rollback, bolt_close, bolt_pull_all, bolt_discard_all
export timedout, acquire_direct, release, in_use_connection_count, acquire, from_uri, DEFAULT_PORT
export Connection, ConnectionPool, ConnectionErrorHandler, TransientError, Structure

const version = v"0.1.0"

const DEFAULT_HOST = "localhost"
const DEFAULT_PORT = 7687
const MAGIC_PREAMBLE = 0x6060B017

const INFINITE = -1
const DEFAULT_MAX_CONNECTION_LIFETIME = 3600

const DEFAULT_USER_AGENT = "juliabolt/$(string(version)) Julia/$(string(VERSION)) ($(string(Sys.KERNEL)))"

include("io.jl")
include("addressing.jl")
include("strpack.jl")
include("errors.jl")

#####################################
#              Testing              #
#####################################

export FakeSocket, QuickConnection

mutable struct FakeSocket <: IO
    address
end

function getpeername(f::FakeSocket)
    return f.address
end

mutable struct QuickConnection
    socket
    address
    closed
    defunct
    in_use
    pool
    QuickConnection(socket; config...) = new(socket, getpeername(socket), false, false, false, nothing)
end

function reset(q::QuickConnection)
    return
end

function close(q::QuickConnection)
    Base.close(q.socket)
end

function timedout(q::QuickConnection)
    return false
end

# Testing ------------------------- #
# # # # # # # # # # # # # # # # # # #



#####################################
#            Server Info            #
#####################################

mutable struct ServerInfo
    address::Tuple{IPAddr, UInt16}
    protocol_version
    metadata::Dict{String, String}

    ServerInfo(address, protocol_version) = new(address, protocol_version, Dict())
end

# Server Info --------------------- #

function agent(s::ServerInfo)::Union{String, Nothing}
    if !haskey(s.metadata, "server")
        return nothing
    end
    return s.metadata["server"]
end 

# Server Info --------------------- #

function version_info(s::ServerInfo)::Union{Array{Integer}, Nothing}

    if agent(s) === nothing
        return nothing
    end
        
    _, value = split(agent(s), "/")
    value_split = split(replace(value, '-'=>'.'), '.')
    versions = Integer[]
    for (i, v) in enumerate(value)
        try
            push!(versions, parse(Int64,v))
        catch
            continue
        end
    end
    
    return versions
end

# Server Info --------------------- #

function supports(s::ServerInfo, feature::String)::Bool
    if agent(s) === nothing
        return false
    elseif !startswith(agent(s), "Neo4j/")
        return false
    elseif feature == "bytes"
        return version_info(s) >= [3,2]
    # elseif feature == "statement_reuse"
    #     return version_info(s) >= [3,2]
    elseif feature == "run_metadata"
        return version_info(s) >= [3]
    else 
        return false
    end
end

# Server Info --------------------- #
# # # # # # # # # # # # # # # # # # #

abstract type AbstractResponse end
abstract type AbstractConnectionPool end

#####################################
#             Connection            #
#####################################

mutable struct Connection
    protocol_version::Integer
    address::Tuple{IPAddr, UInt16}
    socket::IO
    server::ServerInfo
    input_stream::BufferedInputStream
    input_buffer::ChunkedInputBuffer
    output_buffer::ChunkedOutputBuffer
    packer::Packer
    unpacker::Unpacker
    responses::Array{AbstractResponse}
    user_agent::String
    auth_dict::Dict
    # last_run_statement::Union{String, Nothing}
    max_connection_lifetime::Integer
    creation_timestamp::Integer
    closed::Bool
    defunct::Bool
    in_use::Bool
    pool::Union{AbstractConnectionPool, Nothing}

    # Error #: Error class used for raising connection errors
    # error_handler

    function Connection(protocol_version::Integer, address::Tuple{IPAddr, UInt16}, sock::IO; config...)
        output_buffer = ChunkedOutputBuffer()
        
        # Determine the user agent and ensure it is a Unicode value
        if !haskey(config, :user_agent)
            user_agent = DEFAULT_USER_AGENT
        else
            user_agent = config[:user_agent]
        end
        
        # Determine auth details
        if !haskey(config, :auth)
            auth_dict = Dict()
        elseif isa(config[:auth], Tuple) && 2<=length(config[:auth])<=3
            auth_dict = Dict("scheme"=>"basic", "principal"=>config[:auth][1], "credentials"=>config[:auth][2])
        elseif isa(config[:auth], Dict)
            auth_dict = config[:auth]
        else
            auth_dict = Dict()
        end

        # # Check for missing password
        # if haskey(auth_dict, :credentials)
            
        # elseif auth_dict["credentials"] == nothing || isempty(auth_dict["credentials"])
        #     throw(ErrorException("Password cannot be empty."))
        # end

        # Pick up the server certificate, if any
        
        if !haskey(config, :max_connection_lifetime)
            max_connection_lifetime = DEFAULT_MAX_CONNECTION_LIFETIME
        else
            max_connection_lifetime = config[:max_connection_lifetime]
        end

        creation_timestamp = round(Int64, time() * 1000)
        
        return new(protocol_version, address, sock, ServerInfo(Sockets.getpeername(sock), protocol_version), 
                BufferedInputStream(sock), ChunkedInputBuffer(), output_buffer, Packer(output_buffer), Unpacker(), AbstractResponse[], user_agent, auth_dict,
                    max_connection_lifetime, creation_timestamp, false, false, false, nothing)
    end
end

# function is_secture(c::Connection)
# end

# function local_port(c::Connection)
# end

# Connection ---------------------- #

function init(c::Connection)
    append(c, 0x01, [c.user_agent, c.auth_dict], 
            Response(c, on_success=(r, m)->merge!(c.server.metadata, m), on_failure=on_init_failure))
    bolt_sync(c)
    c.packer.supports_bytes = supports(c.server, "bytes")
end

# Connection ---------------------- #

function hello(c::Connection)
    headers = Dict("user_agent"=>c.user_agent)
    merge!(headers, c.auth_dict)
    logged_headers = Dict(headers)
    if haskey(logged_headers, "credentials")
        logged_headers["credentials"] = "*******"
    end
    
    append(c, 0x01, [headers], Response(c, on_success=(r, m)->merge!(c.server.metadata, m), on_failure=on_init_failure))

    bolt_sync(c)
    c.packer.supports_bytes = supports(c.server, "bytes")
end

# Connection ---------------- #

function bolt_run(c::Connection, statement::String, parameters=nothing; bookmarks=nothing, metadata=nothing, timeout=nothing, handlers...)
    # if supports(c.server, "statement_reuse")
    #     if uppercase(statement) ∉ ("BEGIN", "COMMIT", "ROLLBACK")
    #         if statement == c.last_run_statement
    #             statement = ""
    #         else
    #             c.last_run_statement = statement
    #         end
    #     end
    # end

    if parameters === nothing
        parameters = Dict()
    end

    if c.protocol_version >= 3
        extra = Dict()
        if bookmarks !== nothing
            extra["bookmarks"] = bookmarks
        end
        if metadata !== nothing
            extra["tx_metadata"] = metadata
        end
        if timeout !== nothing
            extra["tx_timeout"] = Integer(floor(1000 * timeout))
        end
        fields = [statement, parameters, extra]
    else
        if metadata !== nothing
            throw(ErrorException("Transaction metadata is not supported in Bolt v$(c.protocol_version)"))
        end
        if timeout !== nothing
            throw(ErrorException("Transaction timeouts are not supported in Bolt v$(c.protocol_version)"))
        end
        field = [statement, parameters]
    end
    
    append(c, 0x10, fields, Response(c; handlers...))
end

# Connection ---------------------- #

function bolt_discard_all(c::Connection; handlers...)
    append(c, 0x2f, [], Response(c; handlers...))
end

# Connection ---------------------- #

function bolt_pull_all(c::Connection; handlers...)
    append(c, 0x3f, [], Response(c; handlers...))
end

# Connection ---------------------- #

function bolt_begin(c::Connection; timeout::Union{Number, Nothing}=nothing, bookmarks::Union{Vector, Nothing}=nothing, metadata::Union{Dict, Nothing}=nothing, handlers...)
    if c.protocol_version >= 3
        extra = Dict()
        if bookmarks !== nothing
            extra["bookmarks"] = bookmarks
        end
        if metadata !== nothing
            extra["tx_metadata"] = metadata
        end
        if timeout !== nothing
            extra["tx_timeout"] = Integer(floor(1000*timeout))
        end
        append(c, 0x11, [extra], Response(c; handlers...))
    else
        extra = Dict()
        if bookmarks !== nothing
            if c.protocol_version < 2
                extra["bookmark"] = last_bookmark(bookmarks)
            end
            extra["bookmarks"] = bookmarks
        end
        if metadata !== nothing
            throw(ErrorException("Transaction metadata is not supported in Bolt v$(c.protocol_version)"))
        end
        if timeout !== nothing
            throw(ErrorException("Transaction metadata is not supported in Bolt v$(c.protocol_version)"))
        end  
        run(c, "BEGIN", extra; handlers...)
        bolt_discard_all(c; handlers...)     
    end
end

# Connection ---------------------- #

function last_bookmark(b1, b2)
    n1 = tryparse(Integer, rsplit(b1, ":"; limit=2)[2])
    n2 = tryparse(Integer, rsplit(b2, ":"; limit=2)[2])
    return if (n1 > n2) b1 else b2 end
end

# Connection ---------------------- #

function last_bookmark(bookmarks)
    last = nothing
    for bookmark in bookmarks
        if last === nothing
            last = bookmark
        else
            last = last_bookmark(last, bookmark)
        end
    end
    return last
end

# Connection ---------------------- #

function bolt_commit(c::Connection; handlers...)
    if c.protocol_version >= 3
        append(c, 0x12, [], Response(c; handlers...))
    else
        run(c, "COMMIT", []; handlers...)
        bolt_discard_all(c; handlers...)
    end
end

# Connection ---------------------- #

function bolt_rollback(c::Connection; handlers...)
    if c.protocol_version >= 3
        append(c, 0x13, [], Response(c; handlers...))
    else
        run(c, "ROLLBACK", []; handlers...)
        bolt_discard_all(c; handlers...)
    end
end

# Connection ---------------------- #

function append(c::Connection, signiture::UInt8, fields=[], response::Union{AbstractResponse, Nothing}=nothing)
    pack_struct(c.packer, signiture, fields)
    chunk(c.output_buffer)
    chunk(c.output_buffer)
    if response !== nothing
        push!(c.responses, response) 
    end
end

# Connection ---------------------- #

function bolt_reset(c::Connection)
    append(c, 0x0f, [], Response(c))
    bolt_sync(c)
end

# Connection ---------------------- #

function send(c::Connection) 
    data = view(c.output_buffer)
    Base.write(c.socket, data)
    clear(c.output_buffer)
end

# Connection ---------------------- #

function fetch(c::Connection)
    if c.closed
        throw(ErrorException("Failed to read from closed connection."))
    elseif c.defunct
        throw(ErrorException("Failed to read from closed connection."))
    elseif c.responses === nothing || length(c.responses) == 0
        return 0, 0
    end

    receive(c)

    details, summary_signature, summary_metadata = unpack(c)

    if length(details) > 0
        handle_records(c.responses[1], details)
    end

    if summary_signature === nothing
        return length(details), 0
    end
    
    response = popfirst!(c.responses)

    response.complete = true

    if summary_signature == 0x70
        handle_success(response, if (summary_metadata !== nothing) summary_metadata else Dict() end)
    elseif summary_signature == 0x7E
        # c.last_run_statement = nothing
        handle_ignored(response, if (summary_metadata !== nothing) summary_metadata else Dict() end)
    elseif summary_signature == 0x7F
        # c.last_run_statement = nothing
        handle_failure(response, if (summary_metadata !== nothing) summary_metadata else Dict() end)
    else  
        # c.last_run_statement = nothing
        throw(ErrorException("Unexpected response message with signature $(summary_signature)"))
    end

    return length(details), 1
end

# Connection ---------------------- #

function receive(c::Connection)
    received = 0
    
    try
        received = receive_message(c.input_buffer, c.socket, 8192)
    catch
        received = 0
    end

    if received == -1
        throw(ErrorException("KeyboardInterrupt"))
    elseif received == 0
        c.defunct = true
        close(c)
        throw(ErrorException("Failed to read from defunct connection"))
    end
end

# Connection ---------------------- #

function unpack(c::Connection)
    unpacker = c.unpacker
    input_buffer = c.input_buffer

    details = []
    summary_signature = nothing
    summary_metadata = nothing
    more = true
    while more
        unpacker.source = input_buffer.frame
        size, signature = unpack_structure_header(unpacker)
        if size > 1
            throw(ErrorException("Expected one field"))
        elseif signature == 0x71
            data = unpack_list(unpacker)
            push!(details, data)
            more = frame_message(input_buffer)
        else
            summary_signature = signature  
            summary_metadata = unpack_map(unpacker)
            more = false
        end
    end
    return details, summary_signature, summary_metadata
end

# Connection ---------------------- #

function timedout(c::Connection)
    return 0 <= c.max_connection_lifetime <= round(Int64, time() * 1000) - c.creation_timestamp
end

# Connection ---------------------- #

function bolt_sync(c::Connection)
    send(c)
    detail_count = summary_count = 0
    while !isempty(c.responses)
        response = c.responses[1]
        while !response.complete
            detail_delta, summary_delta = fetch(c)
            detail_count += detail_delta
            summary_count += summary_delta
        end
    end
    return detail_count, summary_count
end

# Connection ---------------------- #

function bolt_close(c::Connection)
    if !c.closed
        if c.protocol_version >= 3
            append(c, 0x02, [])
            try 
                send(c)
            catch
            end
        end
        try
            Base.close(c.socket)
        catch
        finally
            c.closed = true
        end
    end
end

# Connection ---------------------- #
# # # # # # # # # # # # # # # # # # #





#####################################
#             Response              #
#####################################

mutable struct Response <: AbstractResponse
    connection::Connection
    on_success::Union{Function, Nothing}
    on_failure::Union{Function, Nothing}
    on_records::Union{Function, Nothing}
    on_ignored::Union{Function, Nothing}
    on_summary::Union{Function, Nothing}
    complete::Bool

    function Response(connection::Connection; handlers...) 
       on_success = nothing
       on_failure = nothing
       on_records = nothing
       on_ignored = nothing
       on_summary = nothing
       for key in keys(handlers)
            if key == :on_success
                on_success = handlers[:on_success]
            elseif key == :on_failure
                on_failure = handlers[:on_failure]
            elseif key == :on_records
                on_records = handlers[:on_records]
            elseif key == :on_ignored
                on_ignored = handlers[:on_ignored]
            elseif key == :on_summary
                on_summary = handlers[:on_summary]
            end
        end
        new(connection, on_success, on_failure, on_records, on_ignored, on_summary, false)
    end
end

# Response ------------------------ #

function handle_success(response::Response, metadata::Dict)
    if response.on_success !== nothing
        response.on_success(response, metadata)
    end 
    if response.on_summary !== nothing
        response.on_summary(response, metadata)
    end
end

# Response ------------------------ #

function handle_failure(response::Response, metadata::Dict)
    bolt_reset(response.connection)
    if response.on_failure !== nothing
        response.on_failure(response, metadata)
    end 
    if response.on_summary !== nothing
        response.on_summary(response, metadata)
    end 
    throw(hydrate_cypher_error(metadata))
end

# Response ------------------------ #

function handle_records(response::Response, records::Array)
    if response.on_records !== nothing
        response.on_records(response, records)
    end 
end

# Response ------------------------ #

function handle_ignored(response::Response, metadata::Union{Dict, Nothing}=nothing)
    if response.on_ignored !== nothing
        response.on_ignored(response, metadata)
    end 
    if response.on_summary !== nothing
        response.on_summary(response, metadata)
    end
end

# Response ------------------------ #

function on_init_failure(response::Response, metadata::Dict)

    code = metadata["code"]
    message = metadata["message"]

    if code == "Neo.ClientError.Security.Unauthorized"
        throw(AuthError())
    else
        throw(ServiceUnavailable())
    end
end

# Response ------------------------ #
# # # # # # # # # # # # # # # # # # #


#####################################
#          Connection Pool          #
#####################################

mutable struct ConnectionErrorHandler
    handlers::Dict{DataType, Function}
    ConnectionErrorHandler() = new(Dict{DataType, Function}())
    ConnectionErrorHandler(handlers::Dict{DataType, Function}) = new(handlers)
end

function handle(c::ConnectionErrorHandler, error, address)
    try
        error_type = typeof(error)
        if haskey(c.handlers, error_type)
            handler = c.handlers[error_type]
            handler(address)
        end
    catch
    end
end

const DEFAULT_MAX_CONNECTION_POOL_SIZE = 100
const DEFAULT_CONNECTION_ACQUISITION_TIMEOUT = 60

mutable struct ConnectionPool <: AbstractConnectionPool
    connector::Function
    address::Address
    connection_error_handler
    max_connection_pool_size
    connection_acquisition_timeout
    connections::Dict
    lock::ReentrantLock
    cond::Condition
    closed::Bool

    function ConnectionPool(connector::Function, address; config...)
        connection_error_handler = ConnectionErrorHandler()
        max_connection_pool_size = if haskey(config, :max_connection_pool_size) 
                                        config[:max_connection_pool_size] 
                                        else DEFAULT_MAX_CONNECTION_POOL_SIZE end
        connection_acquisition_timeout = if haskey(config, :connection_acquisition_timeout) 
                                                config[:connection_acquisition_timeout] 
                                                else DEFAULT_CONNECTION_ACQUISITION_TIMEOUT end
       new(connector, address, connection_error_handler, max_connection_pool_size, connection_acquisition_timeout, Dict(), ReentrantLock(), Condition(), false)
    end
end


# Connection Pool ----------------- #

function acquire_direct(c::ConnectionPool, address)
    if c.closed 
        throw(ErrorException("Connection pool closed"))
    end
    
    try
        lock(c.lock)
        if haskey(c.connections, address)
            connections = c.connections[address]
        else
            connections = c.connections[address] = []
        end
        
        connection_acquisition_start_timestamp = round(Int64, time() * 1000)
        while true
            i = 1
            while i <= length(connections)
                connection = connections[i]
                if connection.closed || connection.defunct || timedout(connection)
                    splice!(connections,i)
        else
                    i += 1
                end
                if !connection.in_use
                    connection.in_use = true
                    return connection
                end
            end
            can_create_new_connection = c.max_connection_pool_size == INFINITE || length(connections) < c.max_connection_pool_size        
            if can_create_new_connection
                
                connection = nothing
                
                try
                    connection = c.connector(address, error_handler=c.connection_error_handler)
                catch e
                    remove(c, address)
                    rethrow(e)
                end
                
                connection.pool = c
                connection.in_use = true
                push!(connections, connection)
                return connection
            end
            
            span_timeout = c.connection_acquisition_timeout - (round(Int64, time() * 1000) - connection_acquisition_start_timestamp)
            if span_timeout > 0
                wait(c.cond)
                if c.connection_acquisition_timeout <= (round(Int64, time() * 1000) - connection_acquisition_start_timestamp)
                    throw(ErrorException("Failed to obtain a connection from pool within $(c.connection_acquisition_timeout)s"))
                end
            else
                throw(ErrorException("Failed to obtain a connection from pool within $(c.connection_acquisition_timeout)s"))
            end
        end
    finally
        try unlock(c.lock) catch; println("EXCEPTION unlock(c.lock)") end
    end
end


# Connection Pool ----------------- #


acquire(c::ConnectionPool, access_mode=nothing) = acquire_direct(c, c.address)


# Connection Pool ----------------- #

function release(c::ConnectionPool, connection)
    try
        lock(c.lock)
        connection.in_use = false
        notify(c.cond)
    finally
        try unlock(c.lock) catch; println("EXCEPTION unlock(c.lock)") end
    end
end


# Connection Pool ----------------- #

function in_use_connection_count(c::ConnectionPool, address)
    if !haskey(c.connections, address)
        return 0
    end
    
    connections = c.connections[address]
    return sum([if connection.in_use 1 else 0 end for connection in connections])
end


# Connection Pool ----------------- #

function deactivate(c::ConnectionPool, address)
    try
        lock(c.lock)
        
        if !haskey(c.connections, address)
            return
        end

        connections = c.connections[address]
    
        i = 1
        while i <= length(connections)
            connection = connections[i]
            if !connection.in_use
                splice!(connections,i)
                try close(connection) catch end
            else
                i += 1
            end
        end

        if length(connections) == 0
            remove(c, address)
        end

    finally
        try unlock(c.lock) catch; println("EXCEPTION unlock(c.lock)") end
    end
end


# Connection Pool ----------------- #

function remove(c::ConnectionPool, address)
    try
        lock(c.lock)
        if haskey(c.connections, address)
            connections = c.connections[address]
            delete!(c.connections, address)

            for connection in connections
                try close(connection) catch end
            end
        end
        
    finally
        try unlock(c.lock) catch; println("EXCEPTION unlock(c.lock)") end
    end
end


# Connection Pool ----------------- #

function close(c::ConnectionPool)
    if c.closed
        return
    end

    try
        lock(c.lock)
        if !c.closed
            c.closed = true
            for address in keys(c.connections)
                remove(c, address)
            end 
        end
    finally
        try unlock(c.lock) catch; println("EXCEPTION unlock(c.lock)") end
    end
end


# Connection Pool ----------------- #

function is_closed(c::ConnectionPool)
    try
        lock(c.lock)
        return c.closed
    finally
        try unlock(c.lock) catch; println("EXCEPTION unlock(c.lock)") end
    end
end

# Connection Pool ----------------- #
# # # # # # # # # # # # # # # # # # #


#####################################
#     JuliaBolt Module Functions    #
#####################################

function handshake(socket::IO; config...)
    address = (local_ip, local_port) = Sockets.getsockname(socket)

    
    # Send details of the protocol versions supported
    supported_versions = UInt32[3, 2, 1, 0]

    handshake = [MAGIC_PREAMBLE; supported_versions]
    handshake .= hton.(handshake)

    Base.write(socket, handshake)

    agreed_version = ntoh(Base.read(socket, UInt32))
    
    if agreed_version in [1,2]
        connection = Connection(agreed_version, address, socket; config...)
        init(connection)
        return connection
    elseif agreed_version in [3]
        connection = Connection(agreed_version, address, socket; config...)
        hello(connection)
        return connection
    elseif agreed_version == 0x48545450
        Base.close(socket)
        throw(ErrorException("Cannot to connect to Bolt service (looks like HTTP)"))
    else
        Base.close(socket)
        throw(ErrorException("Unknown or Unsupported Bolt protocol version $(agreed_version)"))
    end
end

# JuliaBolt Module Functions -------- #

function bolt_connect(address; config...)
    try
        socket = Sockets.connect(address.host, address.port)
        connection = handshake(socket; config...)
        return connection
    catch ex
        rethrow(ex)
    end
end

# JuliaBolt Module Functions -------- #
# # # # # # # # # # # # # # # # # # #

end # Module