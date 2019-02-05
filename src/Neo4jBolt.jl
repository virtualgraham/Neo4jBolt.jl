module Neo4jBolt

using OrderedCollections


include("../lib/JuliaBolt/src/JuliaBolt.jl")

using .JuliaBolt

export fix_parameters, record_data, record_values, record_items, index, value, consume, last_bookmark,
    custom_auth, basic_auth, kerberos_auth, session, single, begin_transaction, run_transaction, commit, 
    rollback, write_transaction, read_transaction, summary, start_node, end_node, READ_ACCESS, WRITE_ACCESS
export Statement, Graph, Node, Relationship, PackStreamHydrator, Driver, UnitOfWork, Path, CartesianPoint,
    WGS84Point, DateWrapper, TimeWrapper, DateTimeWrapper, DurationWrapper

include("errors.jl")
include("config.jl")

const READ_ACCESS = "READ"
const WRITE_ACCESS = "WRITE"

const INITIAL_RETRY_DELAY = 1.0
const RETRY_DELAY_MULTIPLIER = 2.0
const RETRY_DELAY_JITTER_FACTOR = 0.2

const STATEMENT_TYPE_READ_ONLY = "r"
const STATEMENT_TYPE_READ_WRITE = "rw"
const STATEMENT_TYPE_WRITE_ONLY = "w"
const STATEMENT_TYPE_SCHEMA_WRITE = "s"

function custom_auth(scheme, principal, credentials, realm=nothing; parameters...)
    d = Dict{String, Any}(
        "scheme" => scheme,
        "principal" => principal,
        "credentials" => credentials,
    )
    if realm != nothing
        d["realm"] = realm
    end
    p = Dict()
    for (k,v) in parameters
        p[String(k)] = v
    end
    if length(p) > 0
        d["parameters"] = p
    end
    return d
end


function basic_auth(user, password, realm=nothing)
    return custom_auth("basic", user, password, realm)
end


function kerberos_auth(base64_encoded_ticket)
    return custom_auth("kerberos", "", base64_encoded_ticket)
end


#####################################
#               Driver              #
#####################################
using Sockets

mutable struct Driver
    uri_sceme::String
    pool::ConnectionPool
    closed::Bool
    address
    max_retry_time    
    security_plan
    encrypted

    function Driver(uri; config...)

        address = from_uri(uri, JuliaBolt.DEFAULT_PORT)

        if address.scheme != "bolt"
            throw(ErrorException("Neo4jBolt driver requires bolt URI scheme, $(address.scheme) given"))
        end
        
        if haskey(config, "encrypted")
            #
        end
        
        function connector(address; kwargs...)
            return bolt_connect(address; (config..., kwargs...)...)
        end

        pool = ConnectionPool(connector, address; config...)
        
        # acquire and release a connection
        release(pool, acquire(pool))

        if haskey(config, "max_retry_time")
            max_retry_time = config["max_retry_time"]
        else
            max_retry_time = default_config["max_retry_time"]
        end
        
        return new("bolt", pool, false, address, max_retry_time, nothing, false)
        
    end
end

# Driver -------------------------- #

function Base.close(driver::Driver)
    if !driver.closed
        driver.closed = true
        if driver.pool != nothing
            JuliaBolt.close(driver.pool)
        end
    end
end

# Driver -------------------------- #

function session(driver::Driver, access_mode=nothing; parameters...)
    aquierer = function(access_mode)
        return JuliaBolt.acquire(driver.pool, access_mode)
    end
    if !in(:max_retry_time, Base.keys(parameters))
        return Session(aquierer, access_mode; max_retry_time=driver.max_retry_time, parameters...)
    else
        return Session((access_mode)->JuliaBolt.acquire(driver.pool, access_mode), access_mode; parameters...)
    end
end


function session(f::Function, args...; kwargs...)
    sess = session(args...; kwargs...)
    try
        f(sess)
    finally
        close(sess)
    end
end


# Driver -------------------------- #
# # # # # # # # # # # # # # # # # # #



#####################################
#             Statement             #
#####################################

mutable struct Statement
    text::String
    metadata::Union{Dict, Nothing}
    timeout::Union{Number, Nothing}
    
    function Statement(text; metadata::Union{Dict, Nothing}=nothing, timeout::Union{Number, Nothing}=nothing)
        return new(text, metadata, timeout)
    end
end

# Statement ----------------------- #
# # # # # # # # # # # # # # # # # # #



#####################################
#              Session              #
#####################################

mutable struct Session
    acquirer
    default_access_mode
    bookmarks_in
    bookmark_out
    max_retry_time
    closed
    connection
    connection_access_mode
    transaction
    last_result
    
    function Session(acquirer, access_mode; parameters...)
        bookmarks_in = nothing
        if haskey(parameters, :bookmark)
            bookmarks_in = [parameters[:bookmark]]
        end
        if haskey(parameters, :bookmarks)
            bookmarks_in = parameters[:bookmarks]
        end
        if haskey(parameters, :max_retry_time)
            max_retry_time = parameters[:max_retry_time]
        end    
        return new(acquirer, access_mode, bookmarks_in, nothing, max_retry_time, false, nothing, nothing, nothing, nothing)
    end
end

# Session ------------------------- #

function connect(session::Session, access_mode=nothing)
    if access_mode == nothing
        access_mode = session.default_access_mode
    end
    if session.connection != nothing
        if access_mode == session.connection_access_mode
            return
        end
        disconnect(session, true)
    end
    session.connection = session.acquirer(access_mode)
    session.connection_access_mode = access_mode
    
end

# Session ------------------------- #

function disconnect(session::Session, do_sync::Bool)
    if session.connection != nothing
        if do_sync
            try
                bolt_sync(session.connection)
            catch ex
                println("disconnect sync error")
            end
        end
        if session.connection != nothing
            session.connection.in_use = false
            session.connection = nothing
        end
        session.connection_access_mode = nothing
    end
end

# Session ------------------------- #

function Base.close(session::Session)
    try
        if has_transaction(session)
            try
                rollback_transaction(session)
            catch e
                println("error close rollback_tranaction")
            end
        end
    finally
        session.closed = true
    end
    disconnect(session, true)
end

# Session ------------------------- #

function Base.run(session::Session, statement::Statement, parameters::Dict=Dict(); use_julia_dates=true, kwparameters...)
    assert_open(session)
    if length(strip(statement.text)) == 0
        throw(ErrorException("Cannot run an empty statement"))
    end
    if session.connection == nothing
        connect(session)
    end
    cx = session.connection
    protocol_version = cx.protocol_version
    server = cx.server

    has_trans = has_transaction(session)

    statement_text = statement.text
    statement_metadata = statement.metadata
    statement_timeout = statement.timeout

    for (k,v) in kwparameters
        parameters[String(k)] = v
    end
    parameters = fix_parameters(parameters, protocol_version, supports_bytes=JuliaBolt.supports(server, "bytes"))

    fail = function(_,_)
        close_transaction(session)
    end

    hydrant = PackStreamHydrator(protocol_version, use_julia_dates)
    result_metadata = Dict(
        "statement"=> statement_text,
        "parameters"=> parameters,
        "server"=> server,
        "protocol_version"=> protocol_version
    )
    run_metadata = Dict(
        :statement=> statement_metadata,
        :timeout=> statement_timeout,
        :on_success=> (_, r)->merge!(result_metadata, r),
        :on_failure=> fail
    )

    function done(_, summary_metadata)
        merge!(result_metadata, summary_metadata)
        bookmark = if haskey(result_metadata, "bookmark") result_metadata["bookmark"] else nothing end
        if bookmark != nothing
            session.bookmarks_in = [bookmark]
            session.bookmark_out = bookmark
        end
    end
    
    session.last_result = result = StatementResult(session, hydrant, result_metadata)

    if has_trans
        if statement_metadata != nothing
            throw(ErrorException("Metadata can only be attached at transaction level"))
        end
        if statement_timeout != nothing
            throw(ErrorException("Timeouts only apply at transaction level"))
        end
    else
        run_metadata[:bookmarks] = session.bookmarks_in
    end
    
    bolt_run(cx, statement_text, parameters; run_metadata...)
    bolt_pull_all(
        cx, 
        on_records = function(_, r)
            append!(result.records, hydrate_records(hydrant, keys(result), r))
        end,
        on_success=done,
        on_failure=fail,
        on_summary=function(_, r)
            detach(result, false)   
        end          
    )

    if !has_trans
        try 
            JuliaBolt.send(session.connection)
            JuliaBolt.fetch(session.connection)
        catch ex
            if isa(ex, JuliaBolt.ConnectionExpired)
                throw(ErrorException(error))
            else
                rethrow()
            end
        end
    end

    return result
end


Base.run(session::Session, statement::String, parameters::Dict=Dict(); kwparameters...) = 
    Base.run(session, Statement(statement), parameters; kwparameters...)
    
    
Base.run(session::Session, statement::AbstractArray{UInt8}, parameters::Dict=Dict(); kwparameters...) = 
    Base.run(session, Statement(String(statement)), parameters; kwparameters...)

    
# Session ------------------------- #

function send(session::Session)
    if session.connection != nothing
        try 
            JuliaBolt.send(session.connection)
        catch ex
            if isa(ex, JuliaBolt.ConnectionExpired)
                throw(ErrorException(error))
            else
                rethrow()
            end
        end
    end
end

# Session ------------------------- #

function fetch(session::Session)
    if session.connection != nothing
        try 
            detail_count, _ = JuliaBolt.fetch(session.connection)
            return detail_count
        catch ex
            if isa(ex, JuliaBolt.ConnectionExpired)
                throw(ErrorException(error))
            else
                rethrow()
            end
        end
    else 
        return 0
    end
end

# Session ------------------------- #

function sync(session::Session)
    if session.connection != nothing
        try 
            detail_count, _ = bolt_sync(session.connection)
            return detail_count
        catch ex
            if isa(ex, JuliaBolt.ConnectionExpired)
                throw(ErrorException(error))
            else
                rethrow()
            end
        end
    else 
        return 0
    end
end

# Session ------------------------- #

function detach(session::Session, result, do_sync=true)
    count = 0

    if do_sync && attached(result)
        send(session)
        while attached(result)
            count += fetch(session)
        end
    end

    if session.last_result === result
        session.last_result = nothing
        if !has_transaction(session)
            disconnect(session, false)
        end
    end

    result.session = nothing
    return count
end

# Session ------------------------- #

function next_bookmarks(session::Session)
    return session.bookmarks_in
end

# Session ------------------------- #

function last_bookmark(session::Session)
    return session.bookmark_out
end

# Session ------------------------- #

function has_transaction(session::Session)
    return session.transaction != nothing
end

# Session ------------------------- #

function close_transaction(session::Session)
    session.transaction = nothing
end

# Session ------------------------- #

function begin_transaction(session::Session; bookmark=nothing, metadata=nothing, timeout=nothing)
    assert_open(session)
    if has_transaction(session)
        throw(ErrorException("Explicit transaction already open"))
    end

    if bookmark != nothing
        throw(ErrorException("Passing bookmarks at transaction level is deprecated"))
    end

    open_transaction(session, metadata=metadata, timeout=timeout)
    return session.transaction
end

function begin_transaction(f::Function, args...)
    tx = begin_transaction(args...)
    ex_thrown = false
    try
        f(tx)
    catch ex
        ex_thrown = true
        rethrow()
    finally
        
        if tx.closed
            return
        elseif tx.success == nothing 
            tx.success = !ex_thrown
        end
        close(tx)
    end
end

# Session ------------------------- #

function open_transaction(session::Session; access_mode=nothing, metadata=nothing, timeout=nothing)
    session.transaction = Transaction(session, on_close=()->close_transaction(session))
    connect(session, access_mode)
    bolt_begin(session.connection, bookmarks=session.bookmarks_in, metadata=metadata, timeout=timeout)
end

# Session ------------------------- #

function commit_transaction(session::Session)
    assert_open(session)
    if session.transaction == nothing
        throw(ErrorException("No transaction to commit"))
    end
    metadata = Dict()
    try
        bolt_commit(session.connection, on_success=(_, r)->merge!(metadata, r))
    finally
        disconnect(session, true)
        session.transaction = nothing
    end
    bookmark = metadata["bookmark"]
    session.bookmarks_in = [bookmark]
    session.bookmark_out = bookmark
    return bookmark
end

# Session ------------------------- #

function rollback_transaction(session::Session)
    assert_open(session)
    if session.transaction == nothing
        throw(ErrorException("No transaction to rollback"))
    end
    cx = session.connection
    if cx != nothing
        metadata = Dict()
        try
            bolt_rollback(cx, on_success=(_, r)->merge!(metadata, r))
        finally
            disconnect(session, true)
            session.transaction = nothing
        end
    end
end

# Session ------------------------- #


function retry_delay_generator(last_delay)
    delay = if last_delay == 0 INITIAL_RETRY_DELAY else last_delay * RETRY_DELAY_MULTIPLIER end
    jitter = RETRY_DELAY_JITTER_FACTOR * delay
    return delay - jitter + (2 * jitter * rand())
end


function is_retriable_transient_error(error)
    return in(error.code, ("Neo.TransientError.Transaction.Terminated", "Neo.TransientError.Transaction.LockClientStopped"))
end


# function kwargs_to_dict(kwargs)
#     d = Dict()
#     for (k,v) in kwargs
#         d[String(k)] = v
#     end
#     return d
# end

struct UnitOfWork
    f::Function
    metadata::Union{Dict, Nothing}
    timeout::Union{Number, Nothing}
end


function run_transaction(session::Session, access_mode::String, f::Function, args...; kwargs...)
    return run_transaction(session, access_mode, UnitOfWork(f, nothing, nothing), args...; kwargs...)
end


function run_transaction(session::Session, access_mode::String, unit_of_work::UnitOfWork, args...; kwargs...)
    retry_delay = 0
    errors = Exception[]
    t0 = time()
   
    f = unit_of_work.f
    metadata = unit_of_work.metadata
    timeout = unit_of_work.timeout

    while true
        try
            open_transaction(session, access_mode=access_mode, metadata=metadata, timeout=timeout)
            tx = session.transaction
            try
                result = f(tx, args, Dict(kwargs))
                if tx.success == nothing
                    tx.success = true
                end

                return result
            catch
                if tx.success == nothing
                    tx.success = false
                end
                rethrow()
            finally
                close(tx)
            end
        catch ex
            if isa(ex, JuliaBolt.ServiceUnavailable) || isa(ex, SessionExpired) || isa(ex, JuliaBolt.ConnectionExpired)
                print("tx error")
                push!(errors, ex)
            elseif isa(ex, TransientError)
                if is_retriable_transient_error(error)
                    print("tx is_retriable_transient_error error")
                    push!(errors, ex)
                else
                    rethrow()
                end
            else
                rethrow()
            end
        end

        # an error occored, it was appended to errors, and the tx should be retried
        t1 = time()
        if t1 - t0 > session.max_retry_time
            break
        end   
        retry_delay = retry_delay_generator(retry_delay)
        sleep(retry_delay)
    end

    # tx was retried and failed the maximum number of times
    if length(errors) > 0
        throw(errors[length(errors)])
    else
        throw(ErrorException("Transaction failed"))
    end
end



# Session ------------------------- #

function read_transaction(session::Session, unit_of_work, args...; kwargs...)
    assert_open(session)
    return run_transaction(session, READ_ACCESS, unit_of_work, args...; kwargs...)
end

# Session ------------------------- #

function write_transaction(session::Session, unit_of_work, args...; kwargs...)
    assert_open(session)
    return run_transaction(session, WRITE_ACCESS, unit_of_work, args...; kwargs...)
end

# Session ------------------------- #

function assert_open(session::Session)
    if session.closed
        throw(ErrorException("Session Closed"))
    end
end

# Session ------------------------- #
# # # # # # # # # # # # # # # # # # #



#####################################
#            Transaction            #
#####################################

mutable struct Transaction
    session
    on_close
    success::Union{Bool, Nothing}
    closed::Bool
    
    function Transaction(session; on_close=nothing)
        return new(session, on_close, nothing, false)
    end
end

# Transaction --------------------- #

function Base.run(transaction::Transaction, statement, parameters=Dict(); kwparameters...)
    assert_open(transaction)
    return Base.run(transaction.session, statement, parameters; kwparameters...)
end

# Transaction --------------------- #

function sync(transaction::Transaction)
    assert_open(transaction)
    sync(transaction.session)
end

# Transaction --------------------- #

function commit(transaction::Transaction)
    transaction.success = true
    close(transaction)
end

# Transaction --------------------- #

function rollback(transaction::Transaction)
    transaction.success = false
    close(transaction)
end

# Transaction --------------------- #

function Base.close(transaction::Transaction)
    assert_open(transaction)
    try
        sync(transaction)
    catch ex
        if isa(ex, JuliaBolt.CypherError)
            transaction.success = false
        else 
            rethrow()
        end
    finally
        if has_transaction(transaction.session)
            if transaction.success == true
                commit_transaction(transaction.session)
            else
                rollback_transaction(transaction.session)
            end
        end
        transaction.closed = true
        transaction.on_close()
    end
end

# Transaction --------------------- #

function assert_open(transaction::Transaction)
    if transaction.closed
        throw(ErrorException("Transaction closed"))
    end
end

# Transaction --------------------- #
# # # # # # # # # # # # # # # # # # #

include("types/packstream.jl")





function fix_parameters(parameters, protocol_version; kwargs...)
    if parameters == nothing
        return Dict()
    end

    dehydrator = PackStreamDehydrator(protocol_version; kwargs...) 

    try
        return dehydrate(dehydrator, [parameters])[1]
    catch ex
        if isa(ex, TypeError)
            value = error.args[1]
            throw(TypeError("Parameters of type $(typeof(value)) are not supported"))
        else
            rethrow()
        end
    end
end




#####################################
#          Statement Result         #
#####################################

mutable struct StatementResult
    session
    hydrant
    metadata
    records
    summary

    StatementResult(session, hydrant, metadata) = new(session, hydrant, metadata, [], nothing)
end

# Statement Result ---------------- #

function attached(sr::StatementResult)
    return sr.session != nothing && !sr.session.closed
end

# Statement Result ---------------- #

function detach(sr::StatementResult, do_sync=true)
    if attached(sr)
        return detach(sr.session, sr, do_sync)
    else
        return 0
    end
end

# Statement Result ---------------- #

function keys(sr::StatementResult)
    if sr.metadata != nothing && haskey(sr.metadata, "fields")
        return sr.metadata["fields"]
    else
        if attached(sr)
            send(sr.session)
        end
        while attached(sr) && !haskey(sr.metadata, "fields")
            fetch(sr.session)
        end
        return sr.metadata["fields"]
    end
end

# Statement Result ---------------- #

function Base.iterate(sr::StatementResult, sent=false)
    while length(sr.records) == 0 && attached(sr)
        if !sent
            send(sr.session)
            sent = true
        end
        fetch(sr.session)
    end 

    if length(sr.records) > 0
        return (popfirst!(sr.records), sent)
    else 
        return nothing
    end
end

# Statement Result ---------------- #

function Base.IteratorSize(sr::StatementResult)
    return Base.SizeUnknown()
end

# Statement Result ---------------- #

function Base.IteratorEltype(sr::StatementResult)
    return Base.HasEltype()
end

# Statement Result ---------------- #

function Base.eltype(sr::StatementResult)
    return OrderedDict{String, Any}
end

# Statement Result ---------------- #

function summary(sr::StatementResult)
    detach(sr)
    if sr.summary == nothing
        sr.summary = BoltStatementResultSummary(sr.metadata)
    end
    return sr.summary
end

# Statement Result ---------------- #

function consume(sr::StatementResult)
    if attached(sr)
        for _ in sr
            # pass
        end
    end
    return summary(sr)
end

# Statement Result ---------------- #

function single(sr::StatementResult)
    records = [record for record in sr]
    size = length(records)
    if size == 0
        return nothing
    end
    return records[1]
end

# Statement Result ---------------- #

function peek(sr::StatementResult)
    if length(sr.records) > 0
        return sr.records[1]
    elseif !attached(sr)
        return nothing
    else 
        send(sr.session)
    end

    while attached(sr)
        fetch(sr.session)
        if length(sr.records) > 0
            return sr.records[1]
        end
    end
    
    return nothing
end

# Statement Result ---------------- #

function graph(sr::StatementResult)
    detach(sr)
    return sr.hydrant.graph
end

# Bolt Statement Result ----------- #

function value(sr::StatementResult, item=0, default=nothing)
    return [value(record, item, default) for record in sr]
end

# Bolt Statement Result ----------- #

function values(sr::StatementResult, items)
    return [values(record, items) for record in sr]
end

# Bolt Statement Result ----------- #

function data(sr::StatementResult, items)
    return [data(record, items) for record in sr]
end

# Bolt Statement Result ----------- #
# # # # # # # # # # # # # # # # # # #



#####################################
#   Bolt Statement Result Summary   #
#####################################

mutable struct BoltStatementResultSummary
    metadata
    protocol_version
    server
    statement
    parameters
    statement_type
    counters
    result_available_after
    result_consumed_after
    t_first
    t_last
    plan
    profile
    notifications

    function BoltStatementResultSummary(metadata::Dict)
        protocol_version = if haskey(metadata, "protocol_version") metadata["protocol_version"] else nothing end
        server = if haskey(metadata, "server") metadata["server"] else nothing end
        statement = if haskey(metadata, "statement") metadata["statement"] else nothing end
        parameters = if haskey(metadata, "parameters") metadata["parameters"] else nothing end
        statement_type = if haskey(metadata, "type") metadata["type"] else nothing end
        counters = SummaryCounters(if haskey(metadata, "stats") metadata["stats"] else Dict() end)
        result_available_after = if haskey(metadata, "result_available_after") metadata["result_available_after"] else nothing end
        result_consumed_after = if haskey(metadata, "result_consumed_after") metadata["result_consumed_after"] else nothing end
        t_first = if haskey(metadata, "t_first") metadata["t_first"] else nothing end
        t_last = if haskey(metadata, "t_last") metadata["t_last"] else nothing end

        plan = nothing
        profile = nothing
        if haskey(metadata, "plan") 
            plan = make_plan(metadata["plan"] ) 
        end
        if haskey(metadata, "profile") 
            profile = make_plan(metadata["profile"])
            plan = profile
        end
        notifications = []
        if haskey(metadata, "notifications")
            for notification in metadata["notifications"]
                p = nothing
                if haskey(notification, "position")
                    position = notification["position"]
                    if position != nothing
                        p = Position(
                            if haskey(position, "offset") position["offset"] else nothing end,
                            if haskey(position, "line") position["line"] else nothing end,
                            if haskey(position, "column") position["column"] else nothing end
                        )
                    end
                end
                n = Notification(
                    if haskey(notification, "code") notification["code"] else nothing end,
                    if haskey(notification, "title") notification["title"] else nothing end,
                    if haskey(notification, "description") notification["description"] else nothing end,
                    if haskey(notification, "severity") notification["severity"] else nothing end,
                    p
                )
                push!(notifications, n)
            end
        end

        return new(metadata, protocol_version, server, statement, parameters, statement_type, counters,
            result_available_after, result_consumed_after, t_first, t_last, plan, profile, notifications)
    end
end


struct Plan
    operator_type
    identifiers
    arguments
    children
end

# Struct Types -------------------- #

struct ProfiledPlan
    operator_type
    identifiers
    arguments
    children
    db_hits
    rows
end

# Struct Types -------------------- #

struct Notification
    code
    title
    description
    severity
    position
end

# Struct Types -------------------- #

struct Position
    offset
    line
    column
end

function make_plan(plan_dict::Dict)
    operator_type = if haskey(plan_dict, "operatorType") plan_dict["operatorType"] else nothing end
    identifiers = if haskey(plan_dict, "identifiers") plan_dict["identifiers"] else [] end
    arguments = if haskey(plan_dict, "arguments") plan_dict["arguments"] else [] end

    children = if haskey(plan_dict, "children") Vector([make_plan(child) for child in plan_dict["children"]]) else [] end
    
    if haskey(plan_dict, "dbHits") || haskey(plan_dict, "rows")
        db_hits = if haskey(plan_dict, "db_hits") plan_dict["db_hits"] else 0 end
        rows = if haskey(plan_dict, "rows") plan_dict["rows"] else 0 end
        return ProfiledPlan(operator_type, identifiers, arguments, children, db_hits, rows)
    else
        return Plan(operator_type, identifiers, arguments, children)
    end
end

# Bolt Statement Result Summary --- #
# # # # # # # # # # # # # # # # # # #




#####################################
#          Summary Counters         #
#####################################

mutable struct SummaryCounters
    nodes_created
    nodes_deleted
    relationships_created
    relationships_deleted
    properties_set
    labels_added
    labels_removed
    indexes_added
    indexes_removed
    constraints_added
    constraints_removed

    function SummaryCounters(stats::Dict)
        nodes_created = if haskey(stats, "nodes-created") stats["nodes-created"] else nothing end
        nodes_deleted = if haskey(stats, "nodes-deleted") stats["nodes-deleted"] else nothing end
        relationships_created = if haskey(stats, "relationships-created") stats["relationships-created"] else nothing end
        relationships_deleted = if haskey(stats, "relationships-deleted") stats["relationships-deleted"] else nothing end
        properties_set = if haskey(stats, "properties-set") stats["properties-set"] else nothing end
        labels_added = if haskey(stats, "labels-added") stats["labels-added"] else nothing end
        labels_removed = if haskey(stats, "labels-removed") stats["labels-removed"] else nothing end
        indexes_added = if haskey(stats, "indexes-added") stats["indexes-added"] else nothing end
        indexes_removed = if haskey(stats, "indexes-removed") stats["indexes-removed"] else nothing end
        constraints_added = if haskey(stats, "constraints-added") stats["constraints-added"] else nothing end
        constraints_removed = if haskey(stats, "constraints-removed") stats["constraints-removed"] else nothing end
      
        return new(nodes_created, nodes_deleted, relationships_created, relationships_deleted, properties_set, labels_added,
            labels_removed, indexes_added, indexes_removed, constraints_added, constraints_removed)
    end
end

# Summary Counters ---------------- #
# # # # # # # # # # # # # # # # # # #



#####################################
#               Record              #
#####################################

function record_data(dict::OrderedDict{String, Any}, keys::Union{Vector, Nothing}=nothing)
    if keys != nothing
        d = OrderedDict{String, Any}()
        for key in keys
            if isa(key, Integer)
                if key > 0 && key <= length(dict)
                    k = dict.keys[key]
                    d[k] = dict[k]
                else
                    throw(ErrorException("Invalid Index"))
                end
            else
                if haskey(dict, key)
                    d[key] = dict[key]
                else
                    d[key] = nothing
                end
            end
        end
        return d
    end
    return copy(dict)
end


function record_values(dict::OrderedDict{String, Any}, keys::Union{Vector, Nothing}=nothing)
    if keys != nothing
        d = []
        for key in keys
            if isa(key, Integer)
                if key > 0 && key <= length(dict)
                    push!(d, dict.vals[key])
                else
                    throw(ErrorException("Invalid Index"))
                end
            else
                if haskey(dict, key)
                    push!(d, dict[key])
                else
                    push!(d, nothing)
                end
            end
        end
        return d
    end

    return collect(Base.values(dict))
end


function record_items(dict::OrderedDict{String, Any}, keys::Union{Vector, Nothing}=nothing)
    if keys != nothing
        d = []
        for key in keys
            if isa(key, Integer)
                if key > 0 && key <= length(dict)
                    push!(d, (dict.keys[key], dict.vals[key]))
                else
                    throw(ErrorException("Invalid Index"))
                end
            else
                if haskey(dict, key)
                    push!(d, (key, dict[key]))
                else
                    push!(d, (key, nothing))
                end
            end
        end
        return d
    end

    return [(k,v) for (k,v) in dict]
end


function index(dict::OrderedDict{String, Any}, key::Integer)
    if key > 0 && key <= length(dict)
        return key
    else
        throw(ErrorException("Invalid Index"))
    end
end


function index(dict::OrderedDict{String, Any}, key::String)
    i = OrderedCollections.ht_keyindex(dict, key, true)
    if i > 0
        return i
    else
        throw(ErrorException("Invalid Key"))
    end
end


function value(dict::OrderedDict{String, Any}, key::Union{String, Integer}=1, default=nothing)
    if isa(key, Integer)
        if key > 0 && key <= length(dict)
            return dict.vals[key]
        else
            return default
        end
    else
        if haskey(dict, key)
            return dict[key]
        else
            return default
        end
    end
end


# Record -------------------------- #
# # # # # # # # # # # # # # # # # # #


end # module