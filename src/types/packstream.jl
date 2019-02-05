using OrderedCollections

include("graph.jl")
include("spatial.jl")
include("temporal.jl")

INT64_MIN = -(2^63)
INT64_MAX = (2^63) - 1



mutable struct PackStreamHydrator
    graph
    hydration_functions
    use_julia_dates
    
    function PackStreamHydrator(protocol_version, use_julia_dates=true)
        graph = Graph()
        hydration_functions = Dict()
        merge!(hydration_functions, graph_hydration_functions(graph))
        if protocol_version >= 2
            merge!(hydration_functions, spatial_hydration_functions())
            merge!(hydration_functions, temporal_hydration_functions())
        end
        return new(graph, hydration_functions, use_julia_dates)
    end
end

function hydrate(psh::PackStreamHydrator, values)
    function hydrate_(value)
        if isa(value, Structure)
            if haskey(psh.hydration_functions, value.tag)
                return psh.hydration_functions[value.tag](psh, map(hydrate_, value.fields))
            else
                return value
            end
        elseif isa(value, Vector)
            return map(hydrate_, value)
        elseif isa(value, Dict)
            r = Dict()
            for (k, v) in value
                r[k] = hydrate_(v)
            end
            return r
        else
            return value
        end
    end

    return map(hydrate_, values)
end

function hydrate_records(psh::PackStreamHydrator, keys, record_values)
    return [OrderedDict{String, Any}(zip(keys, hydrate(psh, value))) for value in record_values]
end

mutable struct PackStreamDehydrator
    supports_bytes
    protocol_version
    
    function PackStreamDehydrator(protocol_version; supports_bytes=false)
        return new(supports_bytes, protocol_version)
    end
end

function dehydrate(psd::PackStreamDehydrator, values::Vector)

    function dehydrate__(value::Vector)
        return map(dehydrate_, value)
    end

    function dehydrate__(value::Dict)
        r = Dict()
        for (k, v) in value
            r[k] = dehydrate_(v)
        end
        return r
    end

    function dehydrate_(value)
        if (isa(value, Nothing) || isa(value, Bool) || isa(value, AbstractFloat) || isa(value, String))
            return value
        elseif isa(value, Integer)
            if INT64_MIN <= value <= INT64_MAX
                return value
            else
                throw(ErrorException("Integer out of range"))
            end
        elseif isa(value, Vector{UInt8}) 
            if psd.supports_bytes
                return value
            else
                throw(TypeError())
            end
        elseif isa(value, Vector) || isa(value, Dict)
            dehydrate__(value)
        elseif psd.protocol_version >= 2
            dehydrate(value)
        else
            throw(TypeError())
        end
    end
    
    return map(dehydrate_, values)
end

