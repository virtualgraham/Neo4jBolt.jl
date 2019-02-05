mutable struct Graph
    nodes::Dict
    relationships::Dict
end

Graph() = Graph(Dict(), Dict())


abstract type Entity end


mutable struct Node <: Entity
    graph::Graph
    id::Integer
    labels::Set
    properties::Dict
end


function Node(graph::Graph, id::Integer)
    if !haskey(graph.nodes, id)
        graph.nodes[id] = Node(graph, id, Set(), Dict())
    end
    return graph.nodes[id]
end


function Base.:(==)(x::Node, y::Node)
    return x.id == y.id && x.labels == y.labels && x.properties == y.properties
end


function Base.hash(x::Node)
    return Base.hash(x.properties, Base.hash(x.id))
end


function Base.keys(node::Node)
    Base.keys(node.properties)
end


function Base.values(node::Node)
    Base.values(node.properties)
end


mutable struct Relationship <: Entity
    graph::Graph
    id::Integer
    type::String
    properties::Dict
    start_node::Union{Node, Nothing}
    end_node::Union{Node, Nothing}
end


function Relationship(graph::Graph, id::Integer, type::String)
    if !haskey(graph.relationships, id)
        graph.relationships[id] = Relationship(graph, id, type, Dict(), nothing, nothing)
    end
    return graph.relationships[id]
end


function Base.hash(x::Relationship)
    return Base.hash(x.properties, Base.hash(x.id))
end


function Base.keys(r::Relationship)
    Base.keys(r.properties)
end


function Base.values(r::Relationship)
    Base.values(r.properties)
end


mutable struct Path 
    nodes::Vector
    relationships::Vector
end


function Path(start_node::Node, relationships::Vector{Relationship})
    nodes = [start_node]
    for (i, relationship) in enumerate(relationships)
        if relationship.start_node == nodes[length(nodes)]
            push!(nodes, relationship.end_node)
        elseif relationship.end_node == nodes[length(nodes)]
            push!(nodes, relationship.start_node)
        else
            throw(ErrorException("Relationship $(i) does not connect to the last node"))
        end
    end
    return Path(nodes, relationships)
end


function Base.length(p::Path)
    return length(p.relationships)
end

function Base.:(==)(x::Path, y::Path)
    return x.nodes[1] == y.nodes[1] && x.relationships == y.relationships
end


function Base.hash(x::Path)
    return Base.hash(x.nodes[1], Base.hash(x.relationships))
end


function start_node(path::Path)
    return path.nodes[1]
end


function end_node(path::Path)
    return path.nodes[length(path.nodes)]
end


function put_node(graph::Graph, n_id::Integer, labels::Set, properties::Dict=Dict(); kwproperties...)    
    for (k, v) in kwproperties
        if v == nothing
            continue
        end
        properties[String(k)] = v
    end
    
    node = Node(graph, n_id)
    union!(node.labels, labels)
    merge!(node.properties, properties)
    return node
end


put_node(graph::Graph, n_id::Integer, labels::Vector, properties::Dict=Dict(); kwproperties...) = put_node(graph, n_id, Set(labels), properties; kwproperties...)


function put_relationship(graph::Graph, r_id::Integer, start_node::Node, end_node::Node, r_type::String, properties::Dict=Dict(); kwproperties...)
    r = put_unbound_relationship(graph, r_id, r_type, properties::Dict; kwproperties...)
    r.start_node = start_node
    r.end_node = end_node
    return r
end


function put_unbound_relationship(graph::Graph, r_id::Integer, r_type::String, properties::Dict=Dict(); kwproperties...)
    for (k, v) in kwproperties
        if v == nothing
            continue
        end
        properties[String(k)] = v
    end
    
    relationship = Relationship(graph, r_id, r_type)
    merge!(relationship.properties, properties)
    return relationship
end


function hydrate_path(nodes::Vector, relationships::Vector, sequence::Vector)
    if length(nodes) == 0
        throw(ErrorException("Empty nodes array"))
    elseif length(sequence) % 2 != 0
        throw(ErrorException("Invalid sequence length"))
    end

    last_node = nodes[1]
    entities = Relationship[]
    for (i, rel_index) in enumerate(sequence[1:2:length(sequence)])
        if rel_index == 0
            throw(ErrorException("Invalid rel_index"))
        end
        n = sequence[2*i]
        if n < 0
            n = length(nodes) + n
        end
        next_node = nodes[n+1] #index
        if rel_index > 0
            r = relationships[rel_index] #index
            r.start_node = last_node
            r.end_node = next_node
            push!(entities, r)
        else
            r = relationships[-rel_index] #index
            r.start_node = next_node
            r.end_node = last_node
            push!(entities, r)
        end
        last_node = next_node
    end
    return Path(nodes[1], entities)
end


function graph_hydration_functions(graph::Graph)
    return Dict(
        UInt8('N')=>(psh, values)->put_node(graph, values...),
        UInt8('R')=>(psh, values)->put_relationship(graph, values[1], Node(graph, values[2]), Node(graph, values[3]), values[4], values[5]),
        UInt8('r')=>(psh, values)->put_unbound_relationship(graph, values...),
        UInt8('P')=>(psh, values)->hydrate_path(values...)
    )
end