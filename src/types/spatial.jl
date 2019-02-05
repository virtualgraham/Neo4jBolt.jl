abstract type AbstractPoint end
abstract type CartesianPoint <: AbstractPoint end
abstract type WGS84Point <: AbstractPoint  end


Base.:(==)(x::AbstractPoint, y::AbstractPoint) = x.srid == y.srid && x.coords == y.coords


mutable struct Point <: AbstractPoint
    srid::Integer
    coords::Array{Float64}
end


mutable struct CartesianPoint2 <: CartesianPoint
    srid::Integer
    coords::Array{Float64}
    x::Float64
    y::Float64
end


mutable struct CartesianPoint3 <: CartesianPoint
    srid::Integer
    coords::Array{Float64}
    x::Float64
    y::Float64
    z::Float64
end


CartesianPoint(x::Number,y::Number) = CartesianPoint2(7203, [x,y], x, y)
CartesianPoint(x::Number,y::Number,z::Number) = CartesianPoint3(9157, [x,y,z], x, y, z)
CartesianPoint(p::Tuple{Number,Number}) = CartesianPoint(p[1], p[2])
CartesianPoint(p::Tuple{Number,Number, Number}) = CartesianPoint(p[1], p[2], p[3])
  
    
mutable struct WGS84Point2 <: WGS84Point
    srid::Integer
    coords::Array{Float64}
    longitude::Float64
    latitude::Float64
    x::Float64
    y::Float64
end


mutable struct WGS84Point3 <: WGS84Point
    srid::Integer
    coords::Array{Float64}
    longitude::Float64
    latitude::Float64
    height::Float64
    x::Float64
    y::Float64
    z::Float64
end


WGS84Point(longitude::Number,latitude::Number) = WGS84Point2(4326, [longitude,latitude], longitude, latitude, longitude, latitude)
WGS84Point(longitude::Number,latitude::Number,height::Number) = WGS84Point3(4979, [longitude,latitude,height], longitude, latitude, height, longitude, latitude, height)
WGS84Point(p::Tuple{Number,Number}) = WGS84Point(p[1], p[2])
WGS84Point(p::Tuple{Number,Number, Number}) = WGS84Point(p[1], p[2], p[3])


function hydrate_point(srid::Integer, coordinates::Vector)
    if srid == 7203 
        CartesianPoint2(srid, coordinates, coordinates[1], coordinates[2])
    elseif srid == 9157
        CartesianPoint3(srid, coordinates, coordinates[1], coordinates[2], coordinates[3])
    elseif srid == 4326 
        WGS84Point2(srid, coordinates, coordinates[1], coordinates[2], coordinates[1], coordinates[2])
    elseif srid == 4979
        WGS84Point3(srid, coordinates, coordinates[1], coordinates[2], coordinates[3], coordinates[1], coordinates[2], coordinates[3])
    else
        Point(srid, coordinates)
    end
end


hydrate_point(srid::Integer, a, b...) = hydrate_point(srid, [a, b...])


function dehydrate_point(value::AbstractPoint)
    dim = length(value.coords)
    if dim == 2
        return Structure(UInt8('X'), Any[value.srid, value.coords...])
    elseif dim == 3
        return Structure(UInt8('Y'), Any[value.srid, value.coords...])
    else
        throw(ValueError("Cannot dehydrate Point with $(dim) dimensions"))
    end
end


dehydrate(value::AbstractPoint) = dehydrate_point(value)


function spatial_hydration_functions()
    return Dict(
        UInt8('X') => (psh, values)->hydrate_point(values...),
        UInt8('Y') => (psh, values)->hydrate_point(values...)
    )
end