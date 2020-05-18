using URIParser

struct Address
    scheme::String
    host::String
    port::Integer
end

function from_uri(uri::String, default_port::Integer=0)
    parsed = URI(uri)
    return Address(parsed.scheme, parsed.host, if parsed.port == 0 default_port else parsed.port end)
end

Base.:(==)(x::Address, y::Address) = x.scheme == y.scheme && x.host == y.host && x.port == y.port