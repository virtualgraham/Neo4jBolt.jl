using Test


include("../src/Neo4jBolt.jl")
using .Neo4jBolt



# Unit Tests
println("Unit Tests")

include("unit/test_api.jl")
include("unit/test_record.jl")
include("unit/test_security.jl")
include("unit/test_types.jl")



# Integration Tests Using Local Neo4j Database
println("Integration Tests")


struct TestCase
    driver
end


function setup()
    auth_token = ("neo4j", "password")
    bolt_uri = "bolt://localhost:7687"

    return TestCase(Neo4jBoltDriver(bolt_uri, auth=auth_token))
end


function teardown(driver)
    close(driver)
end


include("integration/test_example.jl")
include("integration/test_result.jl")
include("integration/test_session.jl")
include("integration/test_types.jl")