using Test

include("../../src/Neo4jBolt.jl")
using .Neo4jBolt


@testset "AuthTokenTestCase" begin
    
    @testset "test_should_generate_kerberos_auth_token_correctly" begin
        auth = kerberos_auth("I am a base64 service ticket")
        @test auth["scheme"] == "kerberos"
        @test auth["principal"] == ""
        @test auth["credentials"] == "I am a base64 service ticket"
        @test !haskey(auth, "realm")
        @test !haskey(auth, "parameters")
    end

    @testset "test_should_generate_basic_auth_without_realm_correctly" begin
        auth = basic_auth("molly", "meoooow")
        @test auth["scheme"] == "basic"
        @test auth["principal"] == "molly"
        @test auth["credentials"] == "meoooow"
        @test !haskey(auth, "realm")
        @test !haskey(auth, "parameters")
    end

    @testset "test_should_generate_base_auth_with_realm_correctly" begin
        auth = basic_auth("molly", "meoooow", "cat_cafe")
        @test auth["scheme"] == "basic"
        @test auth["principal"] == "molly"
        @test auth["credentials"] == "meoooow"
        @test auth["realm"] == "cat_cafe"
        @test !haskey(auth, "parameters")
    end

    @testset "test_should_generate_custom_auth_correctly" begin
        auth = custom_auth("cat", "molly", "meoooow", "cat_cafe", age="1", color="white")
        @test auth["scheme"] == "cat"
        @test auth["principal"] == "molly"
        @test auth["credentials"] == "meoooow"
        @test auth["realm"] == "cat_cafe"
        @test auth["parameters"] == Dict("age"=>"1", "color"=>"white")
    end
end