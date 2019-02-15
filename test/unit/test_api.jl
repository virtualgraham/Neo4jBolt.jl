function dehydrated_value(value)
    return fix_parameters(Dict("_" => value), 1, supports_bytes=true)["_"]
end

@testset "ValueDehydrationTestCase" begin
    
    @testset "test_should_allow_nothing" begin
        @test dehydrated_value(nothing) == nothing
    end

    @testset "test_should_allow_boolean" begin
        @test dehydrated_value(true) == true
        @test dehydrated_value(false) == false
    end 

    @testset "test_should_allow_integer" begin
        @test dehydrated_value(0) == 0
        @test dehydrated_value(0x7f) == 0x7f
        @test dehydrated_value(0x7FFF) == 0x7FFF
        @test dehydrated_value(0x7FFFFFFF) == 0x7FFFFFFF
        @test dehydrated_value(0x7FFFFFFFFFFFFFFF) == 0x7FFFFFFFFFFFFFFF
    end 

    @testset "test_should_disallow_oversized_integer" begin
        @test_throws ErrorException dehydrated_value(0x10000000000000000)
        @test_throws ErrorException dehydrated_value(-0x10000000000000000)
    end 
    
    @testset "test_should_allow_float" begin
        @test dehydrated_value(0.0) == 0.0
        @test dehydrated_value(3.1415926) == 3.1415926
    end 

    @testset "test_should_allow_string" begin
        @test dehydrated_value("") == ""
        @test dehydrated_value("hello, world") == "hello, world"
    end 

    @testset "test_should_allow_bytes" begin
        @test dehydrated_value(UInt8[]) == UInt8[]
        @test dehydrated_value(UInt8[1,2,3]) == UInt8[1,2,3]
    end 

    @testset "test_should_allow_list" begin
        @test dehydrated_value([]) == []
        @test dehydrated_value([1,2,3]) == [1,2,3]
    end 

    @testset "test_should_allow_dict" begin
        @test dehydrated_value(Dict()) == Dict()
        @test dehydrated_value(Dict("one"=>1, "two"=>2, "three"=>3)) == Dict("one"=>1, "two"=>2, "three"=>3)
        @test dehydrated_value(Dict("list"=>[1,2,3,[4,5,6]], "dict"=>Dict("a"=>1, "b"=>2))) == Dict("list"=>[1,2,3,[4,5,6]], "dict"=>Dict("a"=>1, "b"=>2))
    end 

    
end