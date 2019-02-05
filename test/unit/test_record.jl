using Test
using OrderedCollections

include("../../src/Neo4jBolt.jl")
using .Neo4jBolt

# OrderedDict is used in place of a Python Neo4j's Record Object

@testset "RecordTestCase" begin
    
    @testset "test_record_equality" begin
        record1 = OrderedDict(zip(["name", "empire"], ["Nigel", "The British Empire"]))
        record2 = OrderedDict(zip(["name", "empire"], ["Nigel", "The British Empire"]))
        record3 = OrderedDict(zip(["name", "empire"], ["Stefan", "Das Deutschland"]))
        @test record1 == record2
        @test record1 != record3
        @test record2 != record3
    end


    @testset "test_record_hashing" begin
        record1 = OrderedDict(zip(["name", "empire"], ["Nigel", "The British Empire"]))
        record2 = OrderedDict(zip(["name", "empire"], ["Nigel", "The British Empire"]))
        record3 = OrderedDict(zip(["name", "empire"], ["Stefan", "Das Deutschland"]))
        @test hash(record1) == hash(record2)
        @test hash(record1) != hash(record3)
        @test hash(record2) != hash(record3)
    end
    

    @testset "test_record_iter" begin
        record1 = OrderedDict(zip(["name", "empire"], ["Nigel", "The British Empire"]))
        @test collect(values(record1)) == ["Nigel", "The British Empire"]
    end


    @testset "test_record_as_dict" begin
        record1 = OrderedDict(zip(["name", "empire"], ["Nigel", "The British Empire"]))
        @test record1 == Dict("name"=>"Nigel", "empire"=>"The British Empire")
    end

    # test_record_as_list is equivelent to test_record_iter

    @testset "test_record_len" begin
        a_record = OrderedDict(zip(["name", "empire"], ["Nigel", "The British Empire"]))
        @test length(a_record) == 2
    end

    # test_record_repr

    @testset "test_record_data" begin
        r = OrderedDict(zip(["name", "age", "married"], ["Alice", 33, true]))
        @test record_data(r) == Dict("name"=>"Alice", "age"=>33, "married"=>true)
        @test record_data(r, ["name"]) == Dict("name"=>"Alice")
        @test record_data(r, ["age", "name"]) == Dict("age"=>33, "name"=>"Alice")
        @test record_data(r, ["age", "name", "shoe size"]) == Dict("age"=>33, "name"=>"Alice", "shoe size"=>nothing)
        @test record_data(r, [1,"name"]) == Dict("name"=>"Alice")
        @test record_data(r, [1]) == Dict("name"=>"Alice")
        @test record_data(r, [1,2]) == Dict("age"=>33, "name"=>"Alice")
        @test_throws ErrorException record_data(r, [2,1,1000])    
    end
    

    @testset "test_record_keys" begin
        r = OrderedDict(zip(["name", "age", "married"], ["Alice", 33, true]))
        @test collect(keys(r)) == ["name", "age", "married"]
    end


    @testset "test_record_values" begin
        r = OrderedDict(zip(["name", "age", "married"], ["Alice", 33, true]))
        @test record_values(r) == ["Alice", 33, true]
        @test record_values(r, ["name"]) == ["Alice"]
        @test record_values(r, ["age", "name"]) == [33, "Alice"]
        @test record_values(r, ["age", "name", "shoe size"]) == [33, "Alice", nothing]
        @test record_values(r, [1, "name"]) == ["Alice", "Alice"]
        @test record_values(r, [1]) == ["Alice"]
        @test record_values(r, [2, 1]) == [33, "Alice"]
        @test_throws ErrorException record_values(r, [2, 1, 1000])    
    end


    @testset "test_record_items" begin
        r = OrderedDict(zip(["name", "age", "married"], ["Alice", 33, true]))
        @test record_items(r) == [("name","Alice"), ("age", 33), ("married", true)]
        @test record_items(r, ["name"]) == [("name","Alice")]
        @test record_items(r, ["age", "name"]) == [("age", 33), ("name","Alice")]
        @test record_items(r, ["age", "name", "shoe size"]) == [("age", 33), ("name","Alice"), ("shoe size", nothing)]
        @test record_items(r, [1, "name"]) == [("name", "Alice"), ("name", "Alice")]
        @test record_items(r, [1]) == [("name","Alice")]
        @test record_items(r, [2, 1]) == [("age", 33), ("name","Alice")]
        @test_throws ErrorException record_items(r, [2, 1, 1000])    
    end


    @testset "test_record_index" begin
        r = OrderedDict(zip(["name", "age", "married"], ["Alice", 33, true]))
        @test record_index(r, "name") == 1
        @test record_index(r, "age") == 2
        @test record_index(r, "married") == 3
        @test_throws ErrorException record_index(r, "shoe size")    
        @test record_index(r, 1) == 1
        @test record_index(r, 2) == 2
        @test record_index(r, 3) == 3
        @test_throws ErrorException record_index(r, 4) 
        @test_throws MethodError record_index(r, nothing) 
    end

    
    @testset "test_record_value" begin
        r = OrderedDict(zip(["name", "age", "married"], ["Alice", 33, true]))
        @test record_value(r) == "Alice"
        @test record_value(r, "name") == "Alice"
        @test record_value(r, "age") == 33
        @test record_value(r, "shoe size") == nothing
        @test record_value(r, "shoe size", 6) == 6
        @test record_value(r, 1) == "Alice"
        @test record_value(r, 2) == 33
        @test record_value(r, 3) == true
        @test record_value(r, 4) == nothing
        @test record_value(r, 4, 6) == 6
        @test_throws MethodError record_index(r, nothing) 
    end

    @testset "test_record_value" begin
        r = OrderedDict(zip(["name", "age", "married"], ["Alice", 33, true]))
        @test in("Alice", values(r))
        @test in(33, values(r))
        @test in(true, values(r))
        @test !in(7.5, values(r))
    end
    
end