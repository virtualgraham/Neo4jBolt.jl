using Dates

@testset "NodeTestCase" begin
    
    @testset "test_can_create_node" begin
        g = Graph()
        alice = put_node(g, 1, ["Person"], Dict("name"=>"Alice", "age"=>33))
        @test alice.labels == Set(["Person"])
        @test Set(keys(alice)) == Set(["name", "age"])
        @test Set(values(alice)) == Set(["Alice", 33])
        @test alice.properties == Dict("name"=>"Alice", "age"=>33)
        @test alice.properties["name"] == "Alice"
        @test alice.properties["age"] == 33
        @test length(alice.properties) == 2
        @test "name" in keys(alice.properties)
        @test "age" in keys(alice.properties)
        @test Set(keys(alice.properties)) == Set(["name", "age"])
    end

    @testset "test_null_properties" begin
        g = Graph()
        stuff = put_node(g, 1, [], good=["puppies", "kittens"], bad=nothing)
        @test Set(keys(stuff)) == Set(["good"])
        @test stuff.properties["good"] == ["puppies", "kittens"]
        # Dictionaries in Julia error when accessing nonexistent keys
        @test_throws KeyError stuff.properties["bad"]
        @test length(stuff.properties) == 1
        @test haskey(stuff.properties, "good")
        @test !haskey(stuff.properties, "bad")
    end

    @testset "test_node_equality" begin
        g = Graph()
        node_1 = Node(g, 1234)
        node_2 = Node(g, 1234)
        node_3 = Node(g, 5678)
        @test node_1 == node_2
        @test node_1 != node_3
    end

    @testset "test_node_hashing" begin
        g = Graph()
        node_1 = Node(g, 1234)
        node_2 = Node(g, 1234)
        node_3 = Node(g, 5678)
        @test hash(node_1) == hash(node_2)
        @test hash(node_1) != hash(node_3)
    end
end

@testset "RelationshipTestCase" begin
    
    @testset "test_can_create_relationship" begin
        g = Graph()
        alice = put_node(g, 1, ["Person"], name="Alice", age=33)
        bob = put_node(g, 2, ["Person"], name="Bob", age=44)
        alice_knows_bob = put_relationship(g, 1, alice, bob, "KNOWS", since=1999)
        @test alice_knows_bob.start_node == alice
        @test alice_knows_bob.type == "KNOWS"
        @test alice_knows_bob.end_node == bob
        @test Set(keys(alice_knows_bob)) == Set(["since"])
        @test Set(values(alice_knows_bob)) == Set([1999])
        @test alice_knows_bob.properties == Dict("since"=>1999)
        @test alice_knows_bob.properties["since"] == 1999
    end
    
end

@testset "PathTestCase" begin
    
    @testset "test_can_create_path" begin
        g = Graph()
        alice = put_node(g, 1, ["Person"], Dict("name"=>"Alice", "age"=>33))
        bob = put_node(g, 2, ["Person"], Dict("name"=>"Bob", "age"=>44))
        carol = put_node(g, 3, ["Person"], Dict("name"=>"Carol", "age"=>55))
        alice_knows_bob = put_relationship(g, 1, alice, bob, "KNOWS", Dict("since"=>1999))
        carol_dislikes_bob = put_relationship(g, 2, carol, bob, "DISLIKES")
        path = Path(alice, [alice_knows_bob, carol_dislikes_bob])
        @test start_node(path) == alice
        @test end_node(path) == carol
        @test path.nodes == [alice, bob, carol]
        @test path.relationships == [alice_knows_bob, carol_dislikes_bob]
    end


    @testset "test_can_hydrate_path" begin
        g = Graph()
        alice = put_node(g, 1, ["Person"], Dict("name"=>"Alice", "age"=>33))
        bob = put_node(g, 2, ["Person"], Dict("name"=>"Bob", "age"=>44))
        carol = put_node(g, 3, ["Person"], Dict("name"=>"Carol", "age"=>55))
        r = [Neo4jBolt.put_unbound_relationship(g, 1, "KNOWS", Dict("since"=>1999)), Neo4jBolt.put_unbound_relationship(g, 2, "DISLIKES")]
        path = Neo4jBolt.hydrate_path([alice, bob, carol], r, [1,1,-2,2])
        @test start_node(path) == alice
        @test end_node(path) == carol
        @test path.nodes == [alice, bob, carol]
        expected_alice_knows_bob = put_relationship(g, 1, alice, bob, "KNOWS", Dict("since"=>1999))
        expected_carol_dislikes_bob = put_relationship(g, 2, carol, bob, "DISLIKES")
        @test path.relationships == [expected_alice_knows_bob, expected_carol_dislikes_bob]
    end


    @testset "test_path_equality" begin
        g = Graph()
        alice = put_node(g, 1, ["Person"], Dict("name"=>"Alice", "age"=>33))
        bob = put_node(g, 2, ["Person"], Dict("name"=>"Bob", "age"=>44))
        carol = put_node(g, 3, ["Person"], Dict("name"=>"Carol", "age"=>55))
        alice_knows_bob = put_relationship(g, 1, alice, bob, "KNOWS", Dict("since"=>1999))
        carol_dislikes_bob = put_relationship(g, 2, carol, bob, "DISLIKES")
        path_1 = Path(alice, [alice_knows_bob, carol_dislikes_bob])
        path_2 = Path(alice, [alice_knows_bob, carol_dislikes_bob])
        @test path_1 == path_2
        @test path_2 != "this is not a path"
    end

    @testset "test_path_hashing" begin
        g = Graph()
        alice = put_node(g, 1, ["Person"], Dict("name"=>"Alice", "age"=>33))
        bob = put_node(g, 2, ["Person"], Dict("name"=>"Bob", "age"=>44))
        carol = put_node(g, 3, ["Person"], Dict("name"=>"Carol", "age"=>55))
        alice_knows_bob = put_relationship(g, 1, alice, bob, "KNOWS", Dict("since"=>1999))
        carol_dislikes_bob = put_relationship(g, 2, carol, bob, "DISLIKES")
        path_1 = Path(alice, [alice_knows_bob, carol_dislikes_bob])
        path_2 = Path(alice, [alice_knows_bob, carol_dislikes_bob])
        @test hash(path_1) == hash(path_2)
    end
end

@testset "HydrationTestCase" begin
    
    hydrant = PackStreamHydrator(1)
    
    @testset "test_can_hydrate_node_structure" begin
        structure = Neo4jBolt.Structure(UInt8('N'), [123, ["Person"], Dict("name"=>"Alice")])
        alice, = Neo4jBolt.hydrate(hydrant, [structure])
        @test alice.id == 123
        @test alice.labels == Set(["Person"])
        @test keys(alice) == Set(["name"])
        @test alice.properties["name"] == "Alice"
    end

    @testset "test_hydrating_unknown_structure_returns_same" begin
        structure = Neo4jBolt.Structure(UInt8('?'), ["foo"])
        mystery, = Neo4jBolt.hydrate(hydrant, [structure])
        @test mystery == structure
    end

    @testset "test_can_hydrate_in_list" begin
        structure = Neo4jBolt.Structure(UInt8('N'), [123, ["Person"], Dict("name"=>"Alice")])
        alice_in_list, = Neo4jBolt.hydrate(hydrant, [[structure]])
        @test isa(alice_in_list, Vector)
        alice, = alice_in_list
        @test alice.id == 123
        @test alice.labels == Set(["Person"])
        @test keys(alice) == Set(["name"])
        @test alice.properties["name"] == "Alice"
    end

    @testset "test_can_hydrate_in_dict" begin
        structure = Neo4jBolt.Structure(UInt8('N'), [123, ["Person"], Dict("name"=>"Alice")])
        alice_in_dict, = Neo4jBolt.hydrate(hydrant, [Dict("foo"=>structure)])
        @test isa(alice_in_dict, Dict)
        alice = alice_in_dict["foo"]
        @test alice.id == 123
        @test alice.labels == Set(["Person"])
        @test keys(alice) == Set(["name"])
        @test alice.properties["name"] == "Alice"
    end

    @testset "test_can_hydrate_relationship_structure" begin
        structure_1 = Neo4jBolt.Structure(UInt8('N'), [1, ["Person"], Dict("name"=>"Alice")])
        structure_2 = Neo4jBolt.Structure(UInt8('N'), [2, ["Person"], Dict("name"=>"Bob")])
        structure_3 = Neo4jBolt.Structure(UInt8('R'), [1, 1, 2, "KNOWS", Dict("since"=>1999)])
        alice, bob, alice_knows_bob = Neo4jBolt.hydrate(hydrant, [structure_1, structure_2, structure_3])
        @test isa(alice, Node)
        @test isa(bob, Node)
        @test isa(alice_knows_bob, Relationship)
        @test alice.id == 1
        @test bob.id == 2
        @test alice_knows_bob.id == 1
    end

    @testset "test_can_hydrate_unbound_relationship_structure" begin

    end

    @testset "test_can_hydrate_path_structure" begin

    end
end

@testset "TemporalHydrationTestCase" begin
     
    @testset "test_can_hydrate_julia_date_time_structure" begin
        hydrant = PackStreamHydrator(2)
        structure = Neo4jBolt.Structure(UInt8('d'), [1539344261, 474716862])
        dt, = Neo4jBolt.hydrate(hydrant, [structure])
        @test Dates.year(dt) == 2018
        @test Dates.month(dt) == 10
        @test Dates.day(dt) == 12
        @test Dates.hour(dt) == 11
        @test Dates.minute(dt) == 37
        @test Dates.second(dt) == 41
        @test Dates.millisecond(dt) == 474
    end

    @testset "test_can_hydrate_wrapper_date_time_structure" begin
        hydrant = PackStreamHydrator(2, false)
        structure = Neo4jBolt.Structure(UInt8('d'), [1539344261, 474716862])
        dt, = Neo4jBolt.hydrate(hydrant, [structure])
        dt.seconds == 1539344261
        dt.nanoseconds == 474716862
    end
end