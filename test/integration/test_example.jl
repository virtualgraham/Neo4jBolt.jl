using Test

include("../../src/Neo4jBolt.jl")
using .Neo4jBolt

@testset "ExampleTestCase" begin

    @testset "test_example" begin
        using .Neo4jBolt
        
        driver = Neo4jBoltDriver("bolt://localhost:7687", auth=("neo4j", "password"))
        
        function add_friend(tx, args, kwargs)
            run(tx, "MERGE (a:Person {name: \$name}) " *
                    "MERGE (a)-[:KNOWS]->(friend:Person {name: \$friend_name})", name=args[1], friend_name=args[2])
        end

        function print_friends(tx, args, kwargs)
            for record in run(tx, "MATCH (a:Person)-[:KNOWS]->(friend) WHERE a.name = \$name " *
                                  "RETURN friend.name ORDER BY friend.name", name=args[1])
                println(record["friend.name"])
            end
        end        
        
        session(driver) do sess
            write_transaction(sess, add_friend, "Arthur", "Guinevere")
            write_transaction(sess, add_friend, "Arthur", "Lancelot")
            write_transaction(sess, add_friend, "Arthur", "Merlin")
            read_transaction(sess, print_friends, "Arthur")
        end
        
    end
end
