using Test
using Sockets
using Dates

include("../../src/Neo4jBolt.jl")
using .Neo4jBolt


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

@testset "ResultConsumptionTestCase" begin
    tc = setup()
    
    @testset "test_can_consume_result_immediately" begin
        function w(tx, args, kwargs)
            result = run(tx, "UNWIND range(1, 3) AS n RETURN n")
            @test [value(record, 1) for record in result] == [1, 2, 3]
        end
        
        session(tc.driver) do sess
            read_transaction(sess, w)
        end
    end
    
    @testset "test_can_consume_result_from_buffer" begin
        function w(tx, args, kwargs)
            result = run(tx, "UNWIND range(1, 3) AS n RETURN n")
            Neo4jBolt.detach(result)
            @test [value(record, 1) for record in result] == [1, 2, 3]
        end
        
        session(tc.driver) do sess
            read_transaction(sess, w)
        end
    end

    @testset "test_can_consume_result_after_commit" begin
        session(tc.driver) do sess
            tx = begin_transaction(sess)
            result = run(tx, "UNWIND range(1, 3) AS n RETURN n")
            commit(tx)
            @test [value(record, 1) for record in result] == [1, 2, 3] 
        end
    end

    @testset "test_can_consume_result_after_rollback" begin
        session(tc.driver) do sess
            tx = begin_transaction(sess)
            result = run(tx, "UNWIND range(1, 3) AS n RETURN n")
            rollback(tx)
            @test [value(record, 1) for record in result] == [1, 2, 3] 
        end
    end

    @testset "test_can_consume_result_after_session_close" begin
        result = nothing
        session(tc.driver) do sess
            tx = begin_transaction(sess)
            result = run(tx, "UNWIND range(1, 3) AS n RETURN n")
            commit(tx)
        end
        @test [value(record, 1) for record in result] == [1, 2, 3] 
    end

    @testset "test_can_consume_result_after_session_reuse" begin
        sess = session(tc.driver)
        tx = begin_transaction(sess)
        result_a = run(tx, "UNWIND range(1, 3) AS n RETURN n")
        commit(tx)
        close(sess)
        sess = session(tc.driver)
        tx = begin_transaction(sess)
        result_b = run(tx, "UNWIND range(4, 6) AS n RETURN n")
        commit(tx)
        close(sess)
        @test [value(record, 1) for record in result_a] == [1, 2, 3] 
        @test [value(record, 1) for record in result_b] == [4, 5, 6] 
    end

    @testset "test_can_consume_results_after_harsh_session_death" begin
        sess = session(tc.driver)
        result_a = run(sess, "UNWIND range(1, 3) AS n RETURN n")
        close(sess)
        sess = nothing
        sess = session(tc.driver)
        result_b = run(sess, "UNWIND range(4, 6) AS n RETURN n")
        close(sess)
        sess = nothing
        @test [value(record, 1) for record in result_a] == [1, 2, 3] 
        @test [value(record, 1) for record in result_b] == [4, 5, 6] 
    end    

    @testset "test_can_consume_result_after_session_with_error" begin
        sess = session(tc.driver)
        @test_throws Neo4jBolt.JuliaBolt.CypherSyntaxError consume(run(sess, "X"))
        close(sess)
 
        sess = session(tc.driver)
        tx = begin_transaction(sess)
        result = run(tx, "UNWIND range(1, 3) AS n RETURN n")
        commit(tx)
        close(sess)

        @test [value(record, 1) for record in result] == [1, 2, 3]
    end    

    @testset "test_single_with_exactly_one_record" begin
        sess = session(tc.driver)
        result = run(sess, "UNWIND range(1, 3) AS n RETURN n")
        record = single(result)
        values(record) == [1]
    end    
                      
    teardown(tc.driver)
end