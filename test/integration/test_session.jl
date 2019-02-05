using Test
using Sockets
using UUIDs

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


@testset "AutoCommitTransactionTestCase" begin
    tc = setup()
    
    @testset "test_can_run_simple_statement" begin
        sess = session(tc.driver)
        result = run(sess, "RETURN 1 AS n")
        for record in result
            @test value(record, 1) == 1
            @test value(record, "n") == 1
            @test record["n"] == 1
            @test_throws KeyError record["x"]
            @test_throws KeyError record[(1,2)]
            @test length(record) == 1
        end
        close(sess)
    end

    @testset "test_can_run_simple_statement_with_params" begin
        sess = session(tc.driver)
        count = 0
        result = run(sess, "RETURN {x} AS n", Dict("x"=>Dict("abc"=>["d","e","f"])))
        for record in result
            @test value(record, 1) == Dict("abc"=>["d","e","f"])
            @test record["n"] == Dict("abc"=>["d","e","f"])
            @test length(record) == 1
            count+=1
        end
        close(sess)
        @test count == 1
    end

    @testset "test_autocommit_transactions_use_bookmarks" begin
        bookmarks = []
        
        sess = session(tc.driver)
        result = run(sess, "CREATE ()")
        consume(result)
        lb = last_bookmark(sess)
        @test lb != nothing
        push!(bookmarks, lb)
        close(sess)

        sess = session(tc.driver, bookmarks=bookmarks)
        @test sess.bookmarks_in == bookmarks
        result = run(sess, "CREATE ()")
        consume(result)
        lb = last_bookmark(sess)
        @test lb != nothing
        @test !in(lb, bookmarks)
        close(sess)
    end
    
    @testset "test_fails_on_bad_syntax" begin
        sess = session(tc.driver)
        @test_throws Neo4jBolt.JuliaBolt.CypherSyntaxError run(sess, "X")
    end
    
    @testset "test_fails_on_missing_parameter" begin
        sess = session(tc.driver)
        @test_throws Neo4jBolt.JuliaBolt.ClientError run(sess, "RETURN {x}")
    end
    
    @testset "test_can_run_simple_statement_from_bytes_string" begin
        sess = session(tc.driver)
        count = 0
        result = run(sess, b"RETURN 1 AS n")
        for record in result
            @test value(record, 1) == 1
            @test record["n"] == 1
            @test length(record) == 1
            count+=1
        end
        close(sess)
        @test count == 1
    end
    
    @testset "test_can_run_statement_that_returns_multiple_records" begin
        sess = session(tc.driver)
        count = 0
        result = run(sess, "unwind(range(1, 10)) AS z RETURN z")
        for record in result
            @test 1 <= value(record, 1) <= 10
            count+=1
        end
        close(sess)
        @test count == 10
    end
    
    @testset "test_can_use_with_to_auto_close_session" begin
        session(tc.driver) do sess
            record_list = collect(run(sess, "RETURN 1"))
            @test length(record_list) == 1
            for record in record_list
                @test value(record, 1) == 1
            end
        end
    end

    @testset "test_can_return_node" begin
        session(tc.driver) do sess
            record_list = collect(run(sess, "CREATE (a:Person {name:'Alice'}) RETURN a"))
            @test length(record_list) == 1
            for record in record_list
                alice = value(record, 1)
                @test isa(alice, Node)
                @test alice.labels == Set(["Person"])
                @test alice.properties == Dict("name"=>"Alice")
            end
        end
    end

    @testset "test_can_return_relationship" begin
        session(tc.driver) do sess
            record_list = collect(run(sess, "CREATE ()-[r:KNOWS {since:1999}]->() RETURN r"))
            @test length(record_list) == 1
            for record in record_list
                rel = value(record, 1)
                @test isa(rel, Relationship)
                @test rel.type == "KNOWS"
                @test rel.properties == Dict("since"=>1999)
            end
        end
    end

    @testset "test_can_return_path" begin
        session(tc.driver) do sess
            record_list = collect(run(sess, "MERGE p=({name:'Alice'})-[:KNOWS]->({name:'Bob'}) RETURN p"))
            @test length(record_list) == 1
            for record in record_list
                path = value(record, 1)
                @test isa(path, Path)
                @test start_node(path).properties["name"] == "Alice"
                @test end_node(path).properties["name"] == "Bob"
                @test path.relationships[1].type == "KNOWS"
                @test length(path.nodes) == 2
                @test length(path.relationships) == 1
            end
        end
    end
    
    @testset "test_can_handle_cypher_error" begin
        session(tc.driver) do sess
            @test_throws Neo4jBolt.JuliaBolt.CypherSyntaxError run(sess, "X")
        end
    end
    
    @testset "test_keys_are_available_before_and_after_stream" begin
        session(tc.driver) do sess
            result = run(sess, "UNWIND range(1, 10) AS n RETURN n")
            @test Neo4jBolt.keys(result) == ["n"]
            collect(result)
            @test Neo4jBolt.keys(result) == ["n"]
        end
    end

    @testset "test_keys_with_an_error" begin
        session(tc.driver) do sess
            @test_throws Neo4jBolt.JuliaBolt.CypherSyntaxError begin
                run(sess, "X")
                Neo4jBolt.keys(result)
            end
        end
    end

    @testset "test_should_not_allow_empty_statements" begin
        session(tc.driver) do sess
            @test_throws ErrorException run(sess, "")
        end
    end
        
    @testset "test_statement_object" begin
        session(tc.driver) do sess
            v = value(single(run(sess, "RETURN \$x", x=1)))
            @test v == 1
        end
    end
  
    @testset "test_statement_object" begin
        session(tc.driver) do sess
            v = value(single(run(sess, "RETURN \$x", x=1)))
            @test v == 1
        end
    end
     
    @testset "test_autocommit_transactions_should_support_timeout" begin
        session(tc.driver) do sess1
            consume(run(sess1, "CREATE (a:Node)"))
            session(tc.driver) do sess2
                tx1 = begin_transaction(sess1)
                consume(run(tx1, "MATCH (a:Node) SET a.property = 1"))
                @test_throws Neo4jBolt.JuliaBolt.TransientError consume(run(sess2, Statement("MATCH (a:Node) SET a.property = 2", timeout=0.25)))
            end
        end
    end
    
    teardown(tc.driver)
end


@testset "SummaryTestCase" begin
    tc = setup()
    
    @testset "test_can_obtain_summary_after_consuming_result" begin
        session(tc.driver) do sess
            result = run(sess, "CREATE (n) RETURN n")
            summ = Neo4jBolt.summary(result)
            @test summ.statement == "CREATE (n) RETURN n"
            @test summ.parameters == Dict()
            @test summ.statement_type == "rw"
            @test summ.counters.nodes_created == 1
        end
    end

    @testset "test_no_plan_info" begin
        session(tc.driver) do sess
            result = run(sess, "CREATE (n) RETURN n")
            summ = Neo4jBolt.summary(result)
            @test summ.plan == nothing
            @test summ.profile == nothing
        end
    end

    @testset "test_can_obtain_plan_info" begin
        session(tc.driver) do sess
            result = run(sess, "EXPLAIN CREATE (n) RETURN n")
            summ = Neo4jBolt.summary(result)
            plan = summ.plan
            @test plan.operator_type == "ProduceResults"
            @test plan.identifiers == ["n"]
            @test length(plan.children) == 1
        end
    end

    @testset "test_can_obtain_profile_info" begin
        session(tc.driver) do sess
            result = run(sess, "PROFILE CREATE (n) RETURN n")
            summ = Neo4jBolt.summary(result)
            profile = summ.profile
            @test profile.db_hits == 0
            @test profile.rows == 1
            @test profile.operator_type == "ProduceResults"
            @test profile.identifiers == ["n"]
            @test length(profile.children) == 1
        end
    end
        
    @testset "test_no_notification_info" begin
        session(tc.driver) do sess
            result = run(sess, "CREATE (n) RETURN n")
            summ = Neo4jBolt.summary(result)
            notifications = summ.notifications
            @test notifications == []
        end
    end

    @testset "test_can_obtain_notification_info" begin
        session(tc.driver) do sess
            result = run(sess, "EXPLAIN MATCH (n), (m) RETURN n, m")
            summ = Neo4jBolt.summary(result)
            notifications = summ.notifications
            
            @test length(notifications) == 1
            notification = notifications[1]
            @test notification.code == "Neo.ClientNotification.Statement.CartesianProductWarning"
            @test notification.title == "This query builds a cartesian product between " *
                                         "disconnected patterns."
            @test notification.severity == "WARNING"
            @test notification.description == "If a part of a query contains multiple " *
                                               "disconnected patterns, this will build a " *
                                               "cartesian product between all those parts. This " *
                                               "may produce a large amount of data and slow down " *
                                               "query processing. While occasionally intended, " *
                                               "it may often be possible to reformulate the " *
                                               "query that avoids the use of this cross product, " *
                                               "perhaps by adding a relationship between the " *
                                               "different parts or by using OPTIONAL MATCH " *
                                               "(identifier is: (m))"
            position = notification.position
            @test position != nothing
        end
    end
        
    @testset "test_contains_time_information" begin
        session(tc.driver) do sess
            summary = consume(run(sess, "UNWIND range(1,1000) AS n RETURN n AS number"))
            @test isa(summary.t_first, Integer)
            @test isa(summary.t_last, Integer)
        end
    end

    teardown(tc.driver)
end


@testset "ResetTestCase" begin
    tc = setup()
    
    @testset "test_automatic_reset_after_failure" begin
        session(tc.driver) do sess
            try 
                consume(run(sess, "X"))
                throw(ErrorException("A Cypher error should have occurred"))
            catch ex
                if isa(ex, Neo4jBolt.JuliaBolt.CypherSyntaxError)
                    result = run(sess, "RETURN 1")
                    record = [r for r in result][1]
                    @test value(record, 1) == 1
                else
                    @test false
                end
            end
        end
    end

    teardown(tc.driver)
end


@testset "ExplicitTransactionTestCase" begin
    tc = setup()
    
    @testset "test_can_commit_transaction" begin
        session(tc.driver) do sess
            tx = begin_transaction(sess)
            result = run(tx, "CREATE (a) RETURN id(a)")
            record = [r for r in result][1]
            node_id = value(record, 1)
            @test isa(node_id, Integer)

            run(tx, "MATCH (a) WHERE id(a) = {n} SET a.foo = {foo}", Dict("n"=>node_id, "foo"=>"bar"))

            commit(tx)

            result = run(sess, "MATCH (a) WHERE id(a) = {n} RETURN a.foo", Dict("n"=>node_id))
            record = [r for r in result][1]
            v = value(record, 1)

            @test v == "bar"
        end
    end
    
    @testset "test_can_rollback_transaction" begin
        session(tc.driver) do sess
            tx = begin_transaction(sess)
            
            result = run(tx, "CREATE (a) RETURN id(a)")
            record = [r for r in result][1]
            node_id = value(record, 1)
            @test isa(node_id, Integer)

            run(tx, "MATCH (a) WHERE id(a) = {n} SET a.foo = {foo}", Dict("n"=>node_id, "foo"=>"bar"))

            rollback(tx)

            result = run(sess, "MATCH (a) WHERE id(a) = {n} RETURN a.foo", Dict("n"=>node_id))
            @test length([r for r in result]) == 0
        end
    end

    @testset "test_can_rollback_transaction_using_with_block" begin
        session(tc.driver) do sess
            node_id = 0
            begin_transaction(sess) do tx
                result = run(tx, "CREATE (a) RETURN id(a)")
                record = [r for r in result][1]
                node_id = value(record, 1)
                @test isa(node_id, Integer)

                run(tx, "MATCH (a) WHERE id(a) = {n} SET a.foo = {foo}", Dict("n"=>node_id, "foo"=>"bar"))

                tx.success = false
            end
            result = run(sess, "MATCH (a) WHERE id(a) = {n} RETURN a.foo", Dict("n"=>node_id))
            @test length([r for r in result]) == 0
        end
    end

    @testset "test_broken_transaction_should_not_break_session" begin
        session(tc.driver) do sess
            @test_throws Neo4jBolt.JuliaBolt.CypherSyntaxError begin
                begin_transaction(sess) do tx
                    run(tx, "X")
                end
            end
            begin_transaction(sess) do tx
                 run(tx, "RETURN 1")
            end
        end
    end

    @testset "test_last_run_statement_should_be_cleared_on_failure" begin
        session(tc.driver) do sess
            tx = begin_transaction(sess)
            consume(run(tx, "RETURN 1"))
            connection_1 = sess.connection
            @test connection_1.last_run_statement == "RETURN 1"
            result = run(tx, "X")
            connection_2 = sess.connection
            @test_throws Neo4jBolt.JuliaBolt.CypherSyntaxError consume(result)
            @test connection_2 === connection_1
            @test connection_2.last_run_statement == nothing
            close(tx)
        end
    end    
        
    @testset "test_statement_object_not_supported" begin
        session(tc.driver) do sess
            begin_transaction(sess) do tx
                 @test_throws ErrorException run(tx, Statement("RETURN 1", timeout=0.25))
            end
        end
    end   
            
    @testset "test_transaction_timeout" begin
        session(tc.driver) do sess1
            consume(run(sess1, "CREATE (a:Node)"))
            session(tc.driver) do sess2
                tx1 = begin_transaction(sess1)
                consume(run(tx1, "MATCH (a:Node) SET a.property = 1"))
                tx2 = begin_transaction(sess2, timeout=0.25)
                @test_throws Neo4jBolt.JuliaBolt.TransientError consume(run(tx2, "MATCH (a:Node) SET a.property = 2"))
            end
        end
    end   

    @testset "test_exit_after_explicit_close_should_be_silent" begin
        session(tc.driver) do sess
            tx_ = nothing
            begin_transaction(sess) do tx
                tx_ = tx
                @test !tx.closed
                close(tx)
                @test tx.closed
            end
            @test tx_.closed
        end
    end  
                  
    teardown(tc.driver)
end




@testset "BookmarkingTestCase" begin
    tc = setup()

    @testset "test_can_obtain_bookmark_after_commit" begin
        session(tc.driver) do sess
            begin_transaction(sess) do tx
                run(tx, "RETURN 1")
            end
            @test last_bookmark(sess) != nothing
        end
    end    

    @testset "test_can_pass_bookmark_into_next_transaction" begin
        unique_id = string(uuid4())
        bookmark = nothing
        
        session(tc.driver, access_mode=WRITE_ACCESS) do sess
            begin_transaction(sess) do tx
                run(tx, "CREATE (a:Thing {uuid:\$uuid})", uuid=unique_id)
            end
            bookmark = last_bookmark(sess)
        end

        @test bookmark != nothing

        session(tc.driver, access_mode=READ_ACCESS, bookmark=bookmark) do sess
            begin_transaction(sess) do tx
                result = run(tx, "MATCH (a:Thing {uuid:\$uuid}) RETURN a", uuid=unique_id)
                record_list = [r for r in result]
                @test length(record_list) == 1
                record = record_list[1]
                @test length(record) == 1
                thing = value(record, 1)
                @test isa(thing, Node)
                @test thing.properties["uuid"] == unique_id
            end
        end
    end    

    @testset "test_bookmark_should_be_none_after_rollback" begin
        sess_ = nothing
        
        session(tc.driver, access_mode=WRITE_ACCESS) do sess
            sess_ = sess
            begin_transaction(sess) do tx
                run(tx, "CREATE (a)")
            end
        end
        
        @test last_bookmark(sess_) != nothing
        
        session(tc.driver, access_mode=WRITE_ACCESS) do sess
            sess_ = sess
            begin_transaction(sess) do tx
                run(tx, "CREATE (a)")
                tx.success = false
            end
        end
        
        @test last_bookmark(sess_) == nothing
    end    

    teardown(tc.driver)
end


@testset "SessionCompletionTestCase" begin
    tc = setup()

    @testset "test_should_sync_after_commit" begin
        session(tc.driver) do sess
            tx = begin_transaction(sess)
            result = run(tx, "RETURN 1")
            commit(tx)
            @test length(result.records) == 1
            @test value(result.records[1], 1) == 1
        end
    end
    
    @testset "test_should_sync_after_rollback" begin
        session(tc.driver) do sess
            tx = begin_transaction(sess)
            result = run(tx, "RETURN 1")
            rollback(tx)
            @test length(result.records) == 1
            @test value(result.records[1], 1) == 1
        end
    end

    @testset "test_errors_on_write_transaction" begin
        sess = session(tc.driver)
        @test_throws MethodError write_transaction(sess, (tx, args, kwargs)->run(tx, "CREATE (a:Thing {uuid:\$uuid})", uuid=args[1]), UUIDs.uuid4())
        close(sess)
    end

    @testset "test_errors_on_run_transaction" begin
        sess = session(tc.driver)
        tx = begin_transaction(sess)
        @test_throws MethodError run(tx, "CREATE (a:Thing {uuid:\$uuid})", uuid=uuid4())
        rollback(tx)
        close(sess)
    end

    @testset "test_errors_on_run_session" begin
        sess = session(tc.driver)
        close(sess)
        @test_throws ErrorException run(sess, "RETURN 1")
    end        

    @testset "test_errors_on_begin_transaction" begin
        sess = session(tc.driver)
        close(sess)
        @test_throws ErrorException begin_transaction(sess)
    end    

    @testset "test_large_values" begin
        for i in 1:6
            sess = session(tc.driver)
            run(sess, "RETURN '$(repeat('A', 2^20))'")
            close(sess)
            @test true # no error thrown
        end
    end     

    teardown(tc.driver)
end


@testset "TransactionCommittedTestCase" begin
    tc = setup()

    @testset "test_errors_on_run" begin
        sess = session(tc.driver)
        tx = begin_transaction(sess)
        run(tx, "RETURN 1")
        commit(tx)
        @test_throws ErrorException run(tx, "RETURN 1")
    end     
    
    teardown(tc.driver)
end


@testset "TransactionFunctionTestCase" begin
    tc = setup()

    @testset "test_simple_read" begin
        function work(tx, args, kwargs)
            return value(single(run(tx, "RETURN 1")))
        end

        session(tc.driver) do sess
            v = read_transaction(sess, work)
            @test v == 1
        end
    end     

     @testset "test_read_with_arg" begin
        function work(tx, args, kwargs)
            return value(single(run(tx, "RETURN \$x", x=kwargs[:x])))
        end

        session(tc.driver) do sess
            v = read_transaction(sess, work, x=7)
            @test v == 7
        end
    end     

     @testset "test_simple_write" begin
        function work(tx, args, kwargs)
            return value(single(run(tx, "CREATE (a {x: 7}) RETURN a.x")))
        end

        session(tc.driver) do sess
            v = write_transaction(sess, work)
            @test v == 7
        end
    end    

     @testset "test_write_with_arg" begin
        function work(tx, args, kwargs)
            return value(single(run(tx, "CREATE (a {x: \$x}) RETURN a.x", x=kwargs[:x])))
        end

        session(tc.driver) do sess
            v = write_transaction(sess, work, x=7)
            @test v == 7
        end
    end    

     @testset "test_write_with_arg_and_metadata" begin

        work = UnitOfWork(
            function(tx, args, kwargs)
                return value(single(run(tx, "CREATE (a {x: \$x}) RETURN a.x", x=kwargs[:x])))
            end,
            Dict("foo"=>"bar"),
            25
        )

        session(tc.driver) do sess
            v = write_transaction(sess, work, x=7)
            @test v == 7
        end
    end   
              
    teardown(tc.driver)
end