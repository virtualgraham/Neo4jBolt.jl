using Test
using Sockets
using Dates
using TimeZones

include("../../src/Neo4jBolt.jl")
using .Neo4jBolt


struct TestCase
    driver
end


function setup()
    auth_token = ("neo4j", "password")
    bolt_uri = "bolt://localhost:7687"

    return TestCase(Driver(bolt_uri, auth=auth_token))
end


function teardown(driver)
    close(driver)
end


function run_and_rollback(tx, args, kwargs)
    result = run(tx, args[1]; kwargs...)
    v = value(single(result))
    tx.success = false
    return v
end


@testset "CoreTypeOutputTestCase" begin
    tc = setup()
    
    @testset "test_null" begin
        session(tc.driver) do sess
            result = run(sess, "RETURN null")
            @test value(single(result)) == nothing
        end
    end
    
    @testset "test_boolean" begin
        session(tc.driver) do sess
            result = run(sess, "RETURN true")
            @test value(single(result)) == true
        end
    end

    @testset "test_integer" begin
        session(tc.driver) do sess
            result = run(sess, "RETURN 123456789")
            @test value(single(result)) == 123456789
        end
    end

    @testset "test_float" begin
        session(tc.driver) do sess
            result = run(sess, "RETURN 3.1415926")
            @test value(single(result)) == 3.1415926
        end
    end

    @testset "test_float_nan" begin
        session(tc.driver) do sess
            result = run(sess, "WITH \$x AS x RETURN x", x=NaN)
            @test isnan(value(single(result)))
        end
    end

    @testset "test_float_positive_infinity" begin
        session(tc.driver) do sess
            result = run(sess, "WITH \$x AS x RETURN x", x=Inf)
            @test value(single(result)) == Inf
        end
    end    

    @testset "test_float_negative_infinity" begin
        session(tc.driver) do sess
            result = run(sess, "WITH \$x AS x RETURN x", x=-Inf)
            @test value(single(result)) == -Inf
        end
    end  

    @testset "test_string" begin
        session(tc.driver) do sess
            result = run(sess, "RETURN 'hello, world'")
            @test value(single(result)) == "hello, world"
        end
    end

    @testset "test_bytes" begin
        data = UInt8[0x00, 0x33, 0x66, 0x99, 0xCC, 0xFF]
        session(tc.driver) do sess
            v = write_transaction(sess, run_and_rollback, "CREATE (a {x:\$x}) RETURN a.x", x=data)
            @test v == data
        end
    end

    @testset "test_list" begin
        session(tc.driver) do sess
            result = run(sess, "RETURN ['one', 'two', 'three']")
            @test value(single(result)) == ["one", "two", "three"]
        end
    end

    @testset "test_map" begin
        session(tc.driver) do sess
            result = run(sess, "RETURN {one: 'eins', two: 'zwei', three: 'drei'}")
            @test value(single(result)) == Dict("one"=>"eins", "two"=>"zwei", "three"=>"drei")
        end
    end

    @testset "test_non_string_map_keys" begin
        session(tc.driver) do sess
            @test_throws ErrorException run(sess, "RETURN \$x", x=Dict(1=>"eins", 2=>"zwei", 3=>"drei"))
        end
    end

    teardown(tc.driver)
end


@testset "GraphTypeOutputTestCase" begin
    tc = setup()
    
    @testset "test_node" begin
        session(tc.driver) do sess
            a = write_transaction(sess, run_and_rollback, "CREATE (a:Person {name:'Alice'}) RETURN a")
            @test isa(a, Node)
            @test a.labels == Set(["Person"])
            @test a.properties == Dict("name"=>"Alice")
        end
    end

    @testset "test_relationship" begin
        session(tc.driver) do sess
            a, b, r = write_transaction(sess, run_and_rollback, "CREATE (a)-[r:KNOWS {since:1999}]->(b) RETURN [a, b, r]")
            @test isa(r, Relationship)
            @test r.type == "KNOWS"
            @test r.properties == Dict("since"=>1999)
            @test r.start_node == a
            @test r.end_node == b
        end
    end 

    @testset "test_path" begin
        session(tc.driver) do sess
            a, b, c, ab, bc, p = write_transaction(sess, run_and_rollback, "CREATE p=(a)-[ab:X]->(b)-[bc:X]->(c) RETURN [a, b, c, ab, bc, p]")
            @test isa(p, Path)
            @test length(p) == 2
            @test p.nodes == [a,b,c]
            @test start_node(p) == a
            @test end_node(p) == c
        end
    end 
          
    teardown(tc.driver)
end


@testset "SpatialTypeInputTestCase" begin
    tc = setup()
    
    @testset "test_cartesian_point" begin
        session(tc.driver) do sess
            result = run(sess, "CYPHER runtime=interpreted WITH \$point AS point RETURN point.x, point.y", point=CartesianPoint((1.23, 4.56)))
            x, y = values(single(result))
            @test x == 1.23
            @test y == 4.56
        end
    end

    @testset "test_cartesian_3d_point" begin
        session(tc.driver) do sess
            result = run(sess, "CYPHER runtime=interpreted WITH \$point AS point RETURN point.x, point.y, point.z", point=CartesianPoint((1.23, 4.56, 7.89)))
            x, y, z = values(single(result))
            @test x == 1.23
            @test y == 4.56
            @test z == 7.89
        end
    end
    
    @testset "test_wgs84_point" begin
        session(tc.driver) do sess
            result = run(sess, "CYPHER runtime=interpreted WITH \$point AS point RETURN point.latitude, point.longitude", point=WGS84Point((1.23, 4.56)))
            latitude, longitude = values(single(result))
            @test longitude == 1.23
            @test latitude == 4.56
        end
    end

    @testset "test_wgs84_3d_point" begin
        session(tc.driver) do sess
            result = run(sess, "CYPHER runtime=interpreted WITH \$point AS point RETURN point.latitude, point.longitude, point.height", point=WGS84Point((1.23, 4.56, 7.89)))
            latitude, longitude, height = values(single(result))
            @test longitude == 1.23
            @test latitude == 4.56
            @test height == 7.89
        end
    end

    @testset "test_point_array" begin
        session(tc.driver) do sess
            data = [WGS84Point((1.23, 4.56)), WGS84Point((9.87, 6.54))]
            v = write_transaction(sess, run_and_rollback, "CREATE (a {x:\$x}) RETURN a.x", x=data)
            @test v == data
        end
    end

    teardown(tc.driver)
end


@testset "SpatialTypeOutputTestCase" begin
    tc = setup()

    @testset "test_cartesian_point" begin
        session(tc.driver) do sess
            result = run(sess, "RETURN point({x:3, y:4})")
            v = value(single(result))
            @test isa(v, CartesianPoint)
            @test v.x == 3.0
            @test v.y == 4.0
            @test_throws ErrorException _ = v.z
        end
    end

    @testset "test_cartesian_3d_point" begin
        session(tc.driver) do sess
            result = run(sess, "RETURN point({x:3, y:4, z:5})")
            v = value(single(result))
            @test isa(v, CartesianPoint)
            @test v.x == 3.0
            @test v.y == 4.0
            @test v.z == 5.0
        end
    end
 
    @testset "test_wgs84_point" begin
        session(tc.driver) do sess
            result = run(sess, "RETURN point({latitude:3, longitude:4})")
            v = value(single(result))
            @test isa(v, WGS84Point)
            @test v.latitude == 3.0
            @test v.longitude == 4.0
            @test v.y == 3.0
            @test v.x == 4.0
            @test_throws ErrorException _ = v.height
            @test_throws ErrorException _ = v.z
        end
    end
  
    @testset "test_wgs84_3d_point" begin
        session(tc.driver) do sess
            result = run(sess, "RETURN point({latitude:3, longitude:4, height:5})")
            v = value(single(result))
            @test isa(v, WGS84Point)
            @test v.latitude == 3.0
            @test v.longitude == 4.0
            @test v.height == 5.0
            @test v.y == 3.0
            @test v.x == 4.0
            @test v.z == 5.0
        end
    end
          
    teardown(tc.driver)
end


@testset "TemporalTypeInputTestCase" begin
    tc = setup()

    @testset "test_native_date" begin
        session(tc.driver) do sess
            result = run(sess, "CYPHER runtime=interpreted WITH \$x AS x RETURN x.year, x.month, x.day", x=Date(1976, 6, 13))
            year, month, day = values(single(result))
            @test year == 1976
            @test month == 6
            @test day == 13
        end
    end

    @testset "test_date_wrapper" begin
        session(tc.driver) do sess
            result = run(sess, "CYPHER runtime=interpreted WITH \$x AS x RETURN x.year, x.month, x.day", x=DateWrapper(1976, 6, 13))
            year, month, day = values(single(result))
            @test year == 1976
            @test month == 6
            @test day == 13
        end
    end
    
    @testset "test_date_array" begin
        session(tc.driver) do sess
            data = [today(), Date(1976, 6, 13)]
            v = write_transaction(sess, run_and_rollback, "CREATE (a {x:\$x}) RETURN a.x", x=data)
            @test data == v
        end
    end
    
    @testset "test_native_time" begin
        session(tc.driver) do sess
            result = run(sess, "CYPHER runtime=interpreted WITH \$x AS x RETURN x.hour, x.minute, x.second, x.nanosecond", x=Time(12, 34, 56, 789, 012))
            hour, minute, second, nanosecond = values(single(result))
            @test hour == 12
            @test minute == 34
            @test second == 56
            @test nanosecond == 789012000
        end
    end

    @testset "test_time_wrapper" begin
        session(tc.driver) do sess
            result = run(sess, "CYPHER runtime=interpreted WITH \$x AS x RETURN x.hour, x.minute, x.second, x.nanosecond", x=TimeWrapper(12, 34, 56, 789012000))
            hour, minute, second, nanosecond = values(single(result))
            @test hour == 12
            @test minute == 34
            @test second == 56
            @test nanosecond == 789012000
        end
    end
    
    @testset "test_whole_second_time" begin
        session(tc.driver) do sess
            result = run(sess, "CYPHER runtime=interpreted WITH \$x AS x RETURN x.hour, x.minute, x.second", x=Time(12, 34, 56))
            hour, minute, second = values(single(result))
            @test hour == 12
            @test minute == 34
            @test second == 56
        end
    end

    @testset "test_nanosecond_resolution_time" begin
        session(tc.driver) do sess
            result = run(sess, "CYPHER runtime=interpreted WITH \$x AS x RETURN x.hour, x.minute, x.second, x.nanosecond", x=Time(12, 34, 56, 789, 012, 345))
            hour, minute, second, nanosecond = values(single(result))
            @test hour == 12
            @test minute == 34
            @test nanosecond == 789012345
        end
    end
              
    @testset "test_time_with_numeric_time_offset" begin
        session(tc.driver) do sess
            result = run(sess, "CYPHER runtime=interpreted WITH \$x AS x RETURN x.hour, x.minute, x.second, x.nanosecond, x.offset", x=TimeWrapper(12, 34, 56, 789012345, timezone=90))
            hour, minute, second, nanosecond, offset = values(single(result))
            @test hour == 12
            @test minute == 34
            @test nanosecond == 789012345
            @test offset == "+01:30"
        end
    end
    
    @testset "test_time_array" begin
        session(tc.driver) do sess
            data = [Time(12, 34, 56), Time(10, 0, 0)]
            v = write_transaction(sess, run_and_rollback, "CREATE (a {x:\$x}) RETURN a.x", x=data)
            @test v == data
        end
    end

    @testset "test_native_datetime" begin
        session(tc.driver) do sess
            result = run(sess, "CYPHER runtime=interpreted WITH \$x AS x " *
                                 "RETURN x.year, x.month, x.day, " *
                                 "x.hour, x.minute, x.second, x.nanosecond", x=DateTime(1976, 6, 13, 12, 34, 56, 789))
            year, month, day, hour, minute, second, nanosecond = values(single(result))
            @test year == 1976
            @test month == 6
            @test day == 13
            @test hour == 12
            @test minute == 34
            @test second == 56
            @test nanosecond == 789000000
        end
    end

    @testset "test_datetime_wrapper" begin
        session(tc.driver) do sess
            result = run(sess, "CYPHER runtime=interpreted WITH \$x AS x " *
                                 "RETURN x.year, x.month, x.day, " *
                                 "x.hour, x.minute, x.second, x.nanosecond", x=DateTimeWrapper(1976, 6, 13, 12, 34, 56, 789012000))
            year, month, day, hour, minute, second, nanosecond = values(single(result))
            @test year == 1976
            @test month == 6
            @test day == 13
            @test hour == 12
            @test minute == 34
            @test second == 56
            @test nanosecond == 789012000
        end
    end

    @testset "test_whole_second_datetime" begin
        session(tc.driver) do sess
            result = run(sess, "CYPHER runtime=interpreted WITH \$x AS x " *
                                 "RETURN x.year, x.month, x.day, " *
                                 "x.hour, x.minute, x.second", x=DateTime(1976, 6, 13, 12, 34, 56))
            year, month, day, hour, minute, second = values(single(result))
            @test year == 1976
            @test month == 6
            @test day == 13
            @test hour == 12
            @test minute == 34
            @test second == 56
        end
    end

    @testset "test_nanosecond_resolution_datetime" begin
        session(tc.driver) do sess
            result = run(sess, "CYPHER runtime=interpreted WITH \$x AS x " *
                                 "RETURN x.year, x.month, x.day, " *
                                 "x.hour, x.minute, x.second, x.nanosecond", x=DateTimeWrapper(1976, 6, 13, 12, 34, 56, 789012345))
            year, month, day, hour, minute, second, nanosecond = values(single(result))
            @test year == 1976
            @test month == 6
            @test day == 13
            @test hour == 12
            @test minute == 34
            @test second == 56
            @test nanosecond == 789012345
        end
    end
 
    @testset "test_zoneddatetime_with_numeric_time_offset" begin
        session(tc.driver) do sess
            result = run(sess, "CYPHER runtime=interpreted WITH \$x AS x " *
                                 "RETURN x.year, x.month, x.day, " *
                                 "x.hour, x.minute, x.second, x.nanosecond, x.offset", x=ZonedDateTime(DateTime(1976, 6, 13, 12, 34, 56, 789), FixedTimeZone("TZ", 90 * 60)))
            year, month, day, hour, minute, second, nanosecond, offset = values(single(result))
            @test year == 1976
            @test month == 6
            @test day == 13
            @test hour == 12
            @test minute == 34
            @test second == 56
            @test nanosecond == 789000000
            @test offset == "+01:30"
        end
    end

    @testset "test_datetime_wrapper_with_numeric_time_offset" begin
        session(tc.driver) do sess
            result = run(sess, "CYPHER runtime=interpreted WITH \$x AS x " *
                                 "RETURN x.year, x.month, x.day, " *
                                 "x.hour, x.minute, x.second, x.nanosecond, x.offset", x=DateTimeWrapper(1976, 6, 13, 12, 34, 56, 789012345, timezone=90))
            year, month, day, hour, minute, second, nanosecond, offset = values(single(result))
            @test year == 1976
            @test month == 6
            @test day == 13
            @test hour == 12
            @test minute == 34
            @test second == 56
            @test nanosecond == 789012345
            @test offset == "+01:30"
        end
    end

    @testset "test_zoneddatetime_with_named_time_zone" begin
        session(tc.driver) do sess
            result = run(sess, "CYPHER runtime=interpreted WITH \$x AS x " *
                                 "RETURN x.year, x.month, x.day, " *
                                 "x.hour, x.minute, x.second, x.nanosecond, x.timezone", x=ZonedDateTime(DateTime(1976, 6, 13, 12, 34, 56, 789), tz"America/Los_Angeles"))
            year, month, day, hour, minute, second, nanosecond, tz = values(single(result))
            @test year == 1976
            @test month == 6
            @test day == 13
            @test hour == 12
            @test minute == 34
            @test second == 56
            @test nanosecond == 789000000
            @test tz == "America/Los_Angeles"
        end
    end

    @testset "test_datetimewrapper_with_named_time_zone" begin
        session(tc.driver) do sess
            result = run(sess, "CYPHER runtime=interpreted WITH \$x AS x " *
                                 "RETURN x.year, x.month, x.day, " *
                                 "x.hour, x.minute, x.second, x.nanosecond, x.timezone", x=DateTimeWrapper(1976, 6, 13, 12, 34, 56, 789012345, timezone="US/Pacific"))
            year, month, day, hour, minute, second, nanosecond, tz = values(single(result))
            @test year == 1976
            @test month == 6
            @test day == 13
            @test hour == 12
            @test minute == 34
            @test second == 56
            @test nanosecond == 789012345
            @test tz == "US/Pacific"
        end
    end

    @testset "test_duration" begin
        session(tc.driver) do sess
            result = run(sess, "CYPHER runtime=interpreted WITH \$x AS x " *
                                 "RETURN x.months, x.days, x.seconds, x.microsecondsOfSecond", x=(Year(1) + Month(2) + Day(3) + Hour(4) + Minute(5) + Second(6) + Microsecond(789012)))
            months, days, seconds, microseconds = values(single(result))
            @test months == 14
            @test days == 3
            @test seconds == 14706
            @test microseconds == 789012
        end
    end

    @testset "test_duration_wrapper" begin
        session(tc.driver) do sess
            result = run(sess, "CYPHER runtime=interpreted WITH \$x AS x " *
                                 "RETURN x.months, x.days, x.seconds, x.microsecondsOfSecond", x=DurationWrapper(years=1, months=2, days=3, hours=4, minutes=5, seconds=6, nanoseconds=789012000))
            months, days, seconds, microseconds = values(single(result))
            @test months == 14
            @test days == 3
            @test seconds == 14706
            @test microseconds == 789012
        end
    end

    @testset "test_duration_array" begin
        session(tc.driver) do sess
            data = [(Year(1) + Month(2) + Day(3) + Hour(4) + Minute(5) + Second(6)), (Year(9) + Month(8) + Day(7) + Hour(6) + Minute(5) + Second(4))]
            v = write_transaction(sess, run_and_rollback, "CREATE (a {x:\$x}) RETURN a.x", x=data)
            @test v == data
        end
    end

    @testset "test_mixed_array" begin
        session(tc.driver) do sess
            data = [Date(1976, 6, 13), (Year(9) + Month(8) + Day(7) + Hour(6) + Minute(5) + Second(4))]
            @test_throws Neo4jBolt.JuliaBolt.CypherTypeError write_transaction(sess, run_and_rollback, "CREATE (a {x:\$x}) RETURN a.x", x=data)
        end
    end
    
    teardown(tc.driver)
end




@testset "TemporalTypeOutputTestCase" begin
    tc = setup()

    @testset "test_date" begin
        session(tc.driver) do sess
            result = run(sess, "RETURN date('1976-06-13')")
            v = value(single(result))
            @test isa(v, Date)
            @test v == Date(1976, 6, 13)
        end
    end

    @testset "test_date_wrapper" begin
        session(tc.driver) do sess
            result = run(sess, "RETURN date('1976-06-13')", use_julia_dates=false)
            v = value(single(result))
            @test isa(v, DateWrapper)
            @test v == DateWrapper(1976, 6, 13)
        end
    end

    @testset "test_whole_second_time" begin
        session(tc.driver) do sess
            result = run(sess, "RETURN time('12:34:56')")
            v = value(single(result))
            @test isa(v, Time)
            @test v == Time(12, 34, 56)
        end
    end

    @testset "test_nanosecond_resolution_time" begin
        session(tc.driver) do sess
            result = run(sess, "RETURN time('12:34:56.789012345')")
            v = value(single(result))
            @test isa(v, Time)
            @test v == Time(12, 34, 56, 789, 012, 345)
        end
    end
       
    @testset "test_time_wrapper_with_numeric_time_offset" begin
        session(tc.driver) do sess
            result = run(sess, "RETURN time('12:34:56.789012345+0130')", use_julia_dates=false)
            v = value(single(result))
            @test isa(v, TimeWrapper)
            @test v == TimeWrapper(12, 34, 56, 789012345, timezone=90)
        end
    end

    @testset "test_whole_second_time_wrapper" begin
        session(tc.driver) do sess
            result = run(sess, "RETURN localtime('12:34:56')", use_julia_dates=false)
            v = value(single(result))
            @test isa(v, TimeWrapper)
            @test v == TimeWrapper(12, 34, 56)
        end
    end
 
    @testset "test_whole_second_datetime" begin
        session(tc.driver) do sess
            result = run(sess, "RETURN datetime('1976-06-13T12:34:56')")
            v = value(single(result))
            @test isa(v, ZonedDateTime)
            @test v == ZonedDateTime(DateTime(1976, 6, 13, 12, 34, 56), tz"UTC")
        end
    end

    @testset "test_nanosecond_resolution_datetime" begin
        session(tc.driver) do sess
            result = run(sess, "RETURN datetime('1976-06-13T12:34:56.789012345')", use_julia_dates=false)
            v = value(single(result))
            @test isa(v, DateTimeWrapper)
            @test v == DateTimeWrapper(1976, 6, 13, 12, 34, 56, 789012345, timezone=0)
        end
    end

    @testset "test_datetime_wrapper_with_numeric_time_offset" begin
        session(tc.driver) do sess
            result = run(sess, "RETURN datetime('1976-06-13T12:34:56.789012345+01:30')", use_julia_dates=false)
            v = value(single(result))
            @test isa(v, DateTimeWrapper)
            @test v == DateTimeWrapper(1976, 6, 13, 12, 34, 56, 789012345, timezone=90)
        end
    end
    
    @testset "test_zoned_datetime_with_numeric_time_offset" begin
        session(tc.driver) do sess
            result = run(sess, "RETURN datetime('1976-06-13T12:34:56.789+01:30')")
            v = value(single(result))
            @test isa(v, ZonedDateTime)
            @test v == ZonedDateTime(DateTime(1976, 6, 13, 12, 34, 56, 789), FixedTimeZone("TZ", 90*60))
        end
    end

    @testset "test_zoned_datetime_with_named_time_zone" begin
        session(tc.driver) do sess
            result = run(sess,"RETURN datetime('1976-06-13T12:34:56.789012345[Europe/London]')")
            v = value(single(result))
            @test isa(v, ZonedDateTime)
            @test v == ZonedDateTime(DateTime(1976, 6, 13, 12, 34, 56, 789), tz"Europe/London")
        end
    end

    @testset "test_datetime_wrapper_with_named_time_zone" begin
        session(tc.driver) do sess
            result = run(sess, "RETURN datetime('1976-06-13T12:34:56.789012345[Europe/London]')", use_julia_dates=false)
            v = value(single(result))
            @test isa(v, DateTimeWrapper)
            @test v == DateTimeWrapper(1976, 6, 13, 12, 34, 56, 789012345, timezone="Europe/London")
        end
    end

    @testset "test_whole_second_localdatetime" begin
        session(tc.driver) do sess
            result = run(sess,"RETURN localdatetime('1976-06-13T12:34:56')")
            v = value(single(result))
            @test isa(v, DateTime)
            @test v == DateTime(1976, 6, 13, 12, 34, 56)
        end
    end

    @testset "test_nanosecond_resolution_localdatetime_wrapper" begin
        session(tc.driver) do sess
            result = run(sess, "RETURN localdatetime('1976-06-13T12:34:56.789012345')", use_julia_dates=false)
            v = value(single(result))
            @test isa(v, DateTimeWrapper)
            @test v == DateTimeWrapper(1976, 6, 13, 12, 34, 56, 789012345)
        end
    end

    @testset "test_duration_compound_period" begin
        session(tc.driver) do sess
            result = run(sess, "RETURN duration('P1Y2M3DT4H5M6.789S')")
            v = value(single(result))
            @test isa(v, Dates.CompoundPeriod)
            @test v == Year(1) + Month(2) + Day(3) + Hour(4) + Minute(5) + Second(6) + Millisecond(789)
        end
    end

     @testset "test_duration_wrapper" begin
        session(tc.driver) do sess
            result = run(sess, "RETURN duration('P1Y2M3DT4H5M6.789S')", use_julia_dates=false)
            v = value(single(result))
            @test isa(v, DurationWrapper)
            @test v == DurationWrapper(1,2,3,4,5,6,789000000)
        end
    end   

    @testset "test_nanosecond_resolution_duration_compound_period" begin
        session(tc.driver) do sess
            result = run(sess, "RETURN duration('P1Y2M3DT4H5M6.789123456S')")
            v = value(single(result))
            @test isa(v, Dates.CompoundPeriod)
            @test v == Year(1) + Month(2) + Day(3) + Hour(4) + Minute(5) + Second(6) + Nanosecond(789123456)
        end
    end

     @testset "test_nanosecond_resolution_duration_wrapper" begin
        session(tc.driver) do sess
            result = run(sess, "RETURN duration('P1Y2M3DT4H5M6.789123456S')", use_julia_dates=false)
            v = value(single(result))
            @test isa(v, DurationWrapper)
            @test v == DurationWrapper(1,2,3,4,5,6,789123456)
        end
    end   
                
    teardown(tc.driver)
end