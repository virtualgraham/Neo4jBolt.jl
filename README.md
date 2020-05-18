# Neo4j Bolt Driver for Julia

Neo4jBolt.jl is a Julia port of the official [Neo4j Driver (1.7)](https://github.com/neo4j/neo4j-python-driver/tree/1.7). It supports Neo4j 3.0 and above using the fast binary Bolt protocal. The Bolt driver is designed to be much faster than the HTTP REST based driver. For a Neo4j Julia driver that supports HTTP and REST, see [Neo4j.jl](https://github.com/glesica/Neo4j.jl). 

## Todo

Encrypted SSL connections and cluster routing have not yet been implemented in this version.

## Getting Started with Neo4j

* [Introduction (The Neo4j Operations Manual v3.5)](https://neo4j.com/docs/operations-manual/current/introduction/)
* [Installation (The Neo4j Operations Manual v3.5)](https://neo4j.com/docs/operations-manual/current/installation/)

## Quick Examples

Here are a few usage examples. For a more extensive collection of examples see the integration tests in the test/integration directory of this repository.

### Run Cypher Statement

```
using Neo4jBolt  
      
driver = Neo4jBoltDriver("bolt://localhost:7687", auth=("neo4j", "password"))

session(driver) do sess
    result = run(sess, "UNWIND(RANGE(1, 10)) AS z RETURN z")
    for record in result
        println(record["z"])
    end
end
```


### Run Simple Transaction

```
using Neo4jBolt  
      
driver = Neo4jBoltDriver("bolt://localhost:7687", auth=("neo4j", "password"))

session(driver) do sess
    begin_transaction(sess) do tx
        result = run(tx, "CREATE (a:Person {name:'Alice'}) RETURN a")
        v = value(single(result))
        println(v.labels == Set(["Person"]))
        println(v.properties == Dict("name"=>"Alice"))
    end
end
```


### Unit of Work transactions

```
using Neo4jBolt  
      
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
```

## Implementation Details

### Dates and Times

By default Neo4j Date, Time and Duration types are returned as Julia Date, Time, DateTime, ZonedDateTime or Dates.CompoundPeriod types where appropriate. However, Julia Dates and Neo4j Dates are slightly incompatible. Specifically, Julia DateTimes do not support nanosecond time. Julia Times do not support TimeZones. Also Julia's TimeZone system is not entirely compatible with Neo4j's. As a workaround to these incompatibilities, you can choose to use the provided DateWrapper, TimeWrapper, DateTimeWrapper and DurationWrapper types instead. These wrap Neo4j's native representations in Julia struct types. To return these wrapper types from a query set `use_julia_dates=false` as a keyword argument to the `run` method.

```
# Here a ZonedDateTime is returned but the nanosecods are truncated

session(tc.driver) do sess
      result = run(sess,"RETURN datetime('1976-06-13T12:34:56.789012345[Europe/London]')")
      v = value(single(result))
      @test isa(v, ZonedDateTime)
      @test v == ZonedDateTime(DateTime(1976, 6, 13, 12, 34, 56, 789), tz"Europe/London")
end

# Here a DateTimeWrapper is returned and nanosecods are not truncated

session(tc.driver) do sess
      result = run(sess, "RETURN datetime('1976-06-13T12:34:56.789012345[Europe/London]')", use_julia_dates=false)
      v = value(single(result))
      @test isa(v, DateTimeWrapper)
      @test v == DateTimeWrapper(1976, 6, 13, 12, 34, 56, 789012345, timezone="Europe/London")
end
```
