abstract type Neo4jBoltError <: Exception end
abstract type AbstractSessionError <: Neo4jBoltError end

struct SessionError <: AbstractSessionError 
    session
    args
end

struct SessionExpired <: AbstractSessionError 
    session
    args
end

struct TransactionError <: Neo4jBoltError 
    transaction
    args
end

struct ProtocolError <: Neo4jBoltError 
    driver
    args
end