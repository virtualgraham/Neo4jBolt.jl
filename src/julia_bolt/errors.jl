#=

JuliaBolt is ported from Neobolt: Neo4j Bolt connector for Python
https://github.com/neo4j-drivers/neobolt

Original notices:

# Copyright (c) 2002-2018 "Neo4j,"
# Neo4j Sweden AB [http://neo4j.com]
#
# This file is part of Neo4j.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

=#

#####################################
#              Errors               #
#####################################

abstract type JuliaBoltError <: Exception end

# Errors -------------------------- #

abstract type AbstractSecurityError <: JuliaBoltError end

# Errors -------------------------- #

abstract type AbstractCypherError <: JuliaBoltError end

# Errors -------------------------- #

abstract type AbstractTransientError <: AbstractCypherError end

# Errors -------------------------- #

abstract type AbstractClientError <: AbstractCypherError end

# Errors -------------------------- #

struct ProtocolError <: JuliaBoltError 
    message
    code
    classification
    category
    title
    metadata
end

# Errors -------------------------- #

struct ServiceUnavailable <: JuliaBoltError 
    message
    code
    classification
    category
    title
    metadata
end

# Errors -------------------------- #

struct ConnectionExpired <: JuliaBoltError 
    message
    code
    classification
    category
    title
    metadata
end

# Errors -------------------------- #

struct SecurityError <: AbstractSecurityError 
    message
    code
    classification
    category
    title
    metadata
end

# Errors -------------------------- #

struct CypherError <: AbstractCypherError 
    message
    code
    classification
    category
    title
    metadata
end

# Errors -------------------------- #

struct ClientError <: AbstractClientError 
    message
    code
    classification
    category
    title
    metadata
end

# Errors -------------------------- #

struct DatabaseError <: AbstractCypherError 
    message
    code
    classification
    category
    title
    metadata
end

# Errors -------------------------- #

struct TransientError <: AbstractTransientError 
    message
    code
    classification
    category
    title
    metadata
end

# Errors -------------------------- #

struct DatabaseUnavailableError <: AbstractTransientError 
    message
    code
    classification
    category
    title
    metadata
end

# Errors -------------------------- #

struct ConstraintError <: AbstractClientError 
    message
    code
    classification
    category
    title
    metadata
end

# Errors -------------------------- #

struct CypherSyntaxError <: AbstractClientError 
    message
    code
    classification
    category
    title
    metadata
end

# Errors -------------------------- #

struct CypherTypeError <: AbstractClientError 
    message
    code
    classification
    category
    title
    metadata
end

# Errors -------------------------- #

struct NotALeaderError <: AbstractClientError 
    message
    code
    classification
    category
    title
    metadata
end

# Errors -------------------------- #

struct Forbidden <: AbstractSecurityError 
    message
    code
    classification
    category
    title
    metadata
end

# Errors -------------------------- #

struct ForbiddenOnReadOnlyDatabaseError <: AbstractSecurityError 
    message
    code
    classification
    category
    title
    metadata
end

# Errors -------------------------- #

struct AuthError <: AbstractSecurityError 
    message
    code
    classification
    category
    title
    metadata
end

# Errors -------------------------- #

const client_errors = Dict(
    # ConstraintError
    "Neo.ClientError.Schema.ConstraintValidationFailed" => ConstraintError,
    "Neo.ClientError.Schema.ConstraintViolation" => ConstraintError,
    "Neo.ClientError.Statement.ConstraintVerificationFailed" => ConstraintError,
    "Neo.ClientError.Statement.ConstraintViolation" => ConstraintError,

    # CypherSyntaxError
    "Neo.ClientError.Statement.InvalidSyntax" => CypherSyntaxError,
    "Neo.ClientError.Statement.SyntaxError" => CypherSyntaxError,

    # CypherTypeError
    "Neo.ClientError.Procedure.TypeError" => CypherTypeError,
    "Neo.ClientError.Statement.InvalidType" => CypherTypeError,
    "Neo.ClientError.Statement.TypeError" => CypherTypeError,

    # Forbidden
    "Neo.ClientError.General.ForbiddenOnReadOnlyDatabase" => ForbiddenOnReadOnlyDatabaseError,
    "Neo.ClientError.General.ReadOnly" => Forbidden,
    "Neo.ClientError.Schema.ForbiddenOnConstraintIndex" => Forbidden,
    "Neo.ClientError.Schema.IndexBelongsToConstraint" => Forbidden,
    "Neo.ClientError.Security.Forbidden" => Forbidden,
    "Neo.ClientError.Transaction.ForbiddenDueToTransactionType" => Forbidden,

    # AuthError
    "Neo.ClientError.Security.AuthorizationFailed" => AuthError,
    "Neo.ClientError.Security.Unauthorized" => AuthError,

    # NotALeaderError
    "Neo.ClientError.Cluster.NotALeader" => NotALeaderError
)

# Errors -------------------------- #

const transient_errors = Dict(
    # DatabaseUnavailableError
    "Neo.TransientError.General.DatabaseUnavailable" => DatabaseUnavailableError
)

# Errors -------------------------- #

function hydrate_cypher_error(metadata::Dict)
    if haskey(metadata, "message")
        message = metadata["message"]
    else
        message = "An unknown error occurred."
    end

    if haskey(metadata, "code")
        code = metadata["code"]
    else
        code = "Neo.DatabaseError.General.UnknownError"
    end

    classification, category, title = "DatabaseError", "General", "UnknownError"  
    try _, classification, category, title = split(code, ".") catch end

    error_type = extract_error_type(classification, code)

    return error_type(message, code, classification, category, title, metadata)
end

# Errors -------------------------- #

function extract_error_type(classification::AbstractString, code::AbstractString)
    if classification == "ClientError"
        if haskey(client_errors, code)
            client_errors[code]
        else
            return ClientError
        end
    elseif classification == "TransientError"
        if haskey(transient_errors, code)
            transient_errors[code]
        else
            return TransientError
        end
    elseif classification == "DatabaseError"
        return DatabaseError
    else
        return CypherError
    end
end

# Errors -------------------------- #
# # # # # # # # # # # # # # # # # # #