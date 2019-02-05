const version = v"0.1.0"

# Auth
const TRUST_ON_FIRST_USE = 0  # Deprecated
const TRUST_SIGNED_CERTIFICATES = 1  # Deprecated
const TRUST_ALL_CERTIFICATES = 2
const TRUST_CUSTOM_CA_SIGNED_CERTIFICATES = 3
const TRUST_SYSTEM_CA_SIGNED_CERTIFICATES = 4
const TRUST_DEFAULT = TRUST_ALL_CERTIFICATES

# Connection Pool Management
const INFINITE = -1
const DEFAULT_MAX_CONNECTION_LIFETIME = 3600  # 1h
const DEFAULT_MAX_CONNECTION_POOL_SIZE = 100
const DEFAULT_CONNECTION_TIMEOUT = 5.0  # 5s

# Connection Settings
const DEFAULT_CONNECTION_ACQUISITION_TIMEOUT = 60  # 1m

# Routing settings
const DEFAULT_MAX_RETRY_TIME = 30.0  # 30s

const LOAD_BALANCING_STRATEGY_LEAST_CONNECTED = 0
const LOAD_BALANCING_STRATEGY_ROUND_ROBIN = 1
const DEFAULT_LOAD_BALANCING_STRATEGY = LOAD_BALANCING_STRATEGY_LEAST_CONNECTED

# Client name
const DEFAULT_USER_AGENT = "juliabolt/$(string(version)) Julia/$(string(VERSION)) ($(string(Sys.KERNEL)))"
    
const default_config = Dict(
    "auth" => nothing,  # provide your own authentication token such as {"username", "password"}
    "encrypted" => nothing,  # default to have encryption enabled if ssl is available on your platform
    "trust" => TRUST_DEFAULT,
    "der_encoded_server_certificate" => nothing,

    "user_agent" => DEFAULT_USER_AGENT,

    # Connection pool management
    "max_connection_lifetime" => DEFAULT_MAX_CONNECTION_LIFETIME,
    "max_connection_pool_size" => DEFAULT_MAX_CONNECTION_POOL_SIZE,
    "connection_acquisition_timeout" => DEFAULT_CONNECTION_ACQUISITION_TIMEOUT,

    # Connection settings:
    "connection_timeout" => DEFAULT_CONNECTION_TIMEOUT,
    "keep_alive" => nothing,

    # Routing settings:
    "max_retry_time" => DEFAULT_MAX_RETRY_TIME,
    "load_balancing_strategy" => DEFAULT_LOAD_BALANCING_STRATEGY,
)
