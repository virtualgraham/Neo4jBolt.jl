using Dates
using TimeZones


struct DateWrapper
    days::Integer
end

Base.:(==)(x::DateWrapper, y::DateWrapper) = x.days == y.days

function DateWrapper(y, m, d)
    days = (Date(y,m,d)-Date(1970,1,1)).value
    return DateWrapper(days)
end

# timezone_offset here is in seconds
struct TimeWrapper
    nanoseconds::Integer
    timezone_offset::Union{Integer, Nothing}
end

Base.:(==)(x::TimeWrapper, y::TimeWrapper) = x.nanoseconds == y.nanoseconds && x.timezone_offset == y.timezone_offset

# if o is an integer then it is timezone_offset in mins
function TimeWrapper(h, m, s, n=0; timezone=nothing) 
    nanoseconds = n + s*1e+9 + m*6e+10 + h*3.6e+12
    return TimeWrapper(nanoseconds, if isa(timezone, Number) Integer(floor(timezone*60)) else timezone end)
end

TimeWrapper(nanoseconds) = TimeWrapper(nanoseconds, nothing)


struct DateTimeWrapper
    seconds::Integer
    nanoseconds::Integer
    timezone_offset::Union{Integer, Nothing}
    timezone_name::Union{String, Nothing}
end

Base.:(==)(x::DateTimeWrapper, y::DateTimeWrapper) = x.seconds == y.seconds && x.nanoseconds == y.nanoseconds && x.timezone_offset == y.timezone_offset && x.timezone_name == y.timezone_name

DateTimeWrapper(seconds, nanoseconds) = DateTimeWrapper(seconds, nanoseconds, nothing, nothing)
DateTimeWrapper(seconds, nanoseconds, timezone::Integer) = DateTimeWrapper(seconds, nanoseconds, timezone, nothing)
DateTimeWrapper(seconds, nanoseconds, timezone::String) = DateTimeWrapper(seconds, nanoseconds, nothing, timezone)

function DateTimeWrapper(y,m,d,h,i,s=0,n=0;timezone=nothing)
    millisecond = (DateTime(y,m,d,h,i,s)-DateTime(1970,1,1)).value
    seconds = Integer(floor(millisecond/1000))
    nanoseconds = n

    if timezone == nothing
        return DateTimeWrapper(seconds, nanoseconds, nothing, nothing)
    elseif isa(timezone, String)
        return DateTimeWrapper(seconds, nanoseconds, nothing, timezone) 
    elseif isa(timezone, Number)
        return DateTimeWrapper(seconds, nanoseconds, Integer(floor(timezone*60)), nothing) 
    else
        throw(ErrorException("Not a valid DateTime"))
    end
end


struct DurationWrapper
    months::Integer
    days::Integer
    seconds::Integer
    nanoseconds::Integer
end

Base.:(==)(x::DurationWrapper, y::DurationWrapper) = x.months == y.months && x.days == y.days && x.seconds == y.seconds && x.nanoseconds == y.nanoseconds


function DurationWrapper(years, months, days, hours, minutes, seconds=0, nanoseconds=0)
    m = months + years * 12
    d = days
    s = seconds + hours * 3600 + minutes * 60
    n = nanoseconds
    return DurationWrapper(m, d, s, n)
end

DurationWrapper(;years=0, months=0, days=0, hours=0, minutes=0, seconds=0, nanoseconds=0) = DurationWrapper(years, months, days, hours, minutes, seconds, nanoseconds)


function hydrate_date(days; use_julia_dates=true)
    if use_julia_dates
        return Date(1970,1,1) + Dates.Day(days)
    else
        return DateWrapper(days)
    end
end


function dehydrate_date(value::Date)
    return Structure(UInt8('D'), Any[(value-Date(1970,1,1)).value])
end


function dehydrate_date(value::DateWrapper)
    return Structure(UInt8('D'), Any[value.days])
end


# Julia Times do not support time zones
function hydrate_time(nanoseconds::Integer, timezone::Union{Integer, Nothing}=nothing; use_julia_dates=true)
    if use_julia_dates
        return Time(Dates.Nanosecond(nanoseconds))
    else
        return TimeWrapper(nanoseconds, timezone)
    end
end


function dehydrate_time(value::Time)
    return Structure(UInt8('t'), Any[value.instant.value])
end


function dehydrate_time(value::TimeWrapper)
    if value.timezone_offset != nothing
        return Structure(UInt8('T'), Any[value.nanoseconds, value.timezone_offset])
    else
        return Structure(UInt8('t'), Any[value.nanoseconds])
    end
end


# Julia DateTimes do not support nanosecond accuracy time
function hydrate_datetime(seconds::Integer, nanoseconds::Integer, timezone::Union{String, Integer, Nothing}=nothing; use_julia_dates=true)
    if use_julia_dates
        date_time =  DateTime(1970,1,1) + Dates.Second(seconds) + Dates.Nanosecond(nanoseconds)
        if timezone == nothing
            return date_time
        elseif isa(timezone, String)
            return ZonedDateTime(date_time, TimeZone(timezone))
        else
            return ZonedDateTime(date_time, FixedTimeZone("Time Zone", timezone))
        end
    else
        if timezone == nothing
            return DateTimeWrapper(seconds, nanoseconds, nothing, nothing)
        elseif isa(timezone, String)
            return DateTimeWrapper(seconds, nanoseconds, nothing, timezone)
        else
            return DateTimeWrapper(seconds, nanoseconds, timezone, nothing)
        end
    end
end


function dehydrate_datetime(value::DateTime)
    millisecond = (value-DateTime(1970,1,1)).value # miliseconds
    seconds = Integer(floor(millisecond/1000.0))
    nanoseconds = (millisecond % 1000) * 1000000
    return Structure(UInt8('d'), Any[seconds, nanoseconds])
end


function dehydrate_datetime(value::ZonedDateTime)
    d = TimeZones.localtime(value)
    millisecond = (d-DateTime(1970,1,1)).value # miliseconds
    seconds = Integer(floor(millisecond/1000.0))
    nanoseconds = (millisecond % 1000) * 1000000
    if isa(value.timezone, FixedTimeZone)
        return Structure(UInt8('F'), Any[seconds, nanoseconds, value.timezone.offset.std.value])
    else
        return Structure(UInt8('f'), Any[seconds, nanoseconds, string(value.timezone.name)])
    end
end


function dehydrate_datetime(value::DateTimeWrapper)
    if value.timezone_offset != nothing
        return Structure(UInt8('F'), Any[value.seconds, value.nanoseconds, value.timezone_offset])
    elseif value.timezone_name != nothing
        return Structure(UInt8('f'), Any[value.seconds, value.nanoseconds, value.timezone_name])
    else
        return Structure(UInt8('d'), Any[value.seconds, value.nanoseconds])
    end

end


function hydrate_duration(months, days, seconds, nanoseconds; use_julia_dates=true)
    if use_julia_dates
        return Month(months) + Day(days) + Second(seconds) + Nanosecond(nanoseconds)
    else
        return DurationWrapper(months, days, seconds, nanoseconds)
    end
end

function tons(c::Dates.CompoundPeriod)
    s::Float64 = 0.0
    for p in c.periods
        v = Dates.tons(p)
        s += v
    end
    return s
end

function dehydrate_duration(value::Dates.CompoundPeriod)
    value = canonicalize(value)

    months = 0
    periods = Dates.Period[]
    
    for p in value.periods
        if isa(p, Dates.Month) 
            months += p.value
        elseif isa(p, Dates.Year)
            months += p.value * 12
        else
            push!(periods, p)
        end
    end

    value = Dates.CompoundPeriod(periods)
    v = tons(value)
    
    nanoseconds = v % 1000000000
    v = (v - nanoseconds) / 1000000000
    seconds = v % 86400
    v = (v - seconds) / 86400
    days = v
    
    return Structure(UInt8('E'), Any[Integer(months), Integer(days), Integer(seconds), Integer(nanoseconds)])
end

dehydrate_duration(value::Period) = dehydrate_duration(Dates.CompoundPeriod(value))

function dehydrate_duration(value::DurationWrapper) 
    return Structure(UInt8('E'), Any[value.months, value.days, value.seconds, value.nanoseconds])
end



dehydrate(value::DateWrapper) = dehydrate_date(value)
dehydrate(value::TimeWrapper) = dehydrate_time(value)
dehydrate(value::DateTimeWrapper) = dehydrate_datetime(value)
dehydrate(value::DurationWrapper) = dehydrate_duration(value)
dehydrate(value::Dates.Date) = dehydrate_date(value)
dehydrate(value::Dates.Time) = dehydrate_time(value)
dehydrate(value::Dates.DateTime) = dehydrate_datetime(value)
dehydrate(value::ZonedDateTime) = dehydrate_datetime(value)
dehydrate(value::Dates.CompoundPeriod) = dehydrate_duration(value)
dehydrate(value::Period) = dehydrate_date(value)


function temporal_hydration_functions()
    return Dict(
        UInt8('D') => (psh, values)->hydrate_date(values..., use_julia_dates=psh.use_julia_dates),
        UInt8('T') => (psh, values)->hydrate_time(values..., use_julia_dates=psh.use_julia_dates),
        UInt8('t') => (psh, values)->hydrate_time(values..., use_julia_dates=psh.use_julia_dates),
        UInt8('F') => (psh, values)->hydrate_datetime(values..., use_julia_dates=psh.use_julia_dates),
        UInt8('f') => (psh, values)->hydrate_datetime(values..., use_julia_dates=psh.use_julia_dates),
        UInt8('d') => (psh, values)->hydrate_datetime(values..., use_julia_dates=psh.use_julia_dates),
        UInt8('E') => (psh, values)->hydrate_duration(values..., use_julia_dates=psh.use_julia_dates)
    )
end