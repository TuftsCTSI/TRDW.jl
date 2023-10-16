@funsql begin

location() = begin
    from(location)
    define(is_historical => location_id > 1000000000)
end

end
