@funsql begin

care_site() = begin
    from(care_site)
    define(is_historical => care_site_id > 1000000000)
end

end
