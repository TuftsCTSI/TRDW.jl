@funsql begin

care_site() = begin
    from(care_site)
    define(is_historical => care_site_id > 1000000000)
    define(is_clinic => (startswith(care_site_name, "CC")))
    define(is_mwh => (ilike(care_site_name, "melrose%")))
    define(is_lgh => (ilike(care_site_name, "lowell%")))
    define(is_tmc => (is_historical && !is_null(care_site_id)) ||
                     !(is_mwh || is_lgh || is_clinic))
end

join_care_site(ids...; carry=[]) = begin
    as(base)
    join(begin
        care_site()
        $(length(ids) == 0 ? @funsql(define()) :
            @funsql filter(is_descendant_concept(care_site_concept_id, $ids...)))
    end, base.care_site_id == care_site_id)
    define($([@funsql($n => base.$n) for n in carry]...))
end

end
