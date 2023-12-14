@funsql begin

care_site() = begin
    from(care_site)
    as(omop)
    define(
        care_site_id => omop.care_site_id,
        care_site_name => omop.care_site_name,
        concept_id => omop.place_of_service_concept_id,
        location_id => omop.location_id)
    define(is_historical => care_site_id > 1000000000)
    define(is_clinic => (startswith(care_site_name, "CC")))
    define(is_mwh => (ilike(care_site_name, "melrose%")))
    define(is_lgh => (ilike(care_site_name, "lowell%")))
    define(is_tmc => is_historical || !(is_mwh || is_lgh || is_clinic))
end

end
