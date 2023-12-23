@funsql begin

care_site() = begin
    from(care_site)
    as(omop)
    define(
        care_site_id => omop.care_site_id,
        care_site_name => omop.care_site_name,
        concept_id => coalesce(omop.place_of_service_concept_id, 0),
        location_id => omop.location_id)
    left_join(
        concept => concept(),
        concept_id == concept.concept_id,
        optional = true)
    left_join(
        location => location(),
        location_id == location.location_id,
        optional = true)
    cross_join(
        ext => begin
            # computed variables
            select(
                is_historical => :ID > 1000000000,
                is_clinic => startswith(:NAME, "CC"))
            bind(
                :ID => omop.care_site_id,
                :NAME => omop.care_site_name)
        end)
end

end
