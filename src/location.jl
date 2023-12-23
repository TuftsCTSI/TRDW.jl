@funsql begin

location() = begin
    from(location)
    as(omop)
    define(
        omop.location_id,
        omop.address_1,
        omop.address_2,
        omop.city,
        omop.state,
        omop.zip,
        omop.county,
        omop.country_concept_id,
        omop.latitude,
        omop.longitude)
    left_join(
        country_concept => concept(),
        country_concept_id == country_concept.concept_id,
        optional = true)
end

end
