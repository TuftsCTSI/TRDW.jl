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
end

end
