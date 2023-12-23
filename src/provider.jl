@funsql begin

provider() = begin
    from(provider)
    as(omop)
    define(
        provider_id => omop.provider_id,
        provider_name => omop.provider_name,
        npi => omop.npi,
        dea => omop.dea,
        concept_id => coalesce(omop.specialty_concept_id, 0),
        care_site_id => omop.care_site_id,
        year_of_birth => omop.year_of_birth,
        gender_concept_id => coalesce(omop.gender_concept_id, 0))
    left_join(
        concept => concept(),
        concept_id == concept.concept_id,
        optional = true)
    left_join(
        care_site => care_site(),
        care_site_id == care_site.care_site_id,
        optional = true)
    left_join(
        gender_concept => concept(),
        concept_id == gender_concept.concept_id,
        optional = true)
    cross_join(
        ext => begin
            # computed variables
            select(
                is_historical => :ID > 1000000000)
            bind(
                :ID => omop.provider_id)
        end)
end

provider(match...) =
    provider().filter(concept_matches($match))

end
