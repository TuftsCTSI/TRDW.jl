@funsql begin

provider() = begin
    from(provider)
    as(omop)
    define(
        provider_id => omop.provider_id,
        provider_name => omop.provider_name,
        npi => omop.npi,
        dea => omop.dea,
        concept_id => omop.specialty_concept_id,
        care_site_id => omop.care_site_id,
        year_of_birth => omop.year_of_birth,
        gender_concept_id => omop.gender_concept_id)
end

provider(match...) =
    provider().filter(concept_matches($match))

end
