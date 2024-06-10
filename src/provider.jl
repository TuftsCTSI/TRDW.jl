@funsql begin

provider() = begin
    from(provider)
    define(is_preepic => provider_id > 1000000000)
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
end

provider(match...) =
    provider().filter(concept_matches($match))

specialty_concept() = if_defined_scalar(provider,
                                        provider.omop.specialty_concept_id,
	                                    omop.specialty_concept_id)
specialty_isa(args...) =
    category_isa($Specialty, $args, specialty_concept())

end
