@funsql begin

visit() = begin
    from(visit_occurrence)
    define(is_preepic => visit_occurrence_id > 1000000000)
    as(omop)
    define(
        # event columns
        domain_id => "Visit",
        occurrence_id => omop.visit_occurrence_id,
        person_id => omop.person_id,
        concept_id => omop.visit_concept_id,
        datetime => coalesce(omop.visit_start_datetime,
                             timestamp(omop.visit_start_date)),
        datetime_end => coalesce(omop.visit_end_datetime,
                                 timestamp(omop.visit_end_date)),
        type_concept_id => omop.visit_type_concept_id,
        provider_id => omop.provider_id,
        # domain specific columns
        omop.care_site_id,
        omop.admitted_from_concept_id,
        omop.discharged_to_concept_id,
        omop.preceding_visit_occurrence_id)
    join(
        person => person(),
        person_id == person.person_id,
        optional = true)
    join(
        concept => concept(),
        concept_id == concept.concept_id,
        optional = true)
    left_join(
        type_concept => concept(),
        type_concept_id == type_concept.concept_id,
        optional = true)
    left_join(
        provider => provider(),
        provider_id == provider.provider_id,
        optional = true)
    left_join(
        care_site => care_site(),
        omop.care_site_id == care_site.care_site_id,
        optional = true)
    left_join(
        admitted_from_concept => concept(),
        omop.admitted_from_concept_id == admitted_from_concept.concept_id,
        optional = true)
    left_join(
        discharged_to_concept => concept(),
        omop.discharged_to_concept_id == discharged_to_concept.concept_id,
        optional = true)
end

visit(cs) =
    visit().filter(isa($cs))

visit(cs::AbstractString) =
    visit().filter(visit_isa($cs))

end
