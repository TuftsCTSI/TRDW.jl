@funsql begin

visit() = begin
    from(visit_occurrence)
    as(omop)
    define(
        domain_id => "Visit",
        occurrence_id => omop.visit_occurrence_id,
        person_id => omop.person_id,
        concept_id => omop.visit_concept_id,
        datetime => omop.visit_start_datetime,
        end_datetime => omop.visit_end_datetime,
        type_concept_id => omop.visit_type_concept_id,
        provider_id => omop.provider_id,
        care_site_id => omop.care_site_id,
        admitted_from_concept_id => omop.admitted_from_concept_id,
        discharged_to_concept_id => omop.discharged_to_concept_id)
end

visit(match...) =
    visit().filter(concept_matches($match))

end
