@funsql begin

condition() = begin
    from(condition_occurrence)
    left_join(
        condition_source_concept => from(concept),
        condition_source_concept_id == condition_source_concept.concept_id,
        optional = true)
    as(omop)
    define(
        domain_id => "Condition",
        occurrence_id => omop.condition_occurrence_id,
        person_id => omop.person_id,
        concept_id => omop.condition_concept_id,
        icd_concept_id =>
            case(in(omop.condition_source_concept.vocabulary_id, "ICD9CM", "ICD10CM"), omop.condition_source_concept_id),
        datetime => omop.condition_start_datetime,
        end_datetime => omop.condition_end_datetime,
        type_concept_id => omop.condition_type_concept_id,
        status_concept_id => omop.condition_status_concept_id,
        stop_reason => omop.stop_reason,
        provider_id => omop.provider_id,
        visit_occurrence_id => omop.visit_occurrence_id)
    join(
        person => person(),
        person_id == person.person_id,
        optional = true)
    join(
        concept => concept(),
        concept_id == concept.concept_id,
        optional = true)
    left_join(
        icd_concept => concept(),
        icd_concept_id == icd_concept.concept_id,
        optional = true)
    left_join(
        type_concept => concept(),
        type_concept_id == type_concept.concept_id,
        optional = true)
    left_join(
        status_concept => concept(),
        status_concept_id == status_concept.concept_id,
        optional = true)
    left_join(
        provider => provider(),
        provider_id == provider.provider_id,
        optional = true)
    left_join(
        visit => visit(),
        visit_occurrence_id == visit.occurrence_id,
        optional = true)
end

condition(match...) =
    condition().filter(concept_matches($match))

end
