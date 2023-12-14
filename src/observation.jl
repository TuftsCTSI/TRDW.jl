@funsql begin

observation() = begin
    from(observation)
    as(omop)
    define(
        domain_id => "Observation",
        occurrence_id => omop.observation_id,
        concept_id => omop.observation_concept_id,
        datetime => omop.observation_datetime,
        type_concept_id => omop.observation_type_concept_id,
        value_as_number => omop.value_as_number,
        value_as_string => omop.value_as_string,
        value_as_concept_id => omop.value_as_concept_id,
        qualifier_concept_id => omop.qualifier_concept_id,
        unit_concept_id => omop.unit_concept_id,
        provider_id => omop.provider_id,
        visit_occurrence_id => omop.visit_occurrence_id)
end

observation(match...) =
    observation().filter(concept_matches($match))

end
