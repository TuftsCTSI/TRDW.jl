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
        value_as_concept => concept(),
        value_as_concept_id == value_as_concept.concept_id,
        optional = true)
    left_join(
        qualifier_concept => concept(),
        qualifier_concept_id == qualifier_concept.concept_id,
        optional = true)
    left_join(
        unit_concept => concept(),
        unit_concept_id == unit_concept.concept_id,
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

observation(match...) =
    observation().filter(concept_matches($match))

end
