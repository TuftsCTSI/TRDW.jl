@funsql begin

measurement() = begin
    from(measurement)
    as(omop)
    define(
        domain_id => "Measurement",
        occurrence_id => omop.measurement_id,
        person_id => omop.person_id,
        concept_id => omop.measurement_concept_id,
        datetime => omop.measurement_datetime,
        type_concept_id => omop.measurement_type_concept_id,
        operator_concept_id => omop.operator_concept_id,
        value_as_number => omop.value_as_number,
        value_as_concept_id => omop.value_as_concept_id,
        unit_concept_id => omop.unit_concept_id,
        range_low => omop.range_low,
        range_high => omop.range_high,
        provider_id => omop.provider_id,
        visit_occurrence_id => omop.visit_occurrence_id)
end

measurement(match...) =
    measurement().filter(concept_matches($match))

end
