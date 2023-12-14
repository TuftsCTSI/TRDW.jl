@funsql begin

device() = begin
    from(device_exposure)
    as(omop)
    define(
        domain_id => "Device",
        occurrence_id => omop.device_exposure_id,
        person_id => omop.person_id,
        concept_id => omop.device_concept_id,
        datetime => omop.device_exposure_start_datetime,
        end_datetime => omop.device_exposure_end_datetime,
        type_concept_id => omop.device_type_concept_id,
        unique_device_id => omop.unique_device_id,
        production_id => omop.production_id,
        quantity => omop.quantity,
        unit_concept_id => omop.unit_concept_id,
        provider_id => omop.provider_id,
        visit_occurrence_id => omop.visit_occurrence_id)
end

device(match...) =
    device().filter(concept_matches($match))

end
