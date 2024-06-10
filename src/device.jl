@funsql begin

device() = begin
    from(device_exposure)
    define(is_preepic => device_exposure_id > 1500000000)
    as(omop)
    define(
        # event columns
        domain_id => "Device",
        occurrence_id => omop.device_exposure_id,
        person_id => omop.person_id,
        concept_id => omop.device_concept_id,
        datetime => coalesce(omop.device_exposure_start_datetime,
                             timestamp(omop.device_exposure_start_date)),
        datetime_end => coalesce(omop.device_exposure_end_datetime,
                                 timestamp(omop.device_exposure_end_date)),
        type_concept_id => omop.device_type_concept_id,
        provider_id => omop.provider_id,
        visit_occurrence_id => omop.visit_occurrence_id,
        # domain specific columns
        omop.unique_device_id,
        omop.production_id,
        omop.quantity,
        omop.unit_concept_id)
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

device(match...) =
    device().filter(concept_matches($match))

end
