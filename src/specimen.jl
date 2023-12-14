@funsql begin

specimen() = begin
    from(specimen)
    as(omop)
    define(
        domain_id => "Specimen",
        occurrence_id => omop.specimen_id,
        person_id => omop.person_id,
        concept_id => omop.specimen_concept_id,
        type_concept_id => omop.specimen_type_concept_id,
        datetime => omop.specimen_datetime,
        quantity => omop.quantity,
        unit_concept_id => omop.unit_concept_id,
        site_concept_id => omop.anatomic_site_concept_id)
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
        site_concept => concept(),
        site_concept_id == site_concept.concept_id,
        optional = true)
end

specimen(match...) =
    specimen().filter(concept_matches($match))

end
