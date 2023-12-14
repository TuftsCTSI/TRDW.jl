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
        anatomic_site_concept_id => omop.anatomic_site_concept_id)
end

specimen(match...) =
    specimen().filter(concept_matches($match))

end
