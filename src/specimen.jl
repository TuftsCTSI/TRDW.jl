@funsql begin

specimen() = begin
    from(specimen)
    as(omop)
    define(
        # event columns
        domain_id => "Specimen",
        occurrence_id => omop.specimen_id,
        person_id => omop.person_id,
        concept_id => omop.specimen_concept_id,
        datetime => coalesce(omop.specimen_datetime,
                             timestamp(omop.specimen_date)),
        overlap_ending => missing,
        type_concept_id => omop.specimen_type_concept_id,
        provider_id => missing,
        visit_occurrence_id => missing,
        # domain specific columns
        omop.quantity,
        omop.unit_concept_id,
        status_concept_id => omop.disease_status_concept_id,
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
        status_concept => concept(),
        status_concept_id == status_concept.concept_id,
        optional = true)
    left_join(
        site_concept => concept(),
        site_concept_id == site_concept.concept_id,
        optional = true)
    cross_join(
        ext => begin
            # computed variables
            select(
                is_historical => :ID > 1500000000)
            bind(
                :ID => omop.specimen_id)
        end)
end

specimen(match...) =
    specimen().filter(concept_matches($match))

end
