@funsql begin

drug() = begin
    from(drug_exposure)
    as(omop)
    define(
        domain_id => "Drug",
        occurrence_id => omop.drug_exposure_id,
        person_id => omop.person_id,
        concept_id => omop.drug_concept_id,
        datetime => omop.drug_exposure_start_datetime,
        end_datetime => omop.drug_exposure_end_datetime,
        verbatim_end_date => omop.verbatim_end_date,
        type_concept_id => omop.drug_type_concept_id,
        stop_reason => omop.stop_reason,
        refills => omop.refills,
        quantity => omop.quantity,
        days_supply => omop.days_supply,
        sig => omop.sig,
        route_concept_id => omop.route_concept_id,
        lot_number => omop.lot_number,
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
        route_concept => concept(),
        route_concept_id == route_concept.concept_id,
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

drug(match...) =
    drug().filter(concept_matches($match))

end
