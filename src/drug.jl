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
end

drug(match...) =
    drug().filter(concept_matches($match))

end
