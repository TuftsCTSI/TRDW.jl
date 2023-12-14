@funsql begin

procedure() = begin
    from(procedure_occurrence)
    as(omop)
    define(
        domain_id => "Procedure",
        occurrence_id => omop.procedure_occurrence_id,
        person_id => omop.person_id,
        concept_id => omop.procedure_concept_id,
        datetime => omop.procedure_datetime,
        end_datetime => omop.procedure_end_datetime,
        type_concept_id => omop.procedure_type_concept_id,
        modifier_concept_id => omop.modifier_concept_id,
        quantity => omop.quantity,
        provider_id => omop.provider_id,
        visit_occurrence_id => omop.visit_occurrence_id)
end

procedure(match...) =
    procedure().filter(concept_matches($match))

end
