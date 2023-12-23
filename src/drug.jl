@funsql begin

drug() = begin
    from(drug_exposure)
    as(omop)
    define(
        # event columns
        domain_id => "Drug",
        occurrence_id => omop.drug_exposure_id,
        person_id => omop.person_id,
        concept_id => omop.drug_concept_id,
        datetime => coalesce(omop.drug_exposure_start_datetime,
                             timestamp(omop.drug_exposure_start_date)),
        overlap_ending => coalesce(omop.drug_exposure_end_datetime,
                                   timestamp(omop.drug_exposure_end_date)),
        type_concept_id => omop.drug_type_concept_id,
        provider_id => omop.provider_id,
        visit_occurrence_id => omop.visit_occurrence_id,
        # domain specific columns
        omop.verbatim_end_date,
        omop.stop_reason,
        omop.refills,
        omop.quantity,
        omop.days_supply,
        omop.sig,
        omop.route_concept_id,
        omop.lot_number)
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
    cross_join(
        ext => begin
            # computed variables
            select(
                is_historical => :ID > 1500000000)
            bind(
                :ID => omop.drug_exposure_id)
        end)
end

drug(match...) =
    drug().filter(concept_matches($match))

drug_concept() = concept().filter(domain_id == "Drug")
drug_component_class() = drug_concept().filter(concept_class_id == "Component Class")
drug_dose_form_group() = drug_concept().filter(concept_class_id == "Dose Form Group")
drug_ingredient() = drug_concept().filter(concept_class_id == "Ingredient")
drug_pharmacologic_class() = drug_concept().filter(concept_class_id == "Pharmacologic Class")

component_class_isa(args...) = category_isa($ComponentClass, $args, omop.drug_concept_id)
dose_form_group_isa(args...) = category_isa($DoseFormGroup, $args, omop.drug_concept_id)
ingredient_isa(args...) = category_isa($Ingredient, $args, omop.drug_concept_id)

drug_ingredient_via_SNOMED(code, name) = begin
    concept(SNOMED($code, $name))
    concept_relatives("Subsumes",1:3)
    concept_relatives("SNOMED - RxNorm eq")
    filter(concept_class_id=="Ingredient")
    deduplicate(concept_id)
    filter_out_descendants()
end

drug_ingredient_via_NDFRT(code, name) = begin
    concept(NDFRT($code, $name))
    concept_relatives("Subsumes",1:3)
    concept_relatives("NDFRT - RxNorm eq")
    filter(concept_class_id=="Ingredient")
    deduplicate(concept_id)
    filter_out_descendants()
end

drug_ingredient_via_HemOnc(code, name) = begin
    concept(HemOnc($code, $name))
    concept_children()
    filter(concept_class_id=="Ingredient")
end

to_ingredient() = begin
    as(drug)
    join(concept_ancestor => from(concept_ancestor),
        concept_ancestor.descendant_concept_id == drug.concept_id)
    join(concept().filter(concept_class_id=="Ingredient"),
        concept_id == concept_ancestor.ancestor_concept_id)
    define(drug.person_id, drug.occurrence_id)
    partition(drug.concept_id, name="ancestors")
    filter(concept_ancestor.min_levels_of_separation ==
           ancestors.min(concept_ancestor.min_levels_of_separation))
    group(domain_id => "Drug", occurrence_id, person_id, concept_id)
end

end
