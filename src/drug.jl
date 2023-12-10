@funsql begin

drug_exposure(match...) = begin
    from(drug_exposure)
    $(length(match) == 0 ? @funsql(define()) : @funsql(filter(drug_matches($match))))
    left_join(person => person(),
              person_id == person.person_id, optional=true)
    left_join(concept => concept(),
              drug_concept_id == concept.concept_id, optional=true)
    left_join(type_concept => concept(),
              drug_type_concept_id == type_concept.concept_id, optional=true)
    left_join(route_concept => concept(),
              route_concept_id == route_concept.concept_id, optional=true)
    left_join(source_concept => concept(),
              drug_source_concept_id == source_concept.concept_id, optional=true)
    left_join(visit => visit_occurrence(),
        visit_occurrence_id == visit_occurrence.visit_occurrence_id, optional = true)
    left_join(provider => provider(),
              provider.provider_id == provider.provider_id, optional=true)
    join(event => begin
        from(drug_exposure)
        define(
            table_name => "drug_exposure",
            concept_id => drug_concept_id,
            end_date => drug_exposure_end_date,
            is_historical => drug_exposure_id > 1500000000,
            start_date => drug_exposure_start_date,
            source_concept_id => drug_source_concept_id)
    end, drug_exposure_id == event.drug_exposure_id, optional = true)
end

drug_concept() = concept().filter(domain_id == "Drug")
drug_component_class() = drug_concept().filter(concept_class_id == "Component Class")
drug_dose_form_group() = drug_concept().filter(concept_class_id == "Dose Form Group")
drug_ingredient() = drug_concept().filter(concept_class_id == "Ingredient")
drug_pharmacologic_class() = drug_concept().filter(concept_class_id == "Pharmacologic Class")

component_class_isa(args...) = category_isa($ComponentClass, $args, drug_concept_id)
dose_form_group_isa(args...) = category_isa($DoseFormGroup, $args, drug_concept_id)
ingredient_isa(args...) = category_isa($Ingredient, $args, drug_concept_id)

drug_matches(match...) = concept_matches($match; match_prefix=drug)

drug_pivot(match...; event_total=true, person_total=true, roundup=true) = begin
    join_via_cohort(drug_exposure(), drug_exposure; match_prefix=drug, match=$match)
    pairing_pivot($match, drug, drug_exposure_id;
                  event_total=$event_total, person_total=$person_total, roundup=$roundup)
end

filter_cohort_on_drug(match...; exclude=nothing, also=nothing) =
    filter(exists(correlate_via_cohort(drug_exposure(), drug_exposure;
                                       match_prefix=drug, match=$match,
                                       exclude=$exclude, also=$also)))

join_cohort_on_drug(match...; exclude=nothing, carry=nothing) = begin
    join_via_cohort(drug_exposure(), drug_exposure; match_prefix=drug,
                    match=$match, exclude=$exclude, carry=$carry)
end

isa_component_class() = isa_concept_class("Component Class")
isa_dose_form_group() = isa_concept_class("Dose Form Group")
isa_ingredient() = isa_concept_class("Ingredient")

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

to_ingredient() = begin
    as(drug_exposure)
	join(concept_ancestor => from(concept_ancestor),
		concept_ancestor.descendant_concept_id == drug_exposure.drug_concept_id)
	join(concept().filter(concept_class_id=="Ingredient"),
		concept_id == concept_ancestor.ancestor_concept_id)
    define(person_id => drug_exposure.person_id,
           drug_exposure_id => drug_exposure.drug_exposure_id)
	partition(drug_exposure.drug_concept_id, name="ancestors")
    filter(concept_ancestor.min_levels_of_separation ==
           ancestors.min(concept_ancestor.min_levels_of_separation))
    group(concept_id, person_id, drug_exposure_id)
end

end
