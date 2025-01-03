@funsql begin

drug_concept() = concept().filter(domain_id == "Drug")
drug_component_class() = drug_concept().filter(concept_class_id == "Component Class")
drug_dose_form_group() = drug_concept().filter(concept_class_id == "Dose Form Group")
drug_ingredient() = drug_concept().filter(concept_class_id == "Ingredient")
drug_pharmacologic_class() = drug_concept().filter(concept_class_id == "Pharmacologic Class")

drug_ingredient_via_SNOMED(code, name) = begin
    SNOMED($code, $name)
    concept_relatives("Subsumes",1:3)
    concept_relatives("SNOMED - RxNorm eq")
    filter(concept_class_id=="Ingredient")
    deduplicate(concept_id)
    filter_out_descendants()
end

drug_ingredient_via_NDFRT(code, name) = begin
    NDFRT($code, $name)
    concept_relatives("Subsumes",1:3)
    concept_relatives("NDFRT - RxNorm eq")
    filter(concept_class_id=="Ingredient")
    deduplicate(concept_id)
    filter_out_descendants()
end

drug_ingredient_via_ATC(code, name) = begin
    ATC($code, $name)
    concept_relatives("Subsumes",1:3)
    concept_relatives("ATC - RxNorm pr lat")
    filter(concept_class_id=="Ingredient")
    deduplicate(concept_id)
    filter_out_descendants()
end

drug_ingredient_via_HemOnc(code, name) = begin
    HemOnc($code, $name)
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
