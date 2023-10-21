@funsql begin

drug_exposure(ids...) = begin
    from(drug_exposure)
    $(length(ids) == 0 ? @funsql(define()) :
        @funsql filter(is_descendant_concept(drug_concept_id, $ids)))
    define(is_historical => drug_exposure_id > 1500000000)
end

drug_concept() = concept().filter(domain_id == "Drug")
drug_component_class() = drug_concept().filter(concept_class_id == "Component Class")
drug_dose_form_group() = drug_concept().filter(concept_class_id == "Dose Form Group")
drug_ingredient() = drug_concept().filter(concept_class_id == "Ingredient")
drug_pharmacologic_class() = drug_concept().filter(concept_class_id == "Pharmacologic Class")

component_class_isa(args...) = category_isa($ComponentClass, $args, drug_concept_id)
dose_form_group_isa(args...) = category_isa($DoseFormGroup, $args, drug_concept_id)
ingredient_isa(args...) = category_isa($Ingredient, $args, drug_concept_id)

drug_isa(ids...) = is_descendant_concept(drug_concept_id, $ids)
drug_source_relative_isa(ids...) = is_relative_concept(drug_source, $ids, "Is a")

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

link_drug_exposure(drug_exposure=nothing) =
    link(drug_exposure, $(something(drug_exposure, @funsql drug_exposure())))

antijoin_drug_exposure(drug_exposure) =
    antijoin($drug_exposure, drug_exposure_id)

join_drug_exposure(drug_exposure; carry=[]) = begin
    as(base)
    join($(something(drug_exposure, @funsql drug_exposure())), base.person_id == person_id)
    define($([@funsql($n => base.$n) for n in carry]...))
end

correlated_drug_exposure(ids...) = begin
	from(drug_exposure)
	filter(person_id == :person_id)
    $(length(ids) == 0 ? @funsql(define()) :
        @funsql filter(is_descendant_concept(drug_concept_id, $ids...)))
	bind(:person_id => person_id )
end

with_drug_group(extension=nothing) =
    join(drug_group => begin
      from(drug_exposure)
      $(extension == nothing ? @funsql(define()) : extension)
      group(person_id)
    end, person_id == drug_group.person_id)

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
	group(concept_id)
end

end
