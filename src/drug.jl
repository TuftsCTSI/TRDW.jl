@funsql begin

drug_exposure() = begin
    from(drug_exposure)
end

drug_concept() = concept().filter(domain_id == "Drug")
drug_component_class() = drug_concept().filter(concept_class_id == "Component Class")
drug_dose_form_group() = drug_concept().filter(concept_class_id == "Dose Form Group")
drug_ingredient() = drug_concept().filter(concept_class_id == "Ingredient")

component_class_isa(args...) = category_isa($ComponentClass, $args, drug_concept_id)
dose_form_group_isa(args...) = category_isa($DoseFormGroup, $args, drug_concept_id)
ingredient_isa(args...) = category_isa($Ingredient, $args, drug_concept_id)

drug_isa(ids...) = is_descendant_concept(drug_concept_id, $ids...)

isa_component_class() = isa_concept_class("Component Class")
isa_dose_form_group() = isa_concept_class("Dose Form Group")
isa_ingredient() = isa_concept_class("Ingredient")

join_drug(ids...; carry=[]) = begin
    as(base)
    join(begin
        drug_exposure()
        $(length(ids) == 0 ? @funsql(define()) :
            @funsql filter(is_descendant_concept(drug_concept_id, $ids...)))
    end, base.person_id == person_id)
    define($([@funsql($n => base.$n) for n in carry]...))
end

correlated_drug(ids...) = begin
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

end
