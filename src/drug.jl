@funsql begin

drug() = begin
    from(drug_exposure)
end

is_component_class(args...) = in_category(drug, $ComponentClass, $args)
is_dose_form_group(args...) = in_category(drug, $DoseFormGroup, $args)
is_ingredient(args...) = in_category(drug, $Ingredient, $args)

drug_isa(ids...) = is_descendant_concept(drug_concept_id, $ids...)

join_drug(ids...; carry=[]) = begin
    as(base)
    join(begin
        drug()
        filter(is_descendant_concept(drug_concept_id, $ids...))
    end, base.person_id == person_id)
    define($([@funsql($n => base.$n) for n in carry]...))
end

correlated_drug(ids...) = begin
	from(drug_exposure)
	filter(person_id == :person_id)
	filter(is_descendant_concept(drug_concept_id, $ids...))
	bind(:person_id => person_id )
end

with_drug_group(extension=nothing) =
    join(drug_group => begin
      from(drug_exposure)
      $(extension == nothing ? @funsql(define()) : extension)
      group(person_id)
    end, person_id == drug_group.person_id)

end
