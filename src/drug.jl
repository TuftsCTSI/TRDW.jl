@funsql begin

drug_exposure() = begin
    from(drug_exposure)
end

component_class_isa(args...) = in_category($ComponentClass, $args, drug_concept_id)
dose_form_group_isa(args...) = in_category($DoseFormGroup, $args, drug_concept_id)
ingredient_isa(args...) = in_category($Ingredient, $args, drug_concept_id)

drug_isa(ids...) = is_descendant_concept(drug_concept_id, $ids...)

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
