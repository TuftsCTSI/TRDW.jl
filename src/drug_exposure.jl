@funsql begin

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
