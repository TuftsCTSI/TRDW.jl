@funsql correlated_drug_exposure(ids...) = begin
	from(drug_exposure)
	filter(person_id == :person_id)
	filter(is_descendant_concept(drug_concept_id, $ids...))
	bind(:person_id => person_id )
end
