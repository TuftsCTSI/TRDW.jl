@funsql correlated_procedure_occurrence(ids...) = begin
	from(procedure_occurrence)
	filter(person_id == :person_id)
	filter(is_descendant_concept(procedure_concept_id, $ids...))
	bind(:person_id => person_id )
end
