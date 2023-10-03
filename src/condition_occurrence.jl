@funsql correlated_condition_occurrence(ids...) = begin
	from(condition_occurrence)
	filter(person_id == :person_id)
	filter(is_descendant_concept(condition_concept_id, $ids...))
	bind(:person_id => person_id )
end
