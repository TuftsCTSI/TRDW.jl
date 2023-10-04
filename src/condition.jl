@funsql begin

condition() = begin
    from(condition_occurrence)
end

is_condition_status(args...) = in_category(condition_status, $ConditionStatus, $args)

correlated_condition(ids...) = begin
	from(condition_occurrence)
	filter(person_id == :person_id)
	filter(is_descendant_concept(condition_concept_id, $ids...))
	bind(:person_id => person_id )
end

with_condition_group(extension=nothing) =
    join(condition_group => begin
      from(condition_occurrence)
      $(extension == nothing ? @funsql(define()) : extension)
      group(person_id)
    end, person_id == condition_group.person_id)

end
