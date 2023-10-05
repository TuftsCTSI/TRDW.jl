@funsql begin

condition() = begin
    from(condition_occurrence)
end

is_condition_status(args...) = in_category(condition_status, $ConditionStatus, $args)

join_condition(ids...; carry::Vector{Symbol}=[]) = begin
    as(base)
    join(begin
        condition()
        filter(is_descendant_concept(condition_concept_id, $ids...))
    end, base.person_id == person_id)
    define($([@funsql($n => base.$n) for n in carry]...))
end

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
