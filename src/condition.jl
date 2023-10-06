@funsql begin

condition_occurrence() = begin
    from(condition_occurrence)
end

is_condition_status(args...) = in_category(condition_status_concept_id, $ConditionStatus, $args)

condition_isa(ids...) = is_descendant_concept(condition_concept_id, $ids...)

join_condition(ids...; carry=[]) = begin
    as(base)
    join(begin
        condition_occurrence()
        $(length(ids) == 0 ? @funsql(define()) :
            @funsql filter(is_descendant_concept(condition_concept_id, $ids...)))
    end, base.person_id == person_id)
    define($([@funsql($n => base.$n) for n in carry]...))
end

correlated_condition(ids...) = begin
	from(condition_occurrence)
	filter(person_id == :person_id)
    $(length(ids) == 0 ? @funsql(define()) :
        @funsql filter(is_descendant_concept(condition_concept_id, $ids...)))
	bind(:person_id => person_id )
end

with_condition_group(extension=nothing) =
    join(condition_group => begin
      from(condition_occurrence)
      $(extension == nothing ? @funsql(define()) : extension)
      group(person_id)
    end, person_id == condition_group.person_id)

end
