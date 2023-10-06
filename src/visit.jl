@funsql begin

visit() = begin
    from(visit)
end

visit_date_overlaps(start, finish) =
    (visit_start_date <= date($finish) && date($start) <= visit_end_date)

join_visit(ids...; carry=[]) = begin
    as(base)
    join(begin
        visit()
        filter(is_descendant_concept(visit_concept_id, $ids...))
    end, base.person_id == person_id)
    define($([@funsql($n => base.$n) for n in carry]...))
end

correlated_visit(ids...) = begin
    from(visit_occurrence)
	filter(person_id == :person_id)
	filter(is_descendant_concept(visit_concept_id, $ids...))
	bind(:person_id => person_id )
end

with_visit_group(extension=nothing) =
    join(visit_group => begin
        from(visit_occurrence)
        $(extension == nothing ? @funsql(define()) : extension)
        group(person_id)
    end, person_id == visit_group.person_id)

end
