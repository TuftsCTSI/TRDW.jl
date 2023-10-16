@funsql begin

procedure_occurrence() = begin
    from(procedure_occurrence)
    define(is_historical => procedure_occurrence_id > 1500000000)
end

procedure_isa(ids...) = is_descendant_concept(procedure_concept_id, $ids...)
procedure_type_isa(ids...) = is_descendant_concept(procedure_type_concept_id, $ids...)

join_procedure(ids...; carry=[]) = begin
    as(base)
    join(begin
        procedure_occurrence()
        $(length(ids) == 0 ? @funsql(define()) :
            @funsql filter(is_descendant_concept(procedure_concept_id, $ids...)))
    end, base.person_id == person_id)
    define($([@funsql($n => base.$n) for n in carry]...))
end

correlated_procedure(ids...) = begin
    from(procedure_occurrence)
	filter(person_id == :person_id)
    $(length(ids) == 0 ? @funsql(define()) :
        @funsql filter(is_descendant_concept(procedure_concept_id, $ids...)))
	bind(:person_id => person_id )
end

with_procedure_group(extension=nothing) =
    join(procedure_group => begin
        from(procedure_occurrence)
        $(extension == nothing ? @funsql(define()) : extension)
        group(person_id)
    end, person_id == procedure_group.person_id)

end
