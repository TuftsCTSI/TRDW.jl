@funsql begin

procedure() = begin
    from(procedure_occurrence)
end

procedure_isa(ids...) = is_descendant_concept(procedure_concept_id, $ids...)
procedure_type_isa(ids...) = is_descendant_concept(procedure_type_concept_id, $ids...)

join_procedure(ids...; carry=[]) = begin
    as(base)
    join(begin
        procedure()
        filter(is_descendant_concept(procedure_concept_id, $ids...))
    end, base.person_id == person_id)
    define($([@funsql($n => base.$n) for n in carry]...))
end

correlated_procedure(ids...) = begin
    from(procedure_occurrence)
	filter(person_id == :person_id)
	filter(is_descendant_concept(procedure_concept_id, $ids...))
	bind(:person_id => person_id )
end

with_procedure_group(extension=nothing) =
    join(procedure_group => begin
        from(procedure_occurrence)
        $(extension == nothing ? @funsql(define()) : extension)
        group(person_id)
    end, person_id == procedure_group.person_id)

end
