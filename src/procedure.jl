@funsql begin

procedure_occurrence(ids...) = begin
    from(procedure_occurrence)
    $(length(ids) == 0 ? @funsql(define()) :
        @funsql filter(is_descendant_concept(procedure_concept_id, $ids)))
    define(is_historical => procedure_occurrence_id > 1500000000)
end

procedure_matches(ids...) = build_concept_matches($ids, procedure)
procedure_pairing(ids...) = build_concept_pairing($ids, procedure)
procedure_pivot(selection...; total=false, person_total=false, roundup=false) =
    build_pivot($selection, procedure, procedure_occurrence_id,
                $total, $person_total, $roundup)

link_procedure_occurrence(procedure_occurrence=nothing) =
    link(procedure_date, $(something(procedure_occurrence, @funsql procedure_occurrence())))

antijoin_procedure_occurrence(procedure_occurrence) =
    antijoin($procedure_occurrence, procedure_occurrence_id)

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
