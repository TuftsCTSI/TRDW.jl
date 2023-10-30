@funsql begin

procedure_occurrence(match...) = begin
    from(procedure_occurrence)
    $(length(match) == 0 ? @funsql(define()) : @funsql(filter(procedure_matches($match))))
    left_join(visit_occurrence => visit_occurrence(),
              visit_occurrence_id == visit_occurrence.visit_occurrence_id, optional = true)
    define(is_historical => procedure_occurrence_id > 1500000000)
end

procedure_matches(match...) = concept_matches($match; match_prefix=procedure)

procedure_pivot(match...; event_total=true, person_total=true, roundup=true) = begin
    join_via_cohort(procedure_occurrence(), procedure_date;
                    match_prefix=procedure, match=$match)
    pairing_pivot($match, procedure, procedure_occurrence_id;
                  event_total=$event_total, person_total=$person_total, roundup=$roundup)
end

join_procedure_via_cohort(match...; exclude=nothing) = begin
    join_via_cohort(procedure_occurrence(), procedure_date;
                    match_prefix=procedure, match=$match)
    $(isnothing(exclude) ? @funsql(define()) :
      @funsql(filter(!procedure_matches($exclude))))
    define(concept_id => coalesce(procedure_source_concept_id, procedure_concept_id))
end

end
