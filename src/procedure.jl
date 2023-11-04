@funsql begin

procedure(match...) = begin
    from(procedure_occurrence)
    $(length(match) == 0 ? @funsql(define()) : @funsql(filter(procedure_matches($match))))
    left_join(visit => visit_occurrence(),
        visit_occurrence_id == visit_occurrence.visit_occurrence_id, optional = true)
    join(event => begin
        from(procedure_occurrence)
        define(
            table_name => "procedure_occurrence",
            concept_id => procedure_concept_id,
            end_date => procedure_date,
            is_historical => procedure_occurrence_id > 1500000000,
            start_date => procedure_date,
            source_concept_id => procedure_source_concept_id)
    end, procedure_occurrence_id == event.procedure_occurrence_id, optional = true)
end

procedure_occurrence(match...) = procedure($match...)

procedure_matches(match...) = concept_matches($match; match_prefix=procedure)

procedure_pivot(match...; event_total=true, person_total=true, roundup=true) = begin
    join_via_cohort(procedure_occurrence(), procedure_date;
                    match_prefix=procedure, match=$match)
    pairing_pivot($match, procedure, procedure_occurrence_id;
                  event_total=$event_total, person_total=$person_total, roundup=$roundup)
end

filter_cohort_on_procedure(match...; exclude=nothing, also=nothing) =
    filter(exists(correlate_via_cohort(procedure_occurrence(), procedure_date;
                                       match_prefix=procedure, match=$match,
                                       exclude=$exclude, also=$also)))

join_cohort_on_procedure(match...; exclude=nothing) = begin
    join_via_cohort(procedure_occurrence(), procedure_date;
                    match_prefix=procedure, match=$match)
    $(isnothing(exclude) ? @funsql(define()) :
      @funsql(filter(!procedure_matches($exclude))))
    define(concept_id => coalesce(procedure_source_concept_id, procedure_concept_id))
end

to_cpt_hierarchy() = begin
    as(procedure_occurrence)
	join(concept_ancestor => from(concept_ancestor),
		concept_ancestor.descendant_concept_id == procedure_occurrence.procedure_concept_id)
    join(concept().filter(concept_class_id=="CPT4 Hierarchy"),
		concept_id == concept_ancestor.ancestor_concept_id)
    define(person_id => procedure_occurrence.person_id,
           procedure_occurrence_id => procedure_occurrence.procedure_occurrence_id)
	partition(procedure_occurrence.procedure_concept_id, name="ancestors")
    filter(concept_ancestor.min_levels_of_separation ==
           ancestors.min(concept_ancestor.min_levels_of_separation))
    group(concept_id, person_id, procedure_occurrence_id)
end

end
