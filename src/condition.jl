@funsql begin

condition_occurrence(match...) = begin
    from(condition_occurrence)
    $(length(match) == 0 ? @funsql(define()) : @funsql(filter(condition_matches($match))))
    left_join(visit_occurrence => visit_occurrence(),
              visit_occurrence_id == visit_occurrence.visit_occurrence_id, optional = true)
    define(is_historical => condition_occurrence_id > 1000000000)
end

is_primary_discharge_diagnosis() =
    (condition_status_concept_id == 32903)

is_condition_status(args...) =
    in_category($ConditionStatus, $args, condition_status_concept_id)

condition_matches(match...) = concept_matches($match; match_prefix=condition)

condition_pivot(match...; event_total=true, person_total=true, roundup=true) = begin
    join_via_cohort(condition_occurrence(), condition; match=$match)
    pairing_pivot($match, condition, condition_occurrence_id;
                  event_total=$event_total, person_total=$person_total, roundup=$roundup)
end

join_condition_via_cohort(match...; exclude=nothing) = begin
    join_via_cohort(condition_occurrence(), condition; match=$match)
    $(isnothing(exclude) ? @funsql(define()) :
      @funsql(filter(!condition_matches($exclude))))
    define(concept_id => coalesce(condition_source_concept_id, condition_concept_id))
end

group_3char_icd10cm(;carry=[]) = begin
    as(condition_occurrence)
    join(concept_ancestor => from(concept_ancestor),
        concept_ancestor.descendant_concept_id ==
        condition_occurrence.condition_source_concept_id)
    join(begin
             concept()
             filter(vocabulary_id == "ICD10CM")
             filter(in(concept_class_id, "3-char billing code", "3-char nonbill code"))
        end,
        concept_id == concept_ancestor.ancestor_concept_id)
    define($([@funsql($n => condition_occurrence.$n) for n in carry]...))
    partition(condition_occurrence.condition_source_concept_id, name="ancestors")
    filter(concept_ancestor.min_levels_of_separation ==
           ancestors.min(concept_ancestor.min_levels_of_separation))
    group(concept_id)
end

group_clinical_finding(concept_id=nothing;carry=[]) = begin
    define(concept_id => $(something(concept_id, :condition_concept_id)))
    as(condition_occurrence)
    join(concept_ancestor => from(concept_ancestor),
        concept_ancestor.descendant_concept_id == condition_occurrence.condition_concept_id)
    join(concept().filter(concept_class_id=="Clinical Finding"),
        concept_id == concept_ancestor.ancestor_concept_id)
    define($([@funsql($n => condition_occurrence.$n) for n in carry]...))
    partition(condition_occurrence.condition_concept_id, name="ancestors")
    filter(concept_ancestor.min_levels_of_separation ==
           ancestors.min(concept_ancestor.min_levels_of_separation))
    group(concept_id)
end

end
