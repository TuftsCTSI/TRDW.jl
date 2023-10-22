@funsql begin

condition_occurrence() = begin
    from(condition_occurrence)
    left_join(visit_occurrence => visit_occurrence(),
              visit_occurrence_id == visit_occurrence.visit_occurrence_id, optional = true)
end

is_condition_status(args...) =
    in_category($ConditionStatus, $args, condition_status_concept_id)

condition_matches(ids...) = build_concept_matches($ids, condition)
condition_pairing(ids...) = build_concept_pairing($ids, condition)
condition_pivot(selection...; total=false, person_total=false, roundup=false) =
    build_pivot($selection, condition, condition_occurrence_id,
                $total, $person_total, $roundup)

link_condition_occurrence(condition_occurrence=nothing) =
    link(condition, $(something(condition_occurrence, @funsql condition_occurrence())))

join_condition(ids...; carry=[]) = begin
    as(base)
    join(begin
        condition_occurrence()
        $(length(ids) == 0 ? @funsql(define()) :
            @funsql filter(is_descendant_concept(condition_concept_id, $ids)))
    end, base.person_id == person_id)
    define($([@funsql($n => base.$n) for n in carry]...))
end

correlated_condition(ids...) = begin
    from(condition_occurrence)
    filter(person_id == :person_id)
    $(length(ids) == 0 ? @funsql(define()) :
        @funsql filter(is_descendant_concept(condition_concept_id, $ids)))
    bind(:person_id => person_id )
end

with_condition_group(extension=nothing) =
    join(condition_group => begin
      from(condition_occurrence)
      $(extension == nothing ? @funsql(define()) : extension)
      group(person_id)
    end, person_id == condition_group.person_id)

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
