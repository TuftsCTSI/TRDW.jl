@funsql begin

condition(; prefer_icd = false) = begin
    from(condition_occurrence)
    as(omop)
    define(
        domain_id => "Condition",
        occurrence_id => omop.condition_occurrence_id,
        is_historical => omop.condition_occurrence_id > 1000000000,
        person_id => omop.person_id,
        concept_id => $prefer_icd ? omop.condition_source_concept_id : omop.condition_concept_id,
        datetime => omop.condition_start_datetime,
        end_datetime => omop.condition_end_datetime,
        type_concept_id => omop.condition_type_concept_id,
        status_concept_id => omop.condition_status_concept_id,
        stop_reason => omop.stop_reason,
        provider_id => omop.provider_id,
        visit_occurrence_id => omop.visit_occurrence_id)
end

condition(match...) =
    condition().filter(concept_matches($match))

condition_occurrence(match...) = begin
    from(condition_occurrence)
    $(length(match) == 0 ? @funsql(define()) : @funsql(filter(condition_matches($match))))
    left_join(person => person(),
              person_id == person.person_id, optional=true)
    left_join(visit => visit_occurrence(),
        visit_occurrence_id == visit.visit_occurrence_id, optional = true)
    join(event => begin
        from(condition_occurrence)
        define(
            table_name => "condition_occurrence",
            concept_id => condition_concept_id,
            end_date => condition_end_date,
            is_historical => condition_occurrence_id > 1000000000,
            start_date => condition_start_date,
            source_concept_id => condition_source_concept_id)
    end, condition_occurrence_id == event.condition_occurrence_id, optional = true)
end

is_primary_discharge_diagnosis() =
    (condition_status_concept_id == 32903)

condition_status_isa(args...) =
    category_isa($Condition_Status, $args, condition_status_concept_id)
condition_matches(match...) = concept_matches($match; match_prefix=condition)

condition_pivot(match...; event_total=true, person_total=true, roundup=true) = begin
    join_via_cohort(condition_occurrence(), condition; match=$match)
    pairing_pivot($match, condition, condition_occurrence_id;
                  event_total=$event_total, person_total=$person_total, roundup=$roundup)
end

filter_cohort_on_condition(match...; exclude=nothing, also=nothing) =
    filter(exists(correlate_via_cohort(condition_occurrence(), condition;
                                       match=$match, exclude=$exclude,
                                       also=$also)))

join_cohort_on_condition(match...; exclude=nothing, carry=nothing) = begin
    join_via_cohort(condition_occurrence(), condition;
                    match=$match, exclude=$exclude, carry=$carry)
end

crosswalk_from_icd9cm_to_icd10cm(name=nothing) =
    $(let frame = gensym(),
          concept_id = (name == nothing) ? :concept_id :
                         contains(string(name), "concept_id") ? name :
                           Symbol("$(name)_concept_id");
        @funsql(begin
            left_join($frame => begin
                from(concept_relationship)
                filter(relationship_id == "ICD9CM - ICD10CM gem")
            end, $concept_id == $frame.concept_id_1)
            define($concept_id => coalesce($frame.concept_id_2, $concept_id))
        end)
    end)

overwrite_with_icd10cm(source, target) =
    $(let frame = gensym(),
          source_id = contains(string(source), "concept_id") ? source :
                          Symbol("$(source)_concept_id"),
          target_id = contains(string(target), "concept_id") ? target :
                           Symbol("$(target)_concept_id");
        @funsql(begin
            left_join($frame => begin
                from(concept)
                filter(vocabulary_id == "ICD10CM")
            end, $source_id == $frame.concept_id)
            define($target_id => coalesce($frame.concept_id, $target_id))
        end)
    end)

truncate_icd10cm_to_3char(name=nothing) =
    $(let frame = gensym(),
          concept_id = (name == nothing) ? :concept_id :
                         contains(string(name), "concept_id") ? name :
                           Symbol("$(name)_concept_id");
        @funsql(begin
            left_join($frame => begin
                from(concept_relationship)
                filter(relationship_id == "Is a")
                join(icd10cm_3_char => begin
                    from(concept)
                    filter(in(concept_class_id, "3-char billing code", "3-char nonbill code"))
                end, concept_id_2 == icd10cm_3_char.concept_id)
            end, $concept_id == $frame.concept_id_1)
            define($concept_id => coalesce($frame.concept_id_2, $concept_id))
        end)
    end)

truncate_snomed_without_finding_site(name=nothing) =
    $(let frame = gensym(), partname = gensym(),
          concept_id = (name == nothing) ? :concept_id :
                         contains(string(name), "concept_id") ? name :
                           Symbol("$(name)_concept_id");
        @funsql(begin
            left_join($frame => begin
                from(concept_ancestor)
                left_join(f => begin
                    from(concept_relationship)
                    filter(relationship_id == "Has finding site")
                    group(concept_id_1)
                end, f.concept_id_1 == ancestor_concept_id)
                join(c => from(concept).filter(vocabulary_id == "SNOMED"),
                     c.concept_id == ancestor_concept_id)
                filter(isnull(f.concept_id_1))
            end, $concept_id == $frame.descendant_concept_id)
            partition($concept_id, name=$partname)
            filter($frame.min_levels_of_separation ==
                   $partname.min($frame.min_levels_of_separation))
            define($concept_id => $frame.ancestor_concept_id)
        end)
    end)

backwalk_snomed_to_icd10cm_3char(name=nothing) =
    $(let frame = gensym(),
          concept_id = (name == nothing) ? :concept_id :
                         contains(string(name), "concept_id") ? name :
                           Symbol("$(name)_concept_id");
        @funsql(begin
            left_join($frame => begin
                from(concept_relationship)
                filter(relationship_id == "Mapped from")
                join(icd10cm => begin
                    from(concept)
                    filter(vocabulary_id=="ICD10CM")
                end, concept_id_2 == icd10cm.concept_id)
                truncate_icd10cm_to_3char(concept_id_2)
                deduplicate(concept_id_2)
            end, $concept_id == $frame.concept_id_1)
            define($concept_id => coalesce($frame.concept_id_2, $concept_id))
        end)
    end)

to_3char_icd10cm() = begin
    crosswalk_from_icd9cm_to_icd10cm(condition_source)
	truncate_icd10cm_to_3char(condition_source)
	overwrite_with_icd10cm(condition_source, condition)
	#backwalk_snomed_to_icd10cm_3char(condition)
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
