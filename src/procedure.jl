@funsql begin

procedure(match...) = begin
    from(procedure_occurrence)
    $(length(match) == 0 ? @funsql(define()) : @funsql(filter(procedure_matches($match))))
    left_join(person => person(),
              person_id == person.person_id, optional=true)
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

join_cohort_on_procedure(match...; exclude=nothing, carry=nothing) = begin
    join_via_cohort(procedure_occurrence(), procedure_date;
                    match_prefix=procedure, match=$match,
                    exclude=$exclude, carry=$carry)
end

truncate_icd9proc_to_3dig(name=nothing) =
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
                    filter(in(concept_class_id, "3-dig billing code", "3-dig nonbill code"))
                end, concept_id_2 == icd10cm_3_char.concept_id)
            end, $concept_id == $frame.concept_id_1)
            define($concept_id => coalesce($frame.concept_id_2, $concept_id))
        end)
    end)

truncate_to_icd10pcs_3dig(name=nothing) =
    $(let frame = gensym(),
          concept_id = (name == nothing) ? :concept_id :
                         contains(string(name), "concept_id") ? name :
                           Symbol("$(name)_concept_id");
        @funsql(begin
            left_join($frame => begin
                from(concept_relationship)
                filter(relationship_id == "Is a")
                join(hier => begin
                    from(concept)
                    filter(in(concept_class_id, "ICD10PCS Hierarchy"))
                end, concept_id_2 == hier.concept_id)
                join(ca => begin
                    from(concept_ancestor)
                    join(c => begin
                        from(concept)
                        filter(concept_class_id == "ICD10PCS Hierarchy")
                        filter(length(concept_code) == 3)
                    end, c.concept_id == ancestor_concept_id)

                end, concept_id_2 == ca.descendant_concept_id)
                define(concept_id_2 => ca.ancestor_concept_id)
            end, $concept_id == $frame.concept_id_1)
            define($concept_id => coalesce($frame.concept_id_2, $concept_id))
        end)
    end)

truncate_to_cpt4_hierarchy(name=nothing) =
    $(let frame = gensym(),
          concept_id = (name == nothing) ? :concept_id :
                         contains(string(name), "concept_id") ? name :
                           Symbol("$(name)_concept_id");
        @funsql(begin
            left_join($frame => begin
                from(concept_relationship)
                filter(relationship_id=="Is a")
                join(cpt4hier => begin
                    concept()
                    filter(concept_class_id=="CPT4 Hierarchy")
                end, concept_id_2 == cpt4hier.concept_id)
            end, $concept_id == $frame.concept_id_1)
            define($concept_id => coalesce($frame.concept_id_2, $concept_id))
        end)
    end)

crosswalk_cpt4_to_snomed(name=nothing) =
    $(let frame = gensym(),
          concept_id = (name == nothing) ? :concept_id :
                         contains(string(name), "concept_id") ? name :
                           Symbol("$(name)_concept_id");
        @funsql(begin
            left_join($frame => begin
                from(concept_relationship)
                filter(relationship_id=="CPT4 - SNOMED eq")
            end, $concept_id == $frame.concept_id_1)
            define($concept_id => coalesce($frame.concept_id_2, $concept_id))
        end)
    end)

to_procedure_hierarchies() = begin
        truncate_to_cpt4_hierarchy(procedure)
        crosswalk_cpt4_to_snomed(procedure)
        truncate_icd9proc_to_3dig(procedure)
        # todo: crosswalk icd9proc => icd10pcs
        truncate_to_icd10pcs_3dig(procedure)
    end

end

