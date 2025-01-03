@funsql begin

truncate_icd9proc_to_3dig() =
    $(let frame = :_icd9proc_to_3dig;
        @funsql(begin
            left_join($frame => begin
                from(concept_relationship)
                filter(relationship_id == "Is a")
                join(icd10cm_3_char => begin
                    from(concept)
                    filter(in(concept_class_id, "3-dig billing code", "3-dig nonbill code"))
                end, concept_id_2 == icd10cm_3_char.concept_id)
            end, concept_id == $frame.concept_id_1)
            define(concept_id => coalesce($frame.concept_id_2, concept_id))
            undefine($frame)
        end)
    end)

truncate_to_icd10pcs_3dig() =
    $(let frame = :_to_icd10pcs_3dig;
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
            end, concept_id == $frame.concept_id_1)
            define(concept_id => coalesce($frame.concept_id_2, concept_id))
            undefine($frame)
        end)
    end)

truncate_to_cpt4_hierarchy() =
    $(let frame = :_to_cpt4_hierarchy;
        @funsql(begin
            left_join($frame => begin
                from(concept_relationship)
                filter(relationship_id=="Is a")
                join(cpt4hier => begin
                    concept()
                    filter(concept_class_id=="CPT4 Hierarchy")
                end, concept_id_2 == cpt4hier.concept_id)
            end, concept_id == $frame.concept_id_1)
            define(cconcept_id => coalesce($frame.concept_id_2, concept_id))
            undefine($frame)
        end)
    end)

crosswalk_cpt4_to_snomed() =
    $(let frame = :_cpt4_to_snomed;
        @funsql(begin
            left_join($frame => begin
                from(concept_relationship)
                filter(relationship_id=="CPT4 - SNOMED eq")
            end, concept_id == $frame.concept_id_1)
            define(concept_id => coalesce($frame.concept_id_2, concept_id))
            undefine($frame)
        end)
    end)

to_procedure_hierarchies() = begin
    truncate_to_cpt4_hierarchy()
    crosswalk_cpt4_to_snomed()
    truncate_icd9proc_to_3dig()
    truncate_to_icd10pcs_3dig()
end

end
