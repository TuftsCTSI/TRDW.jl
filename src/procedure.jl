@funsql begin

procedure() = begin
    from(procedure_occurrence)
    define(is_preepic => procedure_occurrence_id > 1500000000)
    left_join(
        procedure_source_concept => from(concept),
        procedure_source_concept_id == procedure_source_concept.concept_id,
        optional = true)
    as(omop)
    define(
        # event columns
        domain_id => "Procedure",
        occurrence_id => omop.procedure_occurrence_id,
        person_id => omop.person_id,
        concept_id => omop.procedure_concept_id,
        datetime => coalesce(omop.procedure_datetime,
                             timestamp(omop.procedure_date)),
        datetime_end => missing,
        type_concept_id => omop.procedure_type_concept_id,
        provider_id => omop.provider_id,
        visit_occurrence_id => omop.visit_occurrence_id,
        # domain specific columns
        omop.modifier_concept_id,
        omop.quantity,
        source_concept_id => omop.procedure_source_concept_id)
    join(
        person => person(),
        person_id == person.person_id,
        optional = true)
    join(
        concept => concept(),
        concept_id == concept.concept_id,
        optional = true)
    left_join(
        type_concept => concept(),
        type_concept_id == type_concept.concept_id,
        optional = true)
    left_join(
        modifier_concept => concept(),
        modifier_concept_id == modifier_concept.concept_id,
        optional = true)
    left_join(
        provider => provider(),
        provider_id == provider.provider_id,
        optional = true)
    left_join(
        visit => visit(),
        visit_occurrence_id == visit.occurrence_id,
        optional = true)
end

procedure(match...) =
    procedure().filter(concept_matches($match))

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
