@funsql begin

condition() = begin
    from(condition_occurrence)
    left_join(
        condition_source_concept => from(concept),
        condition_source_concept_id == condition_source_concept.concept_id,
        optional = true)
    define(is_preepic => condition_occurrence_id > 1000000000)
    as(omop)
    define(
        # event columns
        domain_id => "Condition",
        occurrence_id => omop.condition_occurrence_id,
        person_id => omop.person_id,
        concept_id => omop.condition_concept_id,
        datetime => coalesce(omop.condition_start_datetime,
                             timestamp(omop.condition_start_date)),
        datetime_end => coalesce(omop.condition_end_datetime,
                                 timestamp(omop.condition_end_date)),
        type_concept_id => omop.condition_type_concept_id,
        provider_id => omop.provider_id,
        visit_occurrence_id => omop.visit_occurrence_id,
        # domain specific columns
        status_concept_id => omop.condition_status_concept_id,
        omop.stop_reason,
        source_concept_id => omop.condition_source_concept_id)
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
        status_concept => concept(),
        status_concept_id == status_concept.concept_id,
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
  
condition(cs) =
    condition().filter(isa(concept_id, $cs) || isa_icd(source_concept_id, $cs))

define_finding_site(concept_id=concept_id; name=finding_site_concept_id) = begin
    left_join($name => begin
        from(concept_relationship)
        filter(relationship_id == "Has finding site")
    end, $name.concept_id_1 == $concept_id)
    define($name => $name.concept_id_2)
end

define_icd_code_set() = begin
    left_join(
        source_to_icd_concept => begin
            from(concept)
            left_join(
                edg_current_icd10 => begin
                    from(concept_relationship)
                    filter(relationship_id == "Has edg_current_icd10.code")
                end,
                concept_id == edg_current_icd10.concept_id_1)
            join(
                icd_concept => begin
                    from(concept)
                    filter(in(vocabulary_id, "ICD9CM", "ICD10CM"))
                end,
                coalesce(edg_current_icd10.concept_id_2, concept_id) == icd_concept.concept_id)
            group(concept_id)
        end,
        source_concept_id == source_to_icd_concept.concept_id)
    define(icd_code_set =>
        array_join(array_sort(source_to_icd_concept.collect_set(icd_concept.concept_code)), " ");
            before = source_concept_id)
end

crosswalk_from_icd9cm_to_icd10cm() =
    $(let frame = :_icd9cm_to_icd10cm;
        @funsql(begin
            left_join($frame => begin
                from(concept_relationship)
                filter(relationship_id == "ICD9CM - ICD10CM gem")
            end, concept_id == $frame.concept_id_1)
            define(concept_id => coalesce($frame.concept_id_2, concept_id))
            undefine($frame)
        end)
    end)

truncate_icd10cm_to_3char() = begin
    left_join(related_icd10cm_3char => begin
        from(concept_relationship)
        filter(relationship_id == "Is a")
        join(icd10cm_3char => begin
            from(concept)
            filter(in(vocabulary_id, "ICD10CM"))
            filter(3 == length(concept_code))
        end, concept_id_2 == icd10cm_3char.concept_id)
    end, source_concept_id == related_icd10cm_3char.concept_id_1)
    define(source_concept_id => coalesce(related_icd10cm_3char.concept_id_2, source_concept_id))
end

mapto_icd10cm() = begin
    left_join(
        edg_current_icd10 => begin
            from(concept_relationship)
            filter(relationship_id == "Has edg_current_icd10.code")
        end,
        source_concept_id == edg_current_icd10.concept_id_1)
    define(source_concept_id => coalesce(edg_current_icd10.concept_id_2, source_concept_id))
end

prefer_source_icdcm() = begin
    left_join(
        icd_concept => begin
            from(concept)
            filter(in(vocabulary_id, "ICD9CM", "ICD10CM"))
        end,
        source_concept_id == icd_concept.concept_id)
    define(concept_id => coalesce(icd_concept.concept_id, concept_id))
end

snomed_top_ancestors(concept_id=concept_id;
                     exclude::AbstractVector = [SNOMED("404684003", "Clinical finding")]) =
    $(let frame = :_to_snomed_top_ancestors,
          partname = :_snomed_top_ancestors_partition;
        @funsql(begin
            left_join($frame => begin
                from(concept_ancestor)
                filter(!isa_strict(ancestor_concept_id, $exclude))
                join(c => begin
                    from(concept)
                    filter(vocabulary_id == "SNOMED" &&
                           standard_concept == "S")
                end, c.concept_id == ancestor_concept_id)
            end, concept_id == $frame.descendant_concept_id)
            partition(concept_id, name=$partname)
            filter(isnull($frame.ancestor_concept_id) ||
                   $frame.max_levels_of_separation ==
                   $partname.max($frame.max_levels_of_separation))
            define(concept_id => coalesce($frame.ancestor_concept_id, concept_id))
            undefine($frame, $partname)
        end)
    end)

to_snomed_intermediate_conditions() =
    snomed_top_ancestors(exclude=[
        SNOMED("404684003", "Clinical finding"),
        SNOMED("64572001", "Disease"),
        SNOMED("822988000", "Disorder of abdominopelvic segment of trunk"),
        SNOMED("362965005", "Disorder of body system"),
        SNOMED("118934005", "Disorder of head"),
        SNOMED("19660004", "Disorder of soft tissue"),
        SNOMED("128121009", "Disorder of trunk"),
        SNOMED("609624008", "Finding of abdomen"),
        SNOMED("822987005", "Finding of abdominopelvic segment of trunk"),
        SNOMED("118254002", "Finding of head and neck region"),
        SNOMED("699697007", "Finding of sensation by site"),
        SNOMED("302292003", "Finding of trunk structure"),
        SNOMED("609623002", "Finding of upper trunk"),
        SNOMED("298705000", "Finding of region of thorax"),
        SNOMED("609622007", "Disorder of thoracic segment of trunk"),
        SNOMED("118222006", "General finding of observation of patient"),
        SNOMED("248402002", "General finding of soft tissue"),
        SNOMED("406122000", "Head finding"),
        #SNOMED("102957003", "Neurological finding"),
        SNOMED("22253000",  "Pain"),
        SNOMED("276435006", "Pain / sensation finding"),
        #SNOMED("279001004", "Pain finding at anatomical site"),
        #SNOMED("106147001", "Sensory nervous system finding"),
        SNOMED("406123005", "Viscus structure finding")])

icd10cm_chapter() = from($(DataFrame([
    ("A00", "B99", "Infectious and Parasitic"),
    ("C00", "D49", "Neoplasm"),
    ("D50", "D89", "Blood and Immune Disorder"),
    ("E00", "E90", "Endocrine and Metabolic"),
    ("F01", "F99", "Mental and Neurodevelopmental"),
    ("G00", "G99", "Nervous System"),
    ("H00", "H59", "Eye and Adnexa"),
    ("H60", "I95", "Ear and Mastoid"),
    ("I00", "I99", "Circulatory System"),
    ("J00", "J99", "Respiratory System"),
    ("K00", "K95", "Digestive System"),
    ("L00", "L99", "Skin and Subcutaneous"),
    ("M00", "M99", "Musculoskeletal and Connective"),
    ("N00", "N99", "Genitourinary System"),
    ("O00", "O99", "Pregancy Birth and Puerperium"),
    ("P00", "P96", "Perinatal Condition"),
    ("Q00", "Q99", "Congenital Abnormality"),
    ("R00", "R99", "Clinical and Laboratory"),
    ("S00", "T98", "External Consequence"),
    ("U00", "U99", "Special Purpose"),
    ("V00", "Y99", "External Morbidity"),
    ("Z00", "Z99", "Health Status and Service")],
    [:chapter_start, :chapter_ends, :chapter_name])))

define_icd10cm_chapter_name() = begin
    join(_concept => from(concept), _concept.concept_id == source_concept_id)
    left_join(icd10cm_chapter => icd10cm_chapter(),
        _concept.vocabulary_id == "ICD10CM" &&
        _concept.concept_code >= icd10cm_chapter.chapter_start &&
        substring(_concept.concept_code, 1, 3) <= icd10cm_chapter.chapter_ends)
    define(icd10cm_chapter_name => icd10cm_chapter.chapter_name)
end

end
