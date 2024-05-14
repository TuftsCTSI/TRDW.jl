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
        omop.stop_reason)
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
    left_join(
        icd_concept => concept().filter(in(vocabulary_id, "ICD9CM", "ICD10CM")),
        icd_concept.concept_id == omop.condition_source_concept_id,
        optional = true)
end
  
condition(cs; with_icd9gem=false) =
    condition().filter(isa($cs; with_icd9gem=$with_icd9gem))

define_finding_site(concept_id=concept_id; name=finding_site_concept_id) = begin
    left_join($name => begin
        from(concept_relationship)
        filter(relationship_id == "Has finding site")
    end, $name.concept_id_1 == $concept_id)
    define($name => $name.concept_id_2)
end

prefer_source_icdcm() =
    $(let frame = :_source_icdcm;
        @funsql(begin
            left_join($frame => begin
                from(concept)
                filter(in(vocabulary_id, "ICD9CM", "ICD10CM"))
            end, omop.condition_source_concept.concept_id == $frame.concept_id)
            define(concept_id => coalesce($frame.concept_id, concept_id))
            undefine($frame)
        end)
    end)

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

truncate_icd_to_3char() =
    $(let frame = :_icd_to_3char;
        @funsql(begin
            left_join($frame => begin
                from(concept_relationship)
                filter(relationship_id == "Is a")
                join(icd_3_char => begin
                    from(concept)
                    filter(3 == length(concept_code))
                end, concept_id_2 == icd_3_char.concept_id)
            end, concept_id == $frame.concept_id_1)
            define(concept_id => coalesce($frame.concept_id_2, concept_id))
            undefine($frame)
        end)
    end)

to_3char_icdcm(; with_icd9gem=false) = begin
    prefer_source_icdcm()
    $(with_icd9gem ?
      @funsql(crosswalk_from_icd9cm_to_icd10cm()) :
      @funsql(define()))
    truncate_icd_to_3char()
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

icd10cm_chapter_concept_sets() = concept_sets(
    infectious_and_parasitic = ICD10CM(spec="A00-B99"),
    neoplasm = ICD10CM(spec="C00-D49"),
    blood_and_immune_disorder = ICD10CM(spec="D50-D89"),
    endocrine_and_metabolic = ICD10CM(spec="E00-E90"),
    mental_and_neurodevelopmental = ICD10CM(spec="F01-F99"),
    nervous_system = ICD10CM(spec="G00-G99"),
    eye_and_adnexa = ICD10CM(spec="H00-H59"),
    ear_and_mastoid = ICD10CM(spec="H60-I95"),
    circulatory_system = ICD10CM(spec="I00-I99"),
    respiratory_system = ICD10CM(spec="J00-J99"),
    digestive_system = ICD10CM(spec="K00-K95"),
    skin_and_subcutaneous = ICD10CM(spec="L00-L99"),
    musculoskeletal_and_connective = ICD10CM(spec="M00-M99"),
    genitourinary_system = ICD10CM(spec="N00-N99"),
    pregancy_birth_and_puerperium = ICD10CM(spec="O00-O99"),
    perinatal_condition = ICD10CM(spec="P00-P96"),
    congenital_abnormality = ICD10CM(spec="Q00-Q99"),
    clinical_and_laboratory = ICD10CM(spec="R00-R99"),
    external_consequence = ICD10CM(spec="S00-T98"),
    special_purpose = ICD10CM(spec="U00-U99"),
    external_morbidity = ICD10CM(spec="V00-Y99"),
    health_status_and_service = ICD10CM(spec="Z00-Z99")
)

end
