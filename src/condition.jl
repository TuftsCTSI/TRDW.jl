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

condition(match...) =
    condition().filter(concept_matches($match))

is_primary_discharge_diagnosis() =
    ifnull(omop.condition_status_concept_id == 32903, false)

status_isa(args...) =
    category_isa($Condition_Status, $args, omop.condition_status_concept_id)

define_finding_site(concept_id=concept_id; name=finding_site_concept_id, concept=true) = begin
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

end
