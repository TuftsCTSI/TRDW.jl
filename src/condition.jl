@funsql begin

condition() = begin
    from(condition_occurrence)
    left_join(
        condition_source_concept => from(concept),
        condition_source_concept_id == condition_source_concept.concept_id,
        optional = true)
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
   cross_join(
        ext => begin
            # computed variables
            select(
                icd_concept_id =>
                   case(in(:source_vocabulary_id, "ICD9CM", "ICD10CM"), :source_concept_id),
                icd_concept_code =>
                   case(in(:source_vocabulary_id, "ICD9CM", "ICD10CM"), :source_concept_code),
                is_preepic => :ID > 1000000000)
            bind(
                :ID => omop.condition_occurrence_id,
                :source_vocabulary_id => omop.condition_source_concept.vocabulary_id,
                :source_concept_code => omop.condition_source_concept.concept_code,
                :source_concept_id => omop.condition_source_concept.concept_id)
        end)
end

condition(match...) =
    condition().filter(concept_matches($match))

is_primary_discharge_diagnosis() =
    ifnull(omop.condition_status_concept_id == 32903, false)

condition_status_isa(args...) =
    category_isa($Condition_Status, $args, omop.condition_status_concept_id)

define_finding_site(name=finding_site_concept_id, concept_id=concept_id) = begin
    left_join($name => begin
        from(concept_relationship)
        filter(relationship_id == "Has finding site")
    end, $name.concept_id_1 == $concept_id)
    define($name => $name.concept_id_2)
end

crosswalk_from_icd9cm_to_icd10cm() =
    $(let frame = gensym();
        @funsql(begin
            left_join($frame => begin
                from(concept_relationship)
                filter(relationship_id == "ICD9CM - ICD10CM gem")
            end, concept_id == $frame.concept_id_1)
            define(concept_id => coalesce($frame.concept_id_2, concept_id))
        end)
    end)

prefer_source_icdcm() =
    $(let frame = gensym();
        @funsql(begin
            left_join($frame => begin
                from(concept)
                filter(in(vocabulary_id, "ICD9CM", "ICD10CM"))
            end, omop.condition_source_concept.concept_id == $frame.concept_id)
            define(concept_id => coalesce($frame.concept_id, concept_id))
        end)
    end)

truncate_icd10cm_to_3char() =
    $(let frame = gensym();
        @funsql(begin
            left_join($frame => begin
                from(concept_relationship)
                filter(relationship_id == "Is a")
                join(icd10cm_3_char => begin
                    from(concept)
                    filter(in(concept_class_id, "3-char billing code", "3-char nonbill code"))
                end, concept_id_2 == icd10cm_3_char.concept_id)
            end, concept_id == $frame.concept_id_1)
            define(concept_id => coalesce($frame.concept_id_2, concept_id))
        end)
    end)

truncate_snomed_without_finding_site() =
    $(let frame = gensym(), partname = gensym();
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
            end, concept_id == $frame.descendant_concept_id)
            partition(concept_id, name=$partname)
            filter($frame.min_levels_of_separation ==
                   $partname.min($frame.min_levels_of_separation))
            define(concept_id => $frame.ancestor_concept_id)
        end)
    end)

backwalk_snomed_to_icd10cm() =
    $(let frame = gensym();
        @funsql(begin
            left_join($frame => begin
                from(concept_relationship)
                filter(relationship_id == "Mapped from")
                join(icd10cm => begin
                    from(concept)
                    filter(vocabulary_id=="ICD10CM")
                end, concept_id_2 == icd10cm.concept_id)
            end, concept_id == $frame.concept_id_1)
            define(concept_id => coalesce($frame.concept_id_2, concept_id))
        end)
    end)

to_3char_icd10cm() = begin
    prefer_source_icdcm()
    crosswalk_from_icd9cm_to_icd10cm()
	truncate_icd10cm_to_3char()
end

group_clinical_finding(carry...) =
    $(let frame = gensym(), partname = gensym();
        @funsql(begin
            as($frame)
            join(concept_ancestor => from(concept_ancestor),
                concept_ancestor.descendant_concept_id == $frame.concept_id)
            join(concept().filter(concept_class_id=="Clinical Finding"),
                concept_id == concept_ancestor.ancestor_concept_id)
            define($([@funsql($n => $frame.$n) for n in carry]...))
            partition($frame.concept_id, name=$partname)
            filter(concept_ancestor.min_levels_of_separation ==
                   $partname.min(concept_ancestor.min_levels_of_separation))
            define($([@funsql($n => $frame.$n) for n in carry]...))
            group([concept_id, $carry...]...)
        end)
    end)

end

function funsql_pick_ICD10CM(specification)
    specification = strip(replace(uppercase(specification), r"[\s,]+" => " "))
    predicate = []
    negations = []
    for chunk in split(specification, " ")
        if startswith(chunk, "-")
            push!(negations,
                @funsql(startswith(concept_code, $chunk)))
        elseif occursin("-", chunk)
            (lhs, rhs) = split(chunk, "-")
            @assert length(lhs) == length(rhs)
            chunk = ""
            for n in 1:length(lhs)
                needle = lhs[1:n]
                if startswith(rhs, needle)
                    chunk = needle
                end
            end
            push!(predicate, @funsql(
                and(between(concept_code, $lhs, $rhs),
                    startswith(concept_code, $chunk),
                    length(concept_code) == length($lhs))))
        else
            push!(predicate, @funsql(concept_code == $chunk))
        end
    end
    if length(predicate) == 1
        predicate = predicate[1]
    else
        predicate = @funsql(or($predicate...))
    end
    if length(negations) > 0
        if length(negations) == 1
            negations = negations[1]
        else
            negations = @funsql(or($negations...))
        end
        predicate = @funsql(and($predicate, not($negations)))
    end
    @funsql begin
        concept()
        filter(vocabulary_id=="ICD10CM")
        filter($predicate)
    end
end
