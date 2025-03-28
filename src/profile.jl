function funsql_define_age_at_extraction_or_death()
    person = gensym()
    @funsql begin
        join($person => person(), $person.person_id == person_id)
        define(age_at_extraction_or_death=> age_at_extraction_or_death($person))
    end
end

function funsql_define_race()
    person = gensym()
    @funsql begin
        join($person => person(), $person.person_id == person_id)
        define(
            race =>
                $person.race_concept_id == 0 ?
                ( $person.ethnicity_concept_id == 38003563 ?
                "Unspecified (Hispanic)" :
                "Unspecified") :
                $person.race_concept.concept_name)
    end
end

function funsql_define_ethnicity()
    person = gensym()
    @funsql begin
        join($person => person(), $person.person_id == person_id)
        define(
            ethnicity =>
                $person.ethnicity_concept_id == 0 ?
                "Unspecified" :
                $person.ethnicity_concept.concept_name)
    end
end

function funsql_define_sex()
    person = gensym()
    @funsql begin
        join($person => person(), $person.person_id == person_id)
        define(
            sex =>
                $person.gender_concept_id == 0 ? "" :
                $person.gender_concept.concept_code)
    end
end

function funsql_define_birth_date(;name=:birth_date)
    person = gensym()
    @funsql begin
        join($person => from(person), $person.person_id == person_id)
        define($name => date($person.birth_datetime))
    end
end

function funsql_define_birth_year(;name=:birth_year)
    person = gensym()
    @funsql begin
        join($person => from(person), $person.person_id == person_id)
        define($name => $person.year_of_birth)
    end
end

function funsql_define_death_date(;name=:death_date)
    death = gensym()
    @funsql begin
        left_join($death => from(death), $death.person_id == person_id)
        define($name => $death.death_date)
        undefine($death)
    end
end

function funsql_define_death_year(;name=:death_year)
    death = gensym()
    @funsql begin
        left_join($death => from(death), $death.person_id == person_id)
        define($name => year($death.death_date))
        undefine($death)
    end
end

function funsql_define_pat_id(column_name=:pat_id)
    person = gensym()
    @funsql begin
        left_join($person => begin
            from(`trdw_epic.person`)
        end, $person.person_id == person_id)
        with(
            `trdw_epic.person` =>
                from($(FunSQL.SQLTable(qualifiers = [:ctsi, :trdw_epic],
                                       name = :person,
                                       columns = [:person_id, :person_source_value]))))
        define($column_name => $person.person_source_value)
    end
end

function funsql_define_soarian_mrn()
    person_map  = gensym()
    @funsql begin
        left_join($person_map =>
            from(`trdwlegacysoarian.omop_common_person_map`),
            $person_map.person_id == person_id)
        with(
            `trdwlegacysoarian.omop_common_person_map` =>
                from($(FunSQL.SQLTable(qualifiers = [:ctsi, :trdwlegacysoarian],
                                    name = :omop_common_person_map,
                                    columns = [:person_id, :mrn]))))
        define(soarian_mrn => $person_map.mrn)
    end
end

function funsql_define_epic_mrn()
    pat_id = gensym()
    patient = gensym()
    @funsql begin
        define_pat_id($pat_id)
        left_join($patient => begin
            from(`epicclarity.patient`)
        end, $patient.pat_id == $pat_id)
        with(
            `epicclarity.patient` =>
                from($(FunSQL.SQLTable(qualifiers = [:main, :epicclarity],
                                       name = :patient,
                                       columns = [:pat_id, :pat_mrn_id]))))
        define(epic_mrn => $patient.pat_mrn_id)
        undefine($pat_id)
    end
end

function funsql_define_translator(filter=true; name=:translator)
    @funsql begin
        group_with($name => begin
            observation(SNOMED(314431000, "Interpreter present"))
            define(language => replace(replace(
                qualifier_concept.concept_name, " language", ""), " dialect", ""))
        end, $filter)
        define($name => collect_to_string($name.language))
        define($name => $name == "" ? missing : $name)
    end
end

function funsql_define_preferred_language(filter=true; name=:preferred_language)
    @funsql begin
        group_with($name => begin
            observation(SNOMED(428996008, "Language preference"))
            filter(!icontains(value_as_string, "same") && "" != value_as_string)
        end, $filter)
        define($name => last($name.value_as_string))
    end
end


smoking_behavior_concepts() = [
        funsql_OMOP_Extension("OMOP5181846","Cigar smoker"),
        funsql_OMOP_Extension("OMOP5181838","Cigarette smoker"),
        funsql_OMOP_Extension("OMOP5181836","Electronic cigarette smoker"),
        funsql_OMOP_Extension("OMOP5181847","Hookah smoker"),
        funsql_OMOP_Extension("OMOP5181837","Passive smoker"),
        funsql_OMOP_Extension("OMOP5181845","Pipe smoker")]

never_smoker_concepts() = [funsql_OMOP_Extension("OMOP5181834", "Never used tobacco or its derivatives")]

@funsql smoking_behavior_concepts() = concept($(smoking_behavior_concepts())...)

@funsql matches_smoking_behavior() =
    concept_matches($(smoking_behavior_concepts()); on=value_as_concept_id)

@funsql matches_never_smoker() =
    concept_matches($(never_smoker_concepts()); on=value_as_concept_id)

function funsql_define_smoking(filter=true; name=:smoking)
    @funsql begin
        group_with($name => begin
            observation()
            filter(matches_smoking_behavior())
            define(behavior => value_as_concept.concept_name)
        end, $filter)
        define($name => collect_to_string($name.behavior))
        define($name => $name == "" ? missing : $name)
    end
end

function funsql_define_never_smoker(filter=true; name=:never_smoker)
    @funsql begin
        group_with($name => begin
            observation()
            define(was_smoker => matches_smoking_behavior() || in(value_as_string, "prev.", "current", "YES", "cigarettes"))
            define(never_smoker => matches_never_smoker() || in(value_as_string, "never"))
        end, $filter)
        define($name => any($name.never_smoker) && !any($name.was_smoker))
        define($name => $name == "" ? missing : $name)
    end
end

function funsql_define_natural_mother_id(filter=true; name=:natural_mother_id)
    @funsql begin
        group_with($name => begin
            from(fact_relationship)
            filter(in(relationship_concept_id, 4326600, 4277283))
            define(person_id => fact_id_1)
            join(p => from(person).filter(gender_concept_id ==8532),
                 p.person_id == fact_id_2)
        end, $filter)
        define($name => first($name.fact_id_2))
    end
end

function funsql_define_profile(args...)
    query = @funsql(define())
    for arg in args
        query = query |> begin
            arg == :birth_date ? funsql_define_birth_date() :
            arg == :birth_year ? funsql_define_birth_year() :
            arg == :age_at_extraction_or_death ? funsql_define_age_at_extraction_or_death() :
            arg == :current_age ? funsql_define_age_at_extraction_or_death() : # current_age is deprecated, use age_at_extraction_or_death
            arg == :death_date ? funsql_define_death_date() :
            arg == :death_year ? funsql_define_death_year() :
            arg == :pat_id ? funsql_define_pat_id() :
            arg == :epic_mrn ? funsql_define_epic_mrn() :
            arg == :ethnicity ? funsql_define_ethnicity() :
            arg == :never_smoker ? funsql_define_never_smoker() :
            arg == :preferred_language ? funsql_define_preferred_language() :
            arg == :race ? funsql_define_race() :
            arg == :sex ? funsql_define_sex() :
            arg == :smoking ? funsql_define_smoking() :
            arg == :soarian_mrn ? funsql_define_soarian_mrn() :
            arg == :translator ? funsql_define_translator() :
            @error("unknown define $arg")
        end
    end
    query
end
