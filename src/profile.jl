function funsql_define_age_at_extraction_or_death()
    @funsql begin
        if_not_defined(_person,
            join(_person => person(), _person.person_id == person_id))
        define(age_at_extraction_or_death=> age_at_extraction_or_death(_person))
    end
end

function funsql_define_race()
    @funsql begin
        if_not_defined(_person,
            join(_person => person(), _person.person_id == person_id))
        define(
            race =>
                _person.race_concept_id == 0 ?
                ( _person.ethnicity_concept_id == 38003563 ?
                "Unspecified (Hispanic)" :
                "Unspecified") :
                _person.race_concept.concept_name)
    end
end

function funsql_define_ethnicity()
    @funsql begin
        if_not_defined(_person,
            join(_person => person(), _person.person_id == person_id))
        define(
            ethnicity =>
                _person.ethnicity_concept_id == 0 ?
                "Unspecified" :
                _person.ethnicity_concept.concept_name)
    end
end

function funsql_define_sex()
    @funsql begin
        if_not_defined(_person,
            join(_person => person(), _person.person_id == person_id))
        define(
            sex =>
                _person.gender_concept_id == 0 ? "" :
                _person.gender_concept.concept_code)
    end
end

function funsql_define_birth_date(;name=:birth_date)
    @funsql begin
        if_not_defined(_person,
            join(_person => person(), _person.person_id == person_id))
        define($name => date(_person.birth_datetime))
    end
end

function funsql_define_birth_year(;name=:birth_year)
    @funsql begin
        if_not_defined(_person,
            join(_person => person(), _person.person_id == person_id))
        define($name =>
            if_defined_scalar(year_of_birth, year_of_birth,
                              _person.omop.year_of_birth))
    end
end

function funsql_define_death_date(;name=:death_date)
    @funsql begin
        if_not_defined(_death,
            left_join(_death => from(death), _death.person_id == person_id))
        define($name => _death.death_date)
    end
end

function funsql_define_death_year(;name=:death_year)
    @funsql begin
        if_not_defined(_death,
            left_join(_death => from(death), _death.person_id == person_id))
        define($name => year(_death.death_date))
    end
end

function funsql_define_soarian_mrn()
    @funsql begin
        left_join(_person_map =>
            from(`trdwlegacysoarian.omop_common_person_map`),
            _person_map.person_id == person_id)
        with(
            `trdwlegacysoarian.omop_common_person_map` =>
                from($(FunSQL.SQLTable(qualifiers = [:ctsi, :trdwlegacysoarian],
                                    name = :omop_common_person_map,
                                    columns = [:person_id, :mrn]))))
        define(soarian_mrn => _person_map.mrn)
    end
end

function funsql_define_epic_mrn()
    @funsql begin
        if_not_defined(_person,
            join(_person => person(), _person.person_id == person_id))
        left_join(_patient => begin
            from(`epicclarity.patient`)
        end, _patient.pat_id == _person.pat_id)
        with(
            `epicclarity.patient` =>
                from($(FunSQL.SQLTable(qualifiers = [:main, :epicclarity],
                                       name = :patient,
                                       columns = [:pat_id, :pat_mrn_id]))))
        define(epic_mrn => _patient.pat_mrn_id)
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
            join(p => person().filter(gender_concept_id ==8532),
                 p.person_id == fact_id_2)
        end, $filter)
        define($name => first($name.fact_id_2))
    end
end

function funsql_define_profile(args...)
    if length(args) == 0
        args = [
            :age_at_extraction_or_death,
            :birth_date,
            :birth_year,
            :death_date,
            :death_year,
            :epic_mrn,
            :ethnicity,
            :never_smoker,
            :preferred_language,
            :race,
            :sex,
            :smoking,
            :soarian_mrn,
            :translator,
        ]
    end
    query = @funsql(define())
    for arg in args
        query = query |> begin
            arg == :birth_date ? funsql_define_birth_date() :
            arg == :birth_year ? funsql_define_birth_year() :
            arg == :age_at_extraction_or_death ? funsql_define_age_at_extraction_or_death() :
            arg == :death_date ? funsql_define_death_date() :
            arg == :death_year ? funsql_define_death_year() :
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
