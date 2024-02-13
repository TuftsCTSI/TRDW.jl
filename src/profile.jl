function funsql_define_current_age()
    person = gensym()
    @funsql begin
        join($person => person(), $person.person_id == person_id)
        define(age=> current_age($person))
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
        undefine($person)
    end
end

function funsql_define_birth_year(;name=:birth_year)
    person = gensym()
    @funsql begin
        join($person => from(person), $person.person_id == person_id)
        define($name => $person.year_of_birth)
        undefine($person)
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
    person = gensym()
    patient = gensym()
    @funsql begin
        join($person => from(person), $person.person_id == person_id)
        left_join($patient => begin
            from(`global.patient`)
            group(system_epic_id)
            define(mrn => array_join(collect_set(system_epic_mrn), ";"))
        end, $patient.system_epic_id == $person.person_source_value)
        with(
            `global.patient` =>
                from($(FunSQL.SQLTable(qualifiers = [:main, :global],
                                       name = :patient,
                                       columns = [:id, :system_epic_id, :system_epic_mrn]))))
        define(epic_mrn => $patient.mrn)
    end
end

function funsql_define_profile(args...)
    query = @funsql(define())
    for arg in args
        query = query |> begin
            arg == :current_age ? funsql_define_current_age() :
            arg == :epic_mrn ? funsql_define_epic_mrn() :
            arg == :soarian_mrn ? funsql_define_soarian_mrn() :
            arg == :birth_date ? funsql_define_birth_date() :
            arg == :death_date ? funsql_define_death_date() :
            arg == :birth_year ? funsql_define_birth_year() :
            arg == :death_year ? funsql_define_death_year() :
            arg == :sex ? funsql_define_sex() :
            arg == :ethnicity ? funsql_define_ethnicity() :
            arg == :race ? funsql_define_race() :
            @error("unknown define $arg")
        end
    end
    query
end
