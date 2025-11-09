@funsql begin

demographics(; roundup=$(is_discovery())) = begin
    as(cohort)
    over(
        append(
            from(cohort).stratify_by(roundup = $roundup).select(label => "Total", category => missing, n_person),
            from(cohort).stratify_by_age(roundup = $roundup).select(label => "Age", category => age, n_person),
            from(cohort).stratify_by_sex(roundup = $roundup).select(label => "Sex", category => sex_name, n_person),
            from(cohort).stratify_by_race(roundup = $roundup).select(label => "Race", category => race_name, n_person),
            from(cohort).stratify_by_ethnicity(roundup = $roundup).select(label => "Ethnicity", category => ethnicity_name, n_person)))
    format(group_by = label)
end

define_n_person(; roundup=$(is_discovery())) = begin
    define(n_person => count_distinct(person_id))
    order(n_person.desc(nulls=last))
    define(n_person => roundups(n_person; round=$roundup))
end

stratify_by(groups...; roundup=$(is_discovery())) = begin
    group($groups...)
    define_n_person(; roundup=$roundup)
    define(n_event => count(person_id))
    define(n_event => roundups(n_event; round=$roundup))
end

stratify_by_age(; roundup=$(is_discovery())) = begin
    deduplicate(person_id)
    as(base)
    join(person(), base.person_id == person_id)
    define(age => age_at_extraction_or_death())
    group(age => case(
        age >= 90, "90+",
        age >= 80, "80-89",
        age >= 70, "70-79",
        age >= 70, "70-79",
        age >= 60, "60-69",
        age >= 50, "50-59",
        age >= 40, "40-49",
        age >= 30, "30-39",
        age >= 20, "20-29",
        "19 or less"))
    define_n_person(; roundup=$roundup)
    order(age)
    select(n_person, age)
end

stratify_by_age(pair::Pair{Symbol, FunSQL.SQLQuery}; roundup) =
    $(pair[2]).stratify_by_age(; roundup=$roundup)

stratify_by_race(; roundup=$(is_discovery())) = begin
    deduplicate(person_id)
    as(base)
    join(from(person), base.person_id == person_id)
    left_join(race => from(concept),
        race_concept_id == race.concept_id)
    define(race_name =>
        race_concept_id == 0 ?
        ( ethnicity_concept_id == 38003563 ? "Unspecified (Hispanic)" :
          "Unspecified") :
        race.concept_name)
    group(race_name)
    define_n_person(; roundup=$roundup)
    select(n_person, race_name)
end

stratify_by_race(pair::Pair{Symbol, FunSQL.SQLQuery}; roundup) =
    $(pair[2]).stratify_by_race(; roundup=$roundup)

stratify_by_sex(; roundup=$(is_discovery())) = begin
    deduplicate(person_id)
    as(base)
    join(from(person), base.person_id == person_id)
    left_join(sex => from(concept), gender_concept_id == sex.concept_id)
    define(sex_name => gender_concept_id == 0 ? "Unspecified" : sex.concept_name)
    group(sex_name)
    define_n_person(; roundup=$roundup)
    select(n_person, sex_name)
end

stratify_by_sex(pair::Pair{Symbol, FunSQL.SQLQuery}; roundup) =
    $(pair[2]).stratify_by_sex(; roundup=$roundup)

stratify_by_ethnicity(; roundup=$(is_discovery())) = begin
    deduplicate(person_id)
    as(base)
    join(from(person), base.person_id == person_id)
    left_join(ethnicity => from(concept),
        ethnicity_concept_id == ethnicity.concept_id)
    define(ethnicity_name =>
        ethnicity_concept_id == 0 ?
        "Unspecified" : ethnicity.concept_name)
    group(ethnicity_name)
    define_n_person(; roundup=$roundup)
    select(n_person, ethnicity_name)
end

stratify_by_ethnicity(pair::Pair{Symbol, FunSQL.SQLQuery}; roundup) =
    $(pair[2]).stratify_by_ethnicity(; roundup=$roundup)

stratify_by_translator(; roundup=$(is_discovery())) = begin
	deduplicate(person_id)
    define_profile(translator)
	group(translator)
	define_n_person(; roundup=$roundup)
    select(n_person, translator)
end

stratify_by_preferred_language(; roundup=$(is_discovery())) = begin
	deduplicate(person_id)
    define_profile(preferred_language)
    group(preferred_language)
	define_n_person(; roundup=$roundup)
    select(n_person, preferred_language)
end

end
