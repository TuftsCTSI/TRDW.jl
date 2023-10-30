@funsql begin

person() = begin
    from(person)
    left_join(death => from(death),
		person_id == death.person_id, optional=true)
    left_join(race => from(concept),
		race_concept_id == race.concept_id, optional=true)
    left_join(ethnicity => from(concept),
		ethnicity_concept_id == ethnicity.concept_id, optional=true)
    left_join(gender => from(concept),
		gender_concept_id == gender.concept_id, optional=true)
end

person_current_age() = nvl(datediff_year(birth_datetime, now()), year(now()) - year_of_birth)

race_isa(args...) = category_isa($Race, $args, race_concept_id)
ethnicity_isa(args...) = category_isa($Ethnicity, $args, ethnicity_concept_id)

with_group(pair::Pair{Symbol, FunSQL.SQLNode}; mandatory = true) = begin
    left_join($(pair[1]) => begin
        $(pair[2])
        group(person_id)
    end, person_id == $(pair[1]).person_id)
    $(mandatory ? @funsql(filter(not(is_null($(pair[1]).person_id)))) : @funsql(define()))
end

with_group(node::FunSQL.SQLNode) = 
    $(let name = gensym(); @funsql(with_group($name => $node)) end)

join_by_person(next::FunSQL.SQLNode; carry=[]) = begin
    as(base)
    join($next, base.person_id == person_id)
    define($([@funsql($n => base.$n) for n in carry]...))
end

count_n_person(; roundup=true) = begin
    define(n_person => count())
    order(n_person.desc())
    define(n_person => roundups(n_person, $roundup))
end

stratify_by_age(; roundup=true) = begin
    join(p => from(person), p.person_id == person_id)
    define(age => 2023 - p.year_of_birth)
    group(age => case(
        age >= 80, "80+",
        age >= 70, "70-79",
        age >= 60, "60-69",
        age >= 50, "50-59",
        age >= 40, "40-49",
        age >= 30, "30-39",
        "29 or less"))
    count_n_person(; roundup=$roundup)
    order(age)
    select(n_person, age)
end

stratify_by_race(; roundup=true) = begin
    join(p => person(), p.person_id == person_id)
    filter(p.race_concept_id > 0 )
    group(race => p.race.concept_name)
    count_n_person(; roundup=$roundup)
    select(n_person, race)
end

stratify_by_ethnicity(; roundup=true) = begin
    join(p => person(), p.person_id == person_id)
    filter(p.ethnicity_concept_id > 0 )
    group(ethnicity => p.ethnicity.concept_name)
    count_n_person(; roundup=$roundup)
    select(n_person, ethnicity)
end

end
