@funsql begin

person() = begin
    from(person)
    left_join(
        death => from(death),
        person_id == death.person_id,
        optional = true)
    as(omop)
    define(
        person_id => omop.person_id,
        gender_concept_id => omop.gender_concept_id,
        birth_datetime => omop.birth_datetime,
        death_datetime => omop.death.death_datetime,
        death_concept_id =>
            case(is_not_null(omop.death.person_id), coalesce(omop.death.cause_concept_id, 0)),
        death_type_concept_id => omop.death.death_type_concept_id,
        race_concept_id => omop.race_concept_id,
        ethnicity_concept_id => omop.ethnicity_concept_id,
        location_id => omop.location_id,
        provider_id => omop.provider_id,
        care_site_id => omop.care_site_id)
    join(
        gender_concept => concept(),
        concept_id == gender_concept.concept_id,
        optional = true)
    left_join(
        death_concept => concept(),
        death_concept_id == death_concept.concept_id,
        optional = true)
    left_join(
        race_concept => concept(),
        race_concept_id == race_concept.concept_id,
        optional = true)
    left_join(
        ethnicity_concept => concept(),
        ethnicity_concept_id == ethnicity_concept.concept_id,
        optional = true)
    left_join(
        location => location(),
        location_id == location.location_id,
        optional = true)
    left_join(
        provider => provider(),
        provider_id == provider.provider_id,
        optional = true)
    left_join(
        care_site => care_site(),
        care_site_id == care_site.care_site_id,
        optional = true)
end

is_deceased() = isnotnull(omop.death.person_id)

person_current_age() = nvl(datediff_year(birth_datetime, nvl(death_datetime, now()), 
                                         year(nvl(death_datetime, now()) - year_of_birth)))

race_isa(args...) = category_isa($Race, $args, race_concept_id)
ethnicity_isa(args...) = category_isa($Ethnicity, $args, ethnicity_concept_id)

with_group(pair::Pair{Symbol, FunSQL.SQLNode}; mandatory = true, exclude = false) = begin
    left_join($(pair[1]) => begin
        $(pair[2])
        group(person_id)
    end, person_id == $(pair[1]).person_id)
    $(exclude ? @funsql(filter(is_null($(pair[1]).person_id))) :
      mandatory ? @funsql(filter(not(is_null($(pair[1]).person_id)))) :
      @funsql(define()))
end

with_group(node::FunSQL.SQLNode; mandatory=true, exclude=false) =
    $(let name = gensym();
      @funsql(with_group($name => $node; mandatory=$mandatory, exclude=$exclude)) end)

join_by_person(next::FunSQL.SQLNode; carry=[]) = begin
    as(base)
    join($next, base.person_id == person_id)
    define($([@funsql($n => base.$n) for n in carry]...))
end

count_n_person(; roundup=true) = begin
    define(n_person => count_distinct(person_id))
    order(n_person.desc())
    define(n_person => roundups(n_person, $roundup))
end

cohort_count(; roundup=true) = begin
    count_n_person(;roundup=$roundup)
    define(n_event => count(person_id))
    define(n_event => roundups(n_event, $roundup))
end

stratify_by_age(; roundup=true) = begin
    deduplicate(person_id)
    as(base)
    join(person(), base.person_id == person_id)
    define(age => nvl(year(death.death_date), 2023) - year_of_birth)
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
    count_n_person(; roundup=$roundup)
    order(age)
    select(n_person, age)
end

stratify_by_race(; roundup=true) = begin
    deduplicate(person_id)
    as(base)
    join(from(person), base.person_id == person_id)
    left_join(race => from(concept),
        race_concept_id == race.concept_id)
    define(race_name =>
        race_concept_id == 0 ?
        "Unspecified" : race.concept_name)
    group(race_name)
    count_n_person(; roundup=$roundup)
    select(n_person, race_name)
end

stratify_by_ethnicity(; roundup=true) = begin
    deduplicate(person_id)
    as(base)
    join(from(person), base.person_id == person_id)
    left_join(ethnicity => from(concept),
        ethnicity_concept_id == ethnicity.concept_id)
    define(ethnicity_name =>
        ethnicity_concept_id == 0 ?
        "Unspecified" : ethnicity.concept_name)
    group(ethnicity_name)
    count_n_person(; roundup=$roundup)
    select(n_person, ethnicity_name)
end

end
