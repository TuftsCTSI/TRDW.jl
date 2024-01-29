@funsql begin

count_n_person(; roundup=true, order_by=[]) = begin
    $(let
        order_by = length(order_by) > 0 ?
            [@funsql($col.desc(nulls=last)) for col in order_by] :
             @funsql(n_person.desc(nulls=last));
        @funsql(begin
            define(n_person => count_distinct(person_id))
            order($order_by...)
            define(n_person => roundups(n_person; round=$roundup))
        end)
    end)
end

count_n_person(pair::Pair{Symbol, FunSQL.SQLNode}; roundup) =
    $(pair[2]).count_n_person(; roundup=$roundup)

cohort_count(; roundup=true) = begin
    group()
    count_n_person(; roundup=$roundup)
    define(n_event => count(person_id))
    define(n_event => roundups(n_event; round=$roundup))
    define(n_days => count_distinct(`struct`(person_id, date(datetime))))
    define(n_days => roundups(n_days; round=$roundup))
end

cohort_count(pair::Pair{Symbol, FunSQL.SQLNode}; roundup) =
    $(pair[2]).cohort_count(; roundup=$roundup)

stratify_by_age(; roundup=true) = begin
    deduplicate(person_id)
    as(base)
    join(person(), base.person_id == person_id)
    define(age => current_age())
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

stratify_by_age(pair::Pair{Symbol, FunSQL.SQLNode}; roundup) =
    $(pair[2]).stratify_by_age(; roundup=$roundup)

stratify_by_race(; roundup=true) = begin
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
    count_n_person(; roundup=$roundup)
    select(n_person, race_name)
end

stratify_by_race(pair::Pair{Symbol, FunSQL.SQLNode}; roundup) =
    $(pair[2]).stratify_by_race(; roundup=$roundup)

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

stratify_by_ethnicity(pair::Pair{Symbol, FunSQL.SQLNode}; roundup) =
    $(pair[2]).stratify_by_ethnicity(; roundup=$roundup)

end
