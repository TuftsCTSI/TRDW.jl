@funsql begin

person() = begin
    from(person)
    left_join(person_map => from(person_map),
		person_id == person_map.person_id, optional=true)
    left_join(soarian_person_map => from(soarian_person_map),
		person_id == soarian_person_map.person_id, optional=true)
    left_join(death => from(death),
		person_id == death.person_id, optional=true)
    left_join(race => from(concept),
		race_concept_id == race.concept_id, optional=true)
    left_join(ethnicity => from(concept),
		ethnicity_concept_id == ethnicity.concept_id, optional=true)
    left_join(gender => from(concept),
		gender_concept_id == gender.concept_id, optional=true)
	define(
	    soarian_mrn => soarian_person_map.soarian_mrn,
        epic_pat_id => person_map.person_source_value,
		deceased => death.death_date
	)
end

race_isa(args...) = category_isa($Race, $args, race_concept_id)
ethnicity_isa(args...) = category_isa($Ethnicity, $args, ethnicity_concept_id)

with_group(name, subquery) = begin
    join($name => begin
        $subquery
        group(person_id)
    end, person_id == $name.person_id)
end

stratify_by_age() = begin
    join(p => from(person), p.person_id == person_id)
    define(age => 2023 - p.year_of_birth)
    define(age => case(
        age >= 80, "80+",
        age >= 70, "70-79",
        age >= 60, "60-69",
        age >= 50, "50-59",
        age >= 40, "40-49",
        age >= 30, "30-39",
        "29 or less"))
    group(age)
    order(age)
    select(count(), age)
end

stratify_by_race() = begin
    join(p => person(), p.person_id == person_id)
    filter(p.race_concept_id > 0 )
    group(race => p.race.concept_name)
    filter(count()>9)
    order(count().desc())
    select(count(), race)
end

stratify_by_ethnicity() = begin
    join(p => person(), p.person_id == person_id)
    filter(p.ethnicity_concept_id > 0 )
    group(ethnicity => p.ethnicity.concept_name)
    filter(count()>9)
    order(count().desc())
    select(count(), ethnicity)
end

end
