@funsql begin

person() = begin
    from(person)
    left_join(person_map => from(person_map),
		person_id == person_map.person_id, optional=true)
    left_join(soarian_person_map => from(soarian_person_map),
		person_id == soarian_person_map.person_id, optional=true)
    left_join(death => from(death),
		person_id == death.person_id, optional=true)
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

# TODO: how to make this an error when it doesn't match?
filter_by_provider_specialty(name...) = begin
    filter(in(provider_id, begin
        from(provider)
        filter(in(specialty_concept_id, begin
            concept()
            filter(domain_id == "Provider" &&
                 in(concept_name, $(name...)))
            select(concept_id)
        end))
        select(provider_id)
    end))
end

end
