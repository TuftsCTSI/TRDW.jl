@funsql begin

person() = begin
    from(person)
    left_join(person_map => from(person_map),
		person_id == person_map.person_id, optional=true)
    left_join(soarian_person_map => from(soarian_person_map),
		person_id == soarian_person_map.person_id, optional=true)
    left_join(death => from(death),
		person_id == death.person_id, optional=true)
	define(
		gender =>
			gender_concept_id == 8507 ? "M" :
			gender_concept_id == 8532 ? "F" :
			missing,
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
