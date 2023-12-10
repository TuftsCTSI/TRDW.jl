@funsql begin

measurement(match...) = begin
    from(measurement)
    $(length(match) == 0 ? @funsql(define()) : @funsql(filter(measurement_matches($match))))
    left_join(person => person(),
              person_id == person.person_id, optional=true)
    left_join(visit => visit_occurrence(),
        visit_occurrence_id == visit_occurrence.visit_occurrence_id, optional = true)
    join(event => begin
        from(measurement)
        define(
            table_name => "measurement",
            concept_id => measurement_concept_id,
            end_date => measurement_date,
            is_historical => measurement_id > 1500000000,
            start_date => measurement_date,
            source_concept_id => measurement_source_concept_id)
    end, measurement_id == event.measurement_id, optional = true)
end

measurement_matches(match...) = concept_matches($match; match_prefix=measurement)

measurement_pivot(match...; event_total=true, person_total=true, roundup=true) = begin
    join_via_cohort(measurement(), measurement_date;
                    match_prefix=measurement, match=$match)
    pairing_pivot($match, measurement, measurement_id;
                  event_total=$event_total, person_total=$person_total, roundup=$roundup)
end

join_cohort_on_measurement(match...; exclude=nothing, carry=nothing) = begin
    join_via_cohort(measurement(), measurement_date; match_prefix=measurement,
                    match=$match, exclude=$exclude, carry=$carry)
end

truncate_to_loinc_class(name=nothing) = 
    truncate_to_concept_class($name, "LOINC Class")
truncate_to_loinc_group(name=nothing) = 
    truncate_to_concept_class($name, "LOINC Group")
truncate_to_loinc_hierarchy(name=nothing) = 
    truncate_to_concept_class($name, "LOINC Hierarchy")

end
