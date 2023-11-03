@funsql begin

measurement(match...) = begin
    from(measurement)
    $(length(match) == 0 ? @funsql(define()) : @funsql(filter(measurement_matches($match))))
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

join_measurement_via_cohort(match...; exclude=nothing) = begin
    join_via_cohort(measurement(), measurement_date;
                    match_prefix=measurement, match=$match)
    $(isnothing(exclude) ? @funsql(define()) :
      @funsql(filter(!measurement_matches($exclude))))
    define(concept_id => coalesce(measurement_source_concept_id, measurement_concept_id))
end

end
