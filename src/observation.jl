@funsql begin

observation(match...) = begin
    from(observation)
    $(length(match) == 0 ? @funsql(define()) : @funsql(observation_matches($match)))
    left_join(visit_occurrence => visit_occurrence(),
              visit_occurrence_id == visit_occurrence.visit_occurrence_id, optional = true)
    define(is_historical => observation_id > 1500000000)
end

observation_matches(match...) = concept_matches($match; match_prefix=observation)

observation_pivot(match...; event_total=true, person_total=true, roundup=true) = begin
    join_via_cohort(observation(), observation_date;
                    match_prefix=observation, match=$match)
    pairing_pivot($match, observation, observation_id;
                  event_total=$event_total, person_total=$person_total, roundup=$roundup)
end

join_observation_via_cohort(match...; exclude=nothing) = begin
    join_via_cohort(observation(), observation_date;
                    match_prefix=observation, match=$match)
    $(isnothing(exclude) ? @funsql(define()) :
      @funsql(filter(!observation_matches($exclude))))
    define(concept_id => coalesce(observation_source_concept_id, observation_concept_id))
end

end
