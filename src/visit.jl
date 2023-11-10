@funsql begin

visit_occurrence(match...) = begin
    from(visit_occurrence)
    $(length(match) == 0 ? @funsql(define()) : @funsql(filter(visit_matches($match))))
    left_join(person => person(),
              person_id == person.person_id, optional=true)
    left_join(care_site => care_site(),
              care_site_id == care_site.care_site_id, optional=true)
    left_join(location => location(),
              location.location_id == care_site.location_id, optional=true)
    join(event => begin
        from(visit_occurrence)
        define(
            concept_id => visit_concept_id,
            current_age => nvl(datediff_year(person.birth_datetime, visit_start_date),
                            year(visit_start_date) - person.year_of_birth),
            end_date => visit_end_date,
            is_historical => visit_occurrence_id > 1000000000,
            start_date => visit_start_date)
    end, visit_occurrence_id == event.visit_occurrence_id, optional = true)
end

visit_matches(match...) = concept_matches($match; match_prefix=visit)

visit_pivot(match...; event_total=true, person_total=true, roundup=true) = begin
    join_via_cohort(visit_occurrence(), visit; match=$match)
    pairing_pivot($match, visit, visit_occurrence_id;
                  event_total=$event_total, person_total=$person_total, roundup=$roundup)
end

join_visit_via_cohort(match...; exclude=nothing, carry=nothing) = begin
    join_via_cohort(visit_occurrence(), visit; match=$match, exclude=$exclude, carry=$carry)
end

end
