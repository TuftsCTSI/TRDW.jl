@funsql begin

visit_detail(match...) = begin
    from(visit_detail)
    $(length(match) == 0 ? @funsql(define()) : @funsql(filter(visit_detail_matches($match))))
    left_join(person => person(),
              person_id == person.person_id, optional=true)
    left_join(care_site => care_site(),
              care_site_id == care_site.care_site_id, optional=true)
    left_join(location => location(),
              location.location_id == care_site.location_id, optional=true)
    left_join(provider => provider(),
              provider.provider_id == provider.provider_id, optional=true)
    left_join(visit_occurrence => visit_occurrence(),
              visit_occurrence_id == visit_occurrence.visit_occurrence_id, optional=true)
    join(event => begin
        from(visit_detail)
        define(
            concept_id => visit_detail_concept_id,
            current_age => nvl(datediff_year(person.birth_datetime, visit_detail_start_date),
                            year(visit_detail_start_date) - person.year_of_birth),
            end_datetime => coalesce(visit_detail_end_datetime,
                                     end_of_day(visit_detail_end_date)),
            is_historical => visit_detail_occurrence_id > 1000000000,
            start_datetime => coalesce(visit_detail_start_datetime,
                                       to_timestamp(visit_detail_start_date)))
    end, visit_detail_id == event.visit_detail_id, optional = true)
end

visit_detail_matches(match...) = concept_matches($match; match_prefix=visit_detail)

end
