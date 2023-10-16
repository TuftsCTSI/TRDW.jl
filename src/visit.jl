@funsql begin

visit_occurrence() = begin
    from(visit_occurrence)
    left_join(person => person(),
              person_id == person.person_id, optional=true)
    left_join(care_site => care_site(),
              care_site_id == care_site.care_site_id, optional=true)
    left_join(location => location(),
              location.location_id == care_site.location_id, optional=true)
	define(
        age => nvl(datediff_year(person.birth_datetime, visit_start_date),
               year(visit_start_date) - person.year_of_birth),
        is_historical => visit_occurrence_id > 1000000000)
end

visit_date_overlaps(start, finish) =
    (visit_start_date <= date($finish) && date($start) <= visit_end_date)

join_visit(ids...; carry=[]) = begin
    as(base)
    join(begin
        visit_occurrence()
        $(length(ids) == 0 ? @funsql(define()) :
            @funsql filter(is_descendant_concept(visit_concept_id, $ids...)))
    end, base.person_id == person_id)
    define($([@funsql($n => base.$n) for n in carry]...))
end

correlated_visit(ids...) = begin
    visit_occurrence()
	filter(person_id == :person_id)
    $(length(ids) == 0 ? @funsql(define()) :
        @funsql filter(is_descendant_concept(visit_concept_id, $ids...)))
	bind(:person_id => person_id )
end

with_visit_group(extension=nothing) =
    join(visit_group => begin
        visit_occurrence()
        $(extension == nothing ? @funsql(define()) : extension)
        group(person_id)
    end, person_id == visit_group.person_id)

end
