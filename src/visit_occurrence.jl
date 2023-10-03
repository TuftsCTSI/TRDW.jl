@funsql begin

# TODO: fix this logic after TRDW update
is_primary_diagnosis() =
    (condition_status_concept_id == 4230359 &&
     condition_type_concept_id == 44786627)

is_inpatient() = # includes preadmit
    filter(visit_concept_id == 9201)

visit_date_overlaps(start, finish) =
    (visit_start_date <= date($finish) && date($start) <= visit_end_date)

with_visit_group(extension=nothing) =
    join(visit_group => begin
      from(visit_occurrence)
      $(extension == nothing ? @funsql(define()) : extension)
      group(person_id)
    end, person_id == visit_group.person_id)

end
