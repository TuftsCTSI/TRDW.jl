@funsql begin

note() = begin
    from(note)
end

join_cohort_on_note(match...; exclude=nothing, carry=nothing) = begin
    join_via_cohort(from(note), note_date; match_prefix=note_class,
                    match=$match, carry=$carry, exclude=$exclude)
end

note_pivot(match...; event_total=true, person_total=true, roundup=true) = begin
    join_via_cohort(from(note), note_date; match_prefix=note_class, match=$match)
    pairing_pivot($match, note, note_id,
                  event_total=$event_total, person_total=$person_total, roundup=$roundup)
end

end
