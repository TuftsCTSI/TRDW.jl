@funsql begin

note() = begin
    from(note)
end

join_note_class_via_cohort(match...; exclude=nothing) = begin
    join_via_cohort(
        $(@funsql begin
            from(note)
            select(note_id, person_id, note_date, note_class_concept_id)
        end), note_date; match_prefix=note_class, match=$match)
    $(isnothing(exclude) ? @funsql(define()) :
      @funsql(filter(!concept_matches($exclude; match_prefix=note_class))))
    define(concept_id => note_class_concept_id)
end

end
