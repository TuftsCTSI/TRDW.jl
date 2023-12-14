@funsql begin

note() = begin
    from(note)
    as(omop)
    define(
        domain_id => "Note",
        occurrence_id => omop.note_id,
        person_id => omop.person_id,
        datetime => omop.note_datetime,
        type_concept_id => omop.note_type_concept_id,
        class_concept_id => omop.note_class_concept_id,
        title => omop.note_title,
        text => omop.note_text,
        provider_id => omop.provider_id,
        visit_occurrence_id => omop.visit_occurrence_id)
end

note(match...) =
    note().filter(concept_matches($match))

end
