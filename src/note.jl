@funsql begin

note() = begin
    from(note)
    as(omop)
    define(
        domain_id => "Note",
        occurrence_id => omop.note_id,
        person_id => omop.person_id,
        datetime => omop.note_datetime,
        concept_id => omop.note_class_concept_id,
        type_concept_id => omop.note_type_concept_id,
        title => omop.note_title,
        text => omop.note_text,
        provider_id => omop.provider_id,
        visit_occurrence_id => omop.visit_occurrence_id)
    join(
        person => person(),
        person_id == person.person_id,
        optional = true)
    join(
        concept => concept(),
        concept_id == concept.concept_id,
        optional = true)
    left_join(
        type_concept => concept(),
        type_concept_id == type_concept.concept_id,
        optional = true)
    left_join(
        provider => provider(),
        provider_id == provider.provider_id,
        optional = true)
    left_join(
        visit => visit(),
        visit_occurrence_id == visit.occurrence_id,
        optional = true)
end

note(match...) =
    note().filter(concept_matches($match))

end
