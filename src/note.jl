@funsql begin

note() = begin
    from(note)
    define(is_preepic => note_id > 1500000000)
    as(omop)
    define(
        # event columns
        domain_id => "Note",
        occurrence_id => omop.note_id,
        person_id => omop.person_id,
        concept_id => omop.note_class_concept_id,
        datetime => coalesce(omop.note_datetime,
                             timestamp(omop.note_date)),
        datetime_end => missing,
        type_concept_id => omop.note_type_concept_id,
        provider_id => omop.provider_id,
        visit_occurrence_id => omop.visit_occurrence_id,
        # domain specific columns
        omop.note_title,
        omop.note_text)
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
