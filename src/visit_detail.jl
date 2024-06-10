@funsql begin

visit_detail() = begin
    from(visit_detail)
    define(is_preepic => visit_detail_id > 1000000000)
    as(omop)
    define(
        # event columns
        domain_id => "VisitDetail",
        occurrence_id => omop.visit_detail_id,
        person_id => omop.person_id,
        concept_id => omop.visit_detail_concept_id,
        datetime => coalesce(omop.visit_detail_start_datetime,
                             timestamp(omop.visit_detail_start_date)),
        datetime_end => coalesce(omop.visit_detail_end_datetime,
                                 timestamp(omop.visit_detail_end_date)),
        type_concept_id => omop.visit_detail_type_concept_id,
        provider_id => omop.provider_id,
        visit_occurrence_id => omop.visit_occurrence_id,
        # domain specific columns
        omop.care_site_id,
        omop.admitted_from_concept_id,
        omop.discharged_to_concept_id,
        omop.parent_visit_detail_id,
        omop.preceding_visit_detail_id)
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
        care_site => care_site(),
        omop.care_site_id == care_site.care_site_id,
        optional = true)
    left_join(
        admitted_from_concept => concept(),
        omop.admitted_from_concept_id == admitted_from_concept.concept_id,
        optional = true)
    left_join(
        discharged_to_concept => concept(),
        omop.discharged_to_concept_id == discharged_to_concept.concept_id,
        optional = true)
    left_join(
        visit => visit(),
        visit_occurrence_id == visit.occurrence_id,
        optional = true)
end

visit_detail(match...) =
    visit_detail().filter(concept_matches($match))

visit_detail_isa(args...) = category_isa($Visit, $args, omop.visit_detail_concept_id)

end
