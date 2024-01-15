@funsql begin

visit() = begin
    from(visit_occurrence)
    as(omop)
    define(
        # event columns
        domain_id => "Visit",
        occurrence_id => omop.visit_occurrence_id,
        person_id => omop.person_id,
        concept_id => omop.visit_concept_id,
        datetime => coalesce(omop.visit_start_datetime,
                             timestamp(omop.visit_start_date)),
        datetime_end => coalesce(omop.visit_end_datetime,
                                 timestamp(omop.visit_end_date)),
        type_concept_id => omop.visit_type_concept_id,
        provider_id => omop.provider_id,
        # domain specific columns
        omop.care_site_id,
        omop.admitted_from_concept_id,
        omop.discharged_to_concept_id,
        omop.preceding_visit_occurrence_id)
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
    cross_join(
        ext => begin
            # computed variables
            select(
                is_preepic => :ID > 1000000000)
            bind(
                :ID => omop.visit_occurrence_id)
        end)
end

visit(match...) =
    visit().filter(concept_matches($match))

visit_isa(args...) = category_isa($Visit, $args, omop.visit_concept_id)

exists_visit(match...; having=nothing) =
    exists(begin
        visit($match...)
        filter(person_id == :person_id)
        $(isnothing(having) ? @funsql(define()) : @funsql(filter($having)))
        bind(:person_id => person_id)
    end)

end
