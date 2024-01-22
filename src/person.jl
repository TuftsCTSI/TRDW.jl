@funsql begin

person() = begin
    from(person)
    left_join(
        death => from(death),
        person_id == death.person_id,
        optional = true)
    as(omop)
    define(
        person_id => omop.person_id,
        gender_concept_id => omop.gender_concept_id,
        birth_datetime => coalesce(omop.birth_datetime,
                              timestamp(
                                  make_date(omop.year_of_birth,
                                  coalesce(omop.month_of_birth, 1),
                                  coalesce(omop.day_of_birth, 1)))),
        death_datetime => coalesce(omop.death.death_datetime,
                              timestamp(omop.death.death_date)),
        death_concept_id =>
            case(is_not_null(omop.death.person_id),
                 coalesce(omop.death.cause_concept_id, 0)),
        death_type_concept_id => omop.death.death_type_concept_id,
        race_concept_id => omop.race_concept_id,
        ethnicity_concept_id => omop.ethnicity_concept_id,
        location_id => omop.location_id,
        provider_id => omop.provider_id,
        care_site_id => omop.care_site_id)
    join(
        gender_concept => concept(),
        gender_concept_id == gender_concept.concept_id,
        optional = true)
    left_join(
        death_concept => concept(),
        death_concept_id == death_concept.concept_id,
        optional = true)
    left_join(
        race_concept => concept(),
        race_concept_id == race_concept.concept_id,
        optional = true)
    left_join(
        ethnicity_concept => concept(),
        ethnicity_concept_id == ethnicity_concept.concept_id,
        optional = true)
    left_join(
        location => location(),
        location_id == location.location_id,
        optional = true)
    left_join(
        provider => provider(),
        provider_id == provider.provider_id,
        optional = true)
    left_join(
        care_site => care_site(),
        care_site_id == care_site.care_site_id,
        optional = true)
end

is_deceased() = (:is_deceased => isnotnull(omop.death.person_id))

current_age() = (:current_age => datediff_year(birth_datetime, nvl(death_datetime, now())))
current_age(p) = (:current_age => datediff_year($p.birth_datetime, nvl($p.death_datetime, now())))

race_isa(args...) = category_isa($Race, $args, race_concept_id)
ethnicity_isa(args...) = category_isa($Ethnicity, $args, ethnicity_concept_id)

end
