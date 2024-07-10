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

pat_id() =
    (:pat_id =>
         if_defined_scalar(person, person.omop.person_source_value,
                           if_defined_scalar(omop, omop.person_source_value,
                                             person_source_value)))

is_deceased() = 
    (:is_deceased =>
     isnotnull(if_defined_scalar(person, person.omop.death.person_id, omop.death.person_id)))

zipcode() = if_defined_scalar(location, location.zip, zip)

age_at_extraction_or_death() = (:age_at_extraction_or_death => datediff_year(birth_datetime, nvl(death_datetime, from(cdm_source).select(source_release_date))))
age_at_extraction_or_death(p) = (:age_at_extraction_or_death => datediff_year($p.birth_datetime, nvl($p.death_datetime, from(cdm_source).select(source_release_date))))

race_isa(args...) = category_isa($Race, $args, race_concept_id)
ethnicity_isa(args...) = category_isa($Ethnicity, $args, ethnicity_concept_id)

define_pat_id() = begin
    left_join(person_map => begin
        from($(FunSQL.SQLTable(qualifiers = [:ctsi, :person_map],
                            name = :person_map,
                            columns = [:person_id, :person_source_value])))
    end, person_id == person_map.person_id)
    define(pat_id => person_map.person_source_value)
end

end
