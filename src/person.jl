@funsql begin

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
