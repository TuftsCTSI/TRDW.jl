@funsql begin

unit_matches(match...) =
    concept_matches($match...; match_on=unit_concept_id)
value_matches(match...) =
    concept_matches($match...; match_on=value_as_concept_id)
operator_matches(match...) =
    concept_matches($match...; match_on=operator_concept_id)

truncate_to_loinc_class() =
    truncate_to_concept_class("LOINC Class")
truncate_to_loinc_group() =
    truncate_to_concept_class("LOINC Group")
truncate_to_loinc_hierarchy() =
    truncate_to_concept_class("LOINC Hierarchy")

end
