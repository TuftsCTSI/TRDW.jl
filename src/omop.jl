@funsql restrict_by(q) =
    restrict_by(person_id, $q)

@funsql restrict_by(column_name, q) = begin
    left_join(
        subset => $q.filter(is_not_null($column_name)).group($column_name),
        $column_name == subset.$column_name)
    filter(is_null($column_name) || is_not_null(subset.$column_name))
end

# there are some lookups that are independent of table
value_isa(ids...) = is_descendant_concept(value_as_concept_id, $ids...)
qualifier_isa(ids...) = is_descendant_concept(qualifier_concept_id, $ids...)
unit_isa(ids...) = is_descendant_concept(unit_concept_id, $ids...)
