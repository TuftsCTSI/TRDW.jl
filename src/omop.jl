@funsql is_descendant_concept(concept_id, ids...) =
    exists(begin
        from(concept_ancestor)
        filter(descendant_concept_id == :concept_id &&
               in(ancestor_concept_id, $ids...))
        bind(:concept_id => $concept_id)
    end)

@funsql restrict_by(q) =
    restrict_by(person_id, $q)

@funsql restrict_by(column_name, q) = begin
    left_join(
        subset => $q.filter(is_not_null($column_name)).group($column_name),
        $column_name == subset.$column_name)
    filter(is_null($column_name) || is_not_null(subset.$column_name))
end
