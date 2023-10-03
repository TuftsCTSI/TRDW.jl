@funsql is_descendant_concept(concept_id, ids...) =
    exists(begin
        from(concept_ancestor)
        filter(descendant_concept_id == :concept_id &&
               in(ancestor_concept_id, $ids...))
        bind(:concept_id => $concept_id)
    end)
