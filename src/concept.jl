@funsql concept() = begin
    from(concept)
    filter(is_null(invalid_reason))
    as(concept)
    define(
        concept.concept_id,
        concept.concept_code,
        concept.concept_name,
        concept.domain_id,
        concept.vocabulary_id,
        concept.concept_class_id)
end

@funsql concept_descendants() = begin
    as(base)
    join(
        concept_ancestor => from(concept_ancestor),
        base.concept_id == concept_ancestor.ancestor_concept_id)
    join(
        concept(),
        concept_ancestor.descendant_concept_id == concept_id)
end

@funsql concept_ancestors() = begin
    as(base)
    join(
        concept_ancestor => from(concept_ancestor),
        base.concept_id == concept_ancestor.descendant_concept_id)
    join(
        concept(),
        concept_ancestor.ancestor_concept_id == concept_id)
end

@funsql concept_relatives(relationship_id) = begin
    as(base)
    join(
        concept_relationship => 
            from(concept_relationship).filter(relationship_id == $relationship_id),
        base.concept_id == concept_relationship.concept_id_1)
    join(
        concept(),
        concept_relationship.concept_id_2 == concept_id)
end

@funsql concept_parents() = begin
    as(base)
    join(
        concept_parents => 
            from(concept_relationship).filter(relationship_id == "Subsumes"), 
        base.concept_id == concept_relationship.concept_id_2)
    join(
        concept(),
        concept_relationship.concept_id_1 == concept_id)
end

@funsql concept_children() = concept_relatives("Subsumes")

@funsql concept_siblings() = concept_parents().concept_children()

