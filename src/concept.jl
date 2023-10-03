@funsql begin

concept() = begin
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

with_concept_id(ids...) =
	filter(in(concept_id, $(ids...)))
	
with_snomed(codes...) = begin
	filter(vocabulary_id == "SNOMED") 
	filter(in(concept_code, $(codes...)))
end
	
with_icd10(codes...) = begin
	filter(` ILIKE `(vocabulary_id, "ICD10%"))
	filter(in(concept_code, $(codes...)))
end

concept_descendants() = begin
    as(base)
    join(
        concept_ancestor => from(concept_ancestor),
        base.concept_id == concept_ancestor.ancestor_concept_id)
    join(
        concept(),
        concept_ancestor.descendant_concept_id == concept_id)
end

concept_ancestors() = begin
    as(base)
    join(
        concept_ancestor => from(concept_ancestor),
        base.concept_id == concept_ancestor.descendant_concept_id)
    join(
        concept(),
        concept_ancestor.ancestor_concept_id == concept_id)
end

concept_relatives(relationship_id) = begin
    as(base)
    join(
        concept_relationship => 
            from(concept_relationship).filter(relationship_id == $relationship_id),
        base.concept_id == concept_relationship.concept_id_1)
    join(
        concept(),
        concept_relationship.concept_id_2 == concept_id)
end

concept_parents() = begin
    as(base)
    join(
        concept_parents => 
            from(concept_relationship).filter(relationship_id == "Subsumes"), 
        base.concept_id == concept_relationship.concept_id_2)
    join(
        concept(),
        concept_relationship.concept_id_1 == concept_id)
end

concept_children() = concept_relatives("Subsumes")

concept_siblings() = concept_parents().concept_children()

end
