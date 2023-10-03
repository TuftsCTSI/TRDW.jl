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

count_concepts(name=nothing) = begin
    define(concept_id => $(name == nothing ? :concept_id : Symbol("$(name)_concept_id")))
    group(concept_id)
    define(count => count[])
    join(c => from(concept), c.concept_id == concept_id)
    order(count.desc(), c.vocabulary_id, c.concept_code)
    select(count, concept_id, c.vocabulary_id, c.concept_code, c.concept_name)
end

with_concept(name, extension=nothing) =
    join($name => begin
      from(concept)
      $(extension == nothing ? @funsql(define()) : extension)
    end, $(Symbol("$(name)_concept_id")) == $name.concept_id)

is_icd10(pats...; base=nothing) = begin
  and(ilike($(FunSQL.Get(:vocabulary_id, over = base)), "ICD10%"),
      or($[@funsql(ilike($(FunSQL.Get(:concept_code, over = base)),
                         $("$(pat)%"))) for pat in pats]...))
end

having_icd10(pats...) =
    filter(is_icd10($pats...))

having_concept_id(ids...) =
	filter(in(concept_id, $(ids...)))
	
having_snomed(codes...) = begin
	filter(vocabulary_id == "SNOMED")
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
        concept_relationship =>
            from(concept_relationship).filter(relationship_id == "Subsumes"),
        base.concept_id == concept_relationship.concept_id_2)
    join(
        concept(),
        concept_relationship.concept_id_1 == concept_id)
end

concept_children() = concept_relatives("Subsumes")

concept_siblings() = concept_parents().concept_children()

end
