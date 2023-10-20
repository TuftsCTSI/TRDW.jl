@funsql begin

concept(ids...) = begin
    from(concept)
    $(length(ids) == 0 ? @funsql(define()) :
      @funsql(filter(in(concept_id, $ids...))))
end

is_descendant_concept(name, ids...) =
    exists(begin
        from(concept_ancestor)
        filter(descendant_concept_id == :concept_id &&
               in(ancestor_concept_id, $(collect(ids))...))
        bind(:concept_id => $(contains(string(name), "concept_id") ? name :
                              Symbol("$(name)_concept_id")))
    end)

isa(ids...; prefix=nothing) =
    is_descendant_concept(
        $(prefix == nothing ? @funsql(concept_id) :
                            @funsql($(Symbol("$(prefix)_concept_id")))),
        $ids...)

select_concept(name, include...) = begin
    define(concept_id => $(contains(string(name), "concept_id") ? name :
                           Symbol("$(name)_concept_id")))
    as(base)
    join(from(concept), base.concept_id == concept_id)
    select($([[@funsql(base.$n) for n in include]...,
              :concept_id, :vocabulary_id, :concept_code, :concept_name])...)
end

select_concept() = select_concept(concept_id)

show_concept() =
    select(concept_id, detail => concat(
           replace(vocabulary_id, " ",""), "(", concept_code, ", `", concept_name,"`)"))

is_icd10(pats...; over=nothing) =
    and(ilike($(FunSQL.Get(:vocabulary_id, over = over)), "ICD10%"),
        or($[@funsql(ilike($(FunSQL.Get(:concept_code, over = over)),
                           $("$(pat)%"))) for pat in pats]...))

is_snomed(codes...; over=nothing) =
    and($(FunSQL.Get(:vocabulary_id, over = over)) == "SNOMED",
        in($(FunSQL.Get(:concept_code, over = over)), pats...))

count_concept(name=nothing) = begin
    define(concept_id => $(name == nothing ? :concept_id :
                           contains(string(name), "concept_id") ? name :
                           Symbol("$(name)_concept_id")))
    group(concept_id)
    define(count => count())
    as(base)
    join(concept(), concept_id == base.concept_id)
    order(base.count.desc(), vocabulary_id, concept_code)
    select(base.count, concept_id, vocabulary_id, concept_code, concept_name)
end

with_concept(name, extension=nothing) =
    join($name => begin
        concept()
        $(extension == nothing ? @funsql(define()) : extension)
    end, $(contains(string(name), "concept_id") ? name :
           Symbol("$(name)_concept_id")) == $name.concept_id)

join_concept(name, ids...; carry=[]) = begin
    as(base)
    join(begin
        concept()
        $(length(ids) == 0 ? @funsql(define()) :
            @funsql filter(is_descendant_concept(concept_id, $ids...)))
    end, base.$(contains(string(name), "concept_id") ? name :
                Symbol("$(name)_concept_id")) == concept_id)
    define($([@funsql($n => base.$n) for n in carry]...))
end

join_concept(;carry=[]) = begin
    as(base)
    join(begin
        concept()
    end, base.concept_id == concept_id)
    define($([@funsql($n => base.$n) for n in carry]...))
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

concept_relatives(relationship_id, n_or_r) = begin
    bounded_iterate(concept_relatives($relationship_id), $n_or_r)
    deduplicate(concept_id)
end

concept_relationships() = begin
    as(base)
    join(concept_relationship => from(concept_relationship),
         base.concept_id == concept_relationship.concept_id_1)
    join(from(relationship), concept_relationship.relationship_id == relationship_id)
    deduplicate(relationship_id)
end

concept_children() = concept_relatives("Subsumes")

concept_children(n_or_r) = concept_relatives("Subsumes", $n_or_r)

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

concept_siblings() = concept_parents().concept_children()

filter_out_ancestors() = begin
    $(let name = gensym(); @funsql(begin
        deduplicate(concept_id)
        left_join(
            $name => from(concept_ancestor),
            concept_id == $name.descendant_concept_id)
        partition($name.ancestor_concept_id)
        filter(count() <= 1)
        filter($name.min_levels_of_separation == 0)
        end)
    end)
end


filter_out_descendants() = begin
    $(let name = gensym(); @funsql(begin
        deduplicate(concept_id)
        left_join(
            $name => from(concept_ancestor),
            concept_id == $name.ancestor_concept_id)
        partition($name.descendant_concept_id)
        filter(count() <= 1)
        filter($name.min_levels_of_separation == 0)
        end)
    end)
end

generalize_to_concept_ancestor(category) = begin
    concept_ancestors()
    deduplicate(concept_id)
    left_join(category => $category,
              concept_id == category.concept_id)
    filter(is_not_null(category.concept_id) ||
           is_null(category.concept_id) &&
           concept_ancestor.min_levels_of_separation == 0)
    filter_out_ancestors()
end

concept_cover(category) = begin
    deduplicate(concept_id)
    as(base)
    left_join(
        begin
            from(concept_ancestor)
            join(category => $category,
                 ancestor_concept_id == category.concept_id)
        end, base.concept_id == descendant_concept_id)
    partition(descendant_concept_id)
    filter(isnull(ancestor_concept_id) || min_levels_of_separation == 1)
    select(concept_id => coalesce(ancestor_concept_id, base.concept_id))
end

end
