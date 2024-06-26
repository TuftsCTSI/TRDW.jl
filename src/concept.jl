@funsql begin

concept() = begin
    from(concept)
    as(omop)
    define(
        omop.concept_id,
        omop.concept_name,
        omop.domain_id,
        omop.vocabulary_id,
        omop.concept_class_id,
        omop.standard_concept,
        omop.concept_code,
        omop.invalid_reason)
end

concept(concept_id::Integer...) =
    concept().filter(in(concept_id, $concept_id...))

concept(p) = concept().filter($p)

concept_like(args...) = concept().filter(icontains(concept_name, $args...))

select_concept(name, include...; order=[]) = begin
    $(let frame = :_select_concept,
          columns = [:concept_id, :domain_id, :concept_class_id, :vocabulary_id,
                     :concept_code, :concept_name],
          concept_id = contains(string(name), "concept_id") ? name :
                         Symbol("$(name)_concept_id"),
          # let `order` be any expression
          order = [Symbol("_$(x[1])") => x[2] for x in enumerate(order)],
          define = [[p for p in order if p isa Pair]..., [p for p in include if p isa Pair]...],
          include = [x isa Pair ? x[1] : x for x in include],
          order = [x isa Pair ? x[1] : x for x in order],
          include = [[@funsql($frame.$n) for n in include]..., columns...],
          order = [[@funsql($frame.$n) for n in order]..., :vocabulary_id, :concept_code];
        @funsql(begin
            define($define...)
            as($frame)
            join(concept(), $frame.$concept_id == concept_id)
            order($order...)
            select($include...)
        end)
    end)
end

select_concept() = select_concept(concept_id)

count_concept(name, names...; roundup=true) = begin
    define(concept_id => $(contains(string(name), "concept_id") ? name :
                           Symbol("$(name)_concept_id")))
    define(concept_id => coalesce(concept_id, 0))
    group(concept_id, $names...)
    define(n_event => count(), n_person => count_distinct(person_id))
    select_concept(concept_id, $names...,
                   n_person => roundups(n_person; round=$roundup),
                   n_event => roundups(n_event; round=$roundup);
                   order = [n_person.desc(nulls=last), n_event.desc(nulls=last)])
end

count_concept(;roundup=true) = count_concept(concept_id; roundup=$roundup)

concept_descendants() = begin
    as(base)
    join(
        concept_ancestor => from(concept_ancestor),
        base.concept_id == concept_ancestor.ancestor_concept_id)
    join(
        concept(),
        concept_ancestor.descendant_concept_id == concept_id,
        optional = true)
    define(concept_ancestor.min_levels_of_separation, concept_ancestor.max_levels_of_separation)
    define(concept_id => concept_ancestor.descendant_concept_id)
end

concept_icd_descendants() = begin
    as(base)
    join(concept().filter(in(vocabulary_id, "ICD9CM", "ICD10CM")),
         base.vocabulary_id == vocabulary_id &&
         startswith(concept_code, base.concept_code))
end

concept_ancestors() = begin
    as(base)
    join(
        concept_ancestor => from(concept_ancestor),
        base.concept_id == concept_ancestor.descendant_concept_id)
    join(
        concept(),
        concept_ancestor.ancestor_concept_id == concept_id,
        optional = true)
    define(concept_ancestor.min_levels_of_separation, concept_ancestor.max_levels_of_separation)
    define(concept_id => concept_ancestor.ancestor_concept_id)
end

concept_relatives(relationship_id) = begin
    as(base)
    join(
        concept_relationship =>
            from(concept_relationship).filter(relationship_id == $relationship_id),
        base.concept_id == concept_relationship.concept_id_1)
    join(
        concept(),
        concept_relationship.concept_id_2 == concept_id,
        optional = true)
    define(concept_id => concept_relationship.concept_id_2)
end

define_concept_relatives(relationship_id) = begin
    left_join(concept_relatives => begin
        from(concept_relationship)
        filter(relationship_id == $relationship_id)
        group(concept_id => concept_id_1)
        define(collect => collect_to_string(concept_id_2))
    end, concept_id == concept_relatives.concept_id)
    define($relationship_id => concept_relatives.collect)
end

define_concept_relatives(relationship_id, another_id, more...) = begin
    define_concept_relatives($relationship_id)
    $(length(more) > 0 ?
      @funsql(define_concept_relatives($another_id, $(more[1]), $(more[2:end])...)) :
      @funsql(define_concept_relatives($another_id)))
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
        concept_relationship.concept_id_1 == concept_id,
        optional = true)
    define(concept_id => concept_relationship.concept_id_1)
end

concept_siblings() = concept_parents().concept_children()

filter_out_ancestors() = begin
    $(let name = :_filter_out_ancestors;
        @funsql(begin
            deduplicate(concept_id)
            left_join(
                $name => from(concept_ancestor),
                concept_id == $name.descendant_concept_id)
            partition($name.ancestor_concept_id)
            filter(count() <= 1)
            filter($name.min_levels_of_separation == 0)
            undefine($name)
        end)
    end)
end

filter_out_descendants() = begin
    $(let name = :_filter_out_descendants;
        @funsql(begin
            deduplicate(concept_id)
            left_join(
                $name => from(concept_ancestor),
                concept_id == $name.ancestor_concept_id)
            partition($name.descendant_concept_id)
            filter(count() <= 1)
            filter($name.min_levels_of_separation == 0)
            undefine($name)
        end)
    end)
    deduplicate(concept_id)
end

truncate_to_concept_class(concept_class_id, relationship_id="Is a") =
    $(let frame = :_truncate_to_concept_class;
        @funsql(begin
            left_join($frame => begin
                from(concept_relationship)
                filter(relationship_id==$relationship_id)
                join(kind => begin
                    concept()
                    filter(concept_class_id==$concept_class_id)
                end, concept_id_2 == kind.concept_id)
            end, concept_id == $frame.concept_id_1)
            define(concept_id => coalesce($frame.concept_id_2, concept_id))
            undefine($frame)
        end)
    end)

concept_cover(category::FunSQL.SQLNode; exclude=[]) = begin
    as(base)
    left_join(
        begin
            from(concept_ancestor)
            join(category => $category,
                 ancestor_concept_id == category.concept_id)
        end, base.concept_id == descendant_concept_id)
    partition(descendant_concept_id)
    filter(isnull(ancestor_concept_id) ||
           min_levels_of_separation == min(min_levels_of_separation))
    define(concept_id => coalesce(
        $(length(exclude) == 0 ? @funsql(ancestor_concept_id) : @funsql begin
           in(ancestor_concept_id, begin
              concept($exclude...)
              concept_ancestors()
              select(concept_id)
           end) ? base.concept_id : ancestor_concept_id
       end), base.concept_id))
    filter_out_descendants()
end

snomed_cover_via_cpt4(; exclude=[]) =
    concept_cover(begin
        concept()
        filter(concept_class_id == "CPT4 Hierarchy")
        left_join(nest_link =>
            from(concept_relationship).filter(relationship_id == "Subsumes"),
            nest_link.concept_id_1 == concept_id)
        left_join(nest =>
            from(concept).filter(concept_class_id == "CPT4 Hierarchy"),
            nest.concept_id == nest_link.concept_id_2)
        filter(isnull(nest.concept_id))
        concept_relatives("Subsumes")
        filter(concept_class_id == "CPT4")
        concept_relatives("CPT4 - SNOMED cat")
    end; exclude=$exclude)

end
