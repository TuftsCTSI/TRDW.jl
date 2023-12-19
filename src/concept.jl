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

concept(ids...) = begin
    concept()
    filter(in(concept_id, $ids...))
end

concept_like(args...) = concept().filter(icontains(concept_name, $args...))

select_concept(name, include...; order=[]) = begin
    define(concept_id => $(contains(string(name), "concept_id") ? name :
                           Symbol("$(name)_concept_id")))
    as(base)
    join(from(concept), base.concept_id == concept_id)
    order($([[@funsql(base.$n) for n in order]..., :vocabulary_id, :concept_code])...)
    select($([[@funsql(base.$n) for n in include]...,
              :concept_id, :vocabulary_id, :concept_class_id, :concept_code, :concept_name])...)
end

select_concept() = select_concept(concept_id)

find_concept(table, concept_id, names...) = begin
	as(base)
	join(begin
		$table
		join(c => concept(), c.concept_id == $concept_id)
		filter(icontains(c.concept_name, $names...))
	end, person_id == base.person_id)
	group($concept_id)
	select_concept($concept_id, n_event => count(), n_person => count_distinct(person_id))
	order(n_person.desc())
end

repr_concept(name=nothing) = begin
    define(concept_id => $(name == nothing ? :concept_id :
                           contains(string(name), "concept_id") ? name :
                           Symbol("$(name)_concept_id")))
    as(base)
    join(concept(), concept_id == base.concept_id)
    select(concept_id, detail => concat(
           replace(vocabulary_id, " ","_"), "(", concept_code, ", \"", concept_name,"\")"))
end

count_concept(name, names...; roundup=true) = begin
    define(concept_id => $(name == nothing ? :concept_id :
                           contains(string(name), "concept_id") ? name :
                           Symbol("$(name)_concept_id")))
    group(concept_id, $names...)
    define(n_event => roundups(count(), $roundup))
    define(n_person => roundups(count_distinct(person_id), $roundup))
    join(c => concept(), concept_id == c.concept_id)
    order(n_person.desc(), c.vocabulary_id, c.concept_code)
    select_concept(concept_id, n_person, n_event, $names...)
end

count_concept(;roundup=true) = count_concept(nothing; roundup=$roundup)

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
    define($([@funsql(base.$n) for n in carry]...))
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

exists_concept_relatives(relationship_id) = exists(begin
        from(concept_relationship)
        filter(relationship_id == $relationship_id)
        filter(concept_id_1 == :concept_id)
        bind(concept_id => concept_id)
    end)

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
    deduplicate(concept_id)
end



truncate_to_concept_class(name, concept_class_id, relationship_id="Is a") =
    $(let frame = gensym(),
          concept_id = (name == nothing) ? :concept_id :
                         contains(string(name), "concept_id") ? name :
                           Symbol("$(name)_concept_id");
        @funsql(begin
            left_join($frame => begin
                from(concept_relationship)
                filter(relationship_id==$relationship_id)
                join(kind => begin
                    concept()
                    filter(concept_class_id==$concept_class_id)
                end, concept_id_2 == kind.concept_id)
            end, $concept_id == $frame.concept_id_1)
            define($concept_id => coalesce($frame.concept_id_2, $concept_id))
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

snomed_cover_via_icd10(;exclude=[]) =
    concept_cover(begin
        concept()
        filter(in(concept_class_id, "3-char billing code", "3-char nonbill code"))
        concept_relatives("Maps to")
    end; exclude=$exclude)

snomed_cover_via_cpt4(;exclude=[]) =
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
