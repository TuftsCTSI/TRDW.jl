flatten_named_concept_sets(s::T) where T<:NamedTuple = [s]
flatten_named_concept_sets(v::T) where T<:Vector{<:NamedTuple} = v
flatten_named_concept_sets(t::T) where T<:Tuple = [(x for x in n) for n in t]
flatten_named_concept_sets(t::T) where T<:Vector = [(x for x in n) for n in t]

function concept_set_columns(match; match_on=nothing)
    retval = Pair[]
    for cpairs in flatten_named_concept_sets(match)
        for (handle, concept_set) in pairs(cpairs)
            push!(retval, handle =>
                  concept_matches(concept_set; match_on=match_on))
        end
    end
    return retval
end
const var"funsql#concept_set_columns" = concept_set_columns

function concept_set_fun(match, fun_name::Symbol, args...)
    retval = Pair[]
    for cpairs in flatten_named_concept_sets(match)
        for (handle, _) in pairs(cpairs)
            push!(retval, handle => FunSQL.Fun(fun_name, handle, args...))
        end
    end
    return retval
end
const var"funsql#concept_set_fun" = concept_set_fun

function concept_set_agg(match, agg_name::Symbol, args...)
    retval = Pair[]
    for cpairs in flatten_named_concept_sets(match)
        for (handle, _) in pairs(cpairs)
            push!(retval, handle => FunSQL.Agg(agg_name, handle, args...))
        end
    end
    return retval
end
const var"funsql#concept_set_agg" = concept_set_agg
const var"funsql#concept_set_count"(match) = concept_set_agg(match, :count_if)
const var"funsql#concept_set_any"(match) = concept_set_agg(match, :any)

function concept_set_roundups(match)
    retval = Pair[]
    for cpairs in flatten_named_concept_sets(match)
        for (handle, _) in pairs(cpairs)
            push!(retval, handle => @funsql roundups($handle))
        end
    end
    return retval
end
const var"funsql#concept_set_roundups" = concept_set_roundups

function group_by_concept(name=nothing; roundup=true,
                          person_threshold=0, event_threshold=0)
    roundup = castbool(roundup)
    concept_id = (name == nothing) ? :concept_id :
                 contains(string(name), "concept_id") ? name :
                 Symbol("$(name)_concept_id")
    base = @funsql(begin
        group(concept_id => $concept_id)
        define(n_event => count_distinct(occurrence_id),
               n_person => count_distinct(person_id))
    end)
    if person_threshold > 0
        base = base |> @funsql(filter(n_person>=$person_threshold))
    end
    if event_threshold > 0
        base = base |> @funsql(filter(n_event>=$event_threshold))
    end
    base = base |> @funsql(begin
        join(c => from(concept), c.concept_id == concept_id)
        order(n_person.desc(), c.concept_name)
    end)
    if roundup
        base = base |> @funsql(define(n_person => concat("≤", roundup(n_person)),
                                      n_event => concat("≤", roundup(n_event))))
    end
    return base |> @funsql(begin
        select(n_person, n_event, c.concept_id, c.vocabulary_id,
               c.concept_code, c.concept_name)
    end)
end
const var"funsql#group_by_concept" = group_by_concept

@funsql concept_set_person_total(match; roundup = true, group = []) = begin
    group(person_id, $group...)
    define(n_event=>count(),
        concept_set_agg($match, count_if)...)
    order(n_event.desc())
    $(castbool(roundup) ? @funsql(define(
        n_event => roundups(n_event),
        concept_set_roundups($match)...)) :
      @funsql(define()))
end

@funsql concept_set_total(match; roundup = true, group = []) = begin
    group(person_id, $group...)
    select(
        dummy => "in case of no match or groups",
        $group...,
        concept_set_agg($match, any)...)
    group($group...)
    select(
       $group...,
       n_people => count(),
       concept_set_agg($match, count_if)...)
    order(n_people.desc())
    $(castbool(roundup) ? @funsql(define(
        n_people => roundups(n_people),
        concept_set_roundups($match)...)) :
      @funsql(define()))
end

@funsql concept_set_pivot(match, match_on=nothing; roundup = true,
                          group = [], by_person = false) = begin
    select(person_id, occurrence_id, $group...,
           concept_set_columns($match; match_on=$match_on)...)
    $(by_person ? @funsql(concept_set_person_total($match; roundup=$roundup, group=$group)) :
      @funsql(concept_set_total($match; roundup=$roundup, group=$group)))
end
