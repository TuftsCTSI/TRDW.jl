flatten_named_concept_sets(s::T) where T<:NamedTuple = [s]
flatten_named_concept_sets(v::T) where T<:Vector{<:NamedTuple} = v
flatten_named_concept_sets(t::T) where T<:Tuple = [(x for x in n) for n in t]
flatten_named_concept_sets(t::T) where T<:Vector = [(x for x in n) for n in t]

function pairing_match(match; match_prefix=nothing, match_source=nothing)
    retval = Pair[]
    for cpairs in flatten_named_concept_sets(match)
        for (handle, cset) in pairs(cpairs)
            push!(retval, handle =>
                  concept_matches(cset; match_prefix=match_prefix, match_source=match_source))
        end
    end
    return retval
end
const var"funsql#pairing_match" = pairing_match

function pairing_fun(match, fun_name::Symbol, args...)
    retval = Pair[]
    for cpairs in flatten_named_concept_sets(match)
        for (handle, _) in pairs(cpairs)
            push!(retval, handle => FunSQL.Fun(fun_name, handle, args...))
        end
    end
    return retval
end
const var"funsql#pairing_fun" = pairing_fun

function pairing_agg(match, agg_name::Symbol, args...)
    retval = Pair[]
    for cpairs in flatten_named_concept_sets(match)
        for (handle, _) in pairs(cpairs)
            push!(retval, handle => FunSQL.Agg(agg_name, handle, args...))
        end
    end
    return retval
end
const var"funsql#pairing_agg" = pairing_agg
const var"funsql#pairing_count"(match) = pairing_agg(match, :count_if)
const var"funsql#pairing_any"(match) = pairing_agg(match, :any)

function pairing_roundups(match)
    retval = Pair[]
    for cpairs in flatten_named_concept_sets(match)
        for (handle, _) in pairs(cpairs)
            push!(retval, handle => @funsql roundups($handle))
        end
    end
    return retval
end
const var"funsql#pairing_roundups" = pairing_roundups

function group_by_concept(name=nothing; roundup=true,
                          person_threshold=0, event_threshold=0)
    concept_id = (name == nothing) ? :concept_id :
                 contains(string(name), "concept_id") ? name :
                 Symbol("$(name)_concept_id")
    base = @funsql(begin
        group(concept_id => $concept_id)
        define(n_event => count(),
               n_person => count_distinct(person_id))
    end)
    if person_threshold > 0
        base = base |> @funsql(filter(n_person>=$person_threshold))
    end
    if event_threshold > 0
        base = base |> @funsql(filter(n_event>=$event_threshold))
    end
    if roundup
        base = base |> @funsql(define(n_person => concat("≤", roundup(n_person)),
                                      n_event => concat("≤", roundup(n_event))))
    end
    return base |> @funsql(begin
        join(c => from(concept), c.concept_id == concept_id)
        order(n_person.desc(), c.concept_name)
        select(n_person, n_event, c.concept_id, c.vocabulary_id,
               c.concept_code, c.concept_name)
    end)
end
const var"funsql#group_by_concept" = group_by_concept

@funsql pairing_person_total(match; roundup::Bool = true) = begin
    group(person_id)
    order(count().desc())
    define(n_events=>count(),
        pairing_agg($match, count_if)...)
    $(roundup ? @funsql(define(
        n_events => roundups(n_events),
        pairing_roundups($match)...)) :
      @funsql(define()))
end

@funsql pairing_event_total(match; roundup::Bool = true) = begin
    group(person_id)
    select(
        n_events=>count(),
        pairing_agg($match, any)...)
    group()
    select(
       n_people => count(),
       n_events => sum(n_events),
       pairing_agg($match, count_if)...)
    $(roundup ? @funsql(define(
        n_people => roundups(n_people),
        n_events => roundups(n_events),
        pairing_roundups($match)...)) :
      @funsql(define()))
end

@funsql pairing_pivot(match, match_prefix::Symbol, pkcol::Symbol; event_total::Bool=true,
                      person_total::Bool=true, roundup::Bool = true) = begin
    filter(concept_matches($match; match_prefix = $match_prefix))
    select(person_id, $pkcol, pairing_match($match; match_prefix=$match_prefix)...)
    $(event_total ? @funsql(pairing_event_total($match; roundup=$roundup)) :
      person_total ? @funsql(pairing_person_total($match; roundup=$roundup)) :
      @funsql(define()))
end
