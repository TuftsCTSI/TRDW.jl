flatten_named_concept_sets(s::T) where T<:NamedTuple = [s]
flatten_named_concept_sets(v::T) where T<:Vector{<:NamedTuple} = v
flatten_named_concept_sets(t::T) where T<:Tuple = [(x for x in n) for n in t]
flatten_named_concept_sets(t::T) where T<:Vector = [(x for x in n) for n in t]

function build_concept_pairing(clist, name=nothing, source=nothing)
    retval = Pair[]
    for cpairs in flatten_named_concept_sets(clist)
        for (handle, cset) in pairs(cpairs)
            push!(retval, handle => build_concept_matches(cset, name, source))
        end
    end
    return retval
end
const var"funsql#build_concept_pairing" = build_concept_pairing

function build_pairing_fun(clist, fun_name::Symbol, args...)
    retval = Pair[]
    for cpairs in flatten_named_concept_sets(clist)
        for (handle, _) in pairs(cpairs)
            push!(retval, handle => FunSQL.Fun(fun_name, handle, args...))
        end
    end
    return retval
end
const var"funsql#build_pairing_fun" = build_pairing_fun

function build_pairing_agg(clist, agg_name::Symbol, args...)
    retval = Pair[]
    for cpairs in flatten_named_concept_sets(clist)
        for (handle, _) in pairs(cpairs)
            push!(retval, handle => FunSQL.Agg(agg_name, handle, args...))
        end
    end
    return retval
end
const var"funsql#build_pairing_agg" = build_pairing_agg

const var"funsql#build_pairing_count"(clist) = build_pairing_agg(clist, :count_if)
const var"funsql#build_pairing_any"(clist) = build_pairing_agg(clist, :any)

function build_pairing_roundups(clist)
    retval = Pair[]
    for cpairs in flatten_named_concept_sets(clist)
        for (handle, _) in pairs(cpairs)
            push!(retval, handle => @funsql roundups($handle))
        end
    end
    return retval
end
const var"funsql#build_pairing_roundups" = build_pairing_roundups

@funsql build_person_total(clist, roundup::Bool) = begin
    group(person_id)
    order(count().desc())
    define(n_events=>count(),
        build_pairing_agg($clist, count_if)...)
    $(roundup ? @funsql(define(
        n_events => roundups(n_events),
        build_pairing_roundups($clist)...)) :
      @funsql(define()))
end

@funsql build_total(clist, roundup::Bool) = begin
    group(person_id)
    select(
        n_events=>count(),
        build_pairing_agg($clist, any)...)
    group()
    select(
       n_people => count(),
       n_events => sum(n_events),
       build_pairing_agg($clist, count_if)...)
    $(roundup ? @funsql(define(
        n_people => roundups(n_people),
        n_events => roundups(n_events),
        build_pairing_roundups($clist)...)) :
      @funsql(define()))
end

@funsql build_pivot(clist, name::Symbol, pkcol::Symbol, 
                    total::Bool, person_total::Bool, roundup::Bool) = begin
    filter(build_concept_matches($clist, $name))
    select(person_id, $pkcol, build_concept_pairing($clist, $name)...)
    $(total ? @funsql(build_total($clist, $roundup)) :
      person_total ? @funsql(build_person_total($clist, $roundup)) :
      @funsql(define()))
end
