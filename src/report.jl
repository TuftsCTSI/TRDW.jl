flatten_csets(s::T) where T<:NamedTuple = [s]
flatten_csets(v::T) where T<:Vector{<:NamedTuple} = v
flatten_csets(t::T) where T<:Tuple = [(x for x in n) for n in t]
flatten_csets(t::T) where T<:Vector = [(x for x in n) for n in t]

function define_csets_matches(csets; on=nothing)
    cols = Pair[]
    for cpairs in flatten_csets(csets)
        for (handle, concept_set) in pairs(cpairs)
            push!(cols, handle =>
                  concept_matches(concept_set; on=on))
        end
    end
    return @funsql(define($cols...))
end
const funsql_define_csets_matches = define_csets_matches

function define_csets_roundups(csets)
    cols = Pair[]
    for cpairs in flatten_csets(csets)
        for (handle, _) in pairs(cpairs)
            push!(cols, handle => @funsql roundups($handle))
        end
    end
    return @funsql(define($cols...))
end
const funsql_define_csets_roundups = define_csets_roundups

function set_aggregate_filter!(node, test, n_replacement = 0)
    @assert node isa FunSQL.SQLNode
    core = getfield(node, :core)
    if core isa FunSQL.AggregateNode
        core.filter = test
        return n_replacement + 1
    end
    if hasfield(typeof(core), :args)
        for arg in core.args
            n_replacement = set_aggregate_filter!(arg, test, n_replacement)
        end
    end
    return n_replacement
end

function define_csets_aggregates(labels::Vector, args::Pair...)
    aggregates = Pair[]
    for slot in labels
        test = @funsql($slot)
        for (handle, template) in args
            @assert template isa FunSQL.SQLNode
            node = deepcopy(template)
            n_replacement = set_aggregate_filter!(node, test)
            @assert n_replacement == 1
            if Symbol("") == handle || "" == handle || handle == nothing
                label = string(slot)
            else
                label = "$(slot)_$(handle)"
            end
            push!(aggregates, label => node)
        end
    end
    return @funsql(define($aggregates...))
end
function define_csets_aggregates(csets::T, args::Pair...) where T<:NamedTuple
    define_csets_aggregates(collect(keys(csets)), args...)
end
const funsql_define_csets_aggregates = define_csets_aggregates

function group_by_concept(name=nothing; roundup=true,
                          person_threshold=0, event_threshold=0,
                          include=[])
    roundup = castbool(roundup)
    concept_id = (name == nothing) ? :concept_id :
                 contains(string(name), "concept_id") ? name :
                 Symbol("$(name)_concept_id")
    base = @funsql(begin
        group(concept_id => $concept_id, $include...)
        define(n_event => count_distinct(occurrence_id),
               n_person => count_distinct(person_id))
    end)
    if person_threshold > 0
        base = base |> @funsql(filter(n_person>=$person_threshold))
    end
    if event_threshold > 0
        base = base |> @funsql(filter(n_event>=$event_threshold))
    end
    include_order = [@funsql($col.asc(nulls=last)) for col in include]
    base = base |> @funsql(begin
        join(c => from(concept), c.concept_id == concept_id)
        order($include_order..., n_person.desc(nulls=last), c.concept_code)
    end)
    if roundup
        base = base |> @funsql(define(n_person => roundups(n_person),
                                      n_event => roundups(n_event)))
    end
    return base |> @funsql(begin
        select($include..., n_person, n_event, c.concept_id, c.vocabulary_id,
               c.concept_code, c.concept_name)
    end)
end
const funsql_group_by_concept = group_by_concept

@funsql concept_set_pivot(match, on=nothing; roundup = true, group = []) = begin
    define($group...)
    define_csets_matches($match; on=$on)
    group(person_id, $group...)
    define_csets_aggregates($match, "" => any(true))
    group($group...)
    define(n_people => count())
    define_csets_aggregates($match, "" => count_if(true))
    order(n_people.desc(nulls=last))
    $(castbool(roundup) ? @funsql(begin
            define(n_people => roundups(n_people))
            define_csets_roundups($match)
        end) : @funsql(define()))
end

function funsql_unpivot(; args::Vector{FunSQL.SQLNode}, name::Symbol = :class, left::Bool = true)
    labels = FunSQL.label.(args)
    case_args = FunSQL.SQLNode[]
    for (i, label) in enumerate(labels)
        push!(case_args, @funsql(_unpivot.index == $i), @funsql($(args[i])))
    end
    @funsql begin
        join(
            _unpivot => from($(; index = 1:length(labels), value = string.(labels))),
            case(args = $case_args),
            left = $left)
        define_front($name => _unpivot.value)
        order(_unpivot.index.asc(nulls = last))
    end
end

funsql_unpivot(args...; name = :class, left = true) =
    funsql_unpivot(args = FunSQL.SQLNode[args...], name = name, left = left)

function funsql_unpivot(ncs::NamedConceptSets; name = :class, left = true)
    args = FunSQL.SQLNode[]
    for (k, r) in ncs.dict
        push!(args, k => funsql_isa(r))
    end
    funsql_unpivot(args = args, name = name, left = left)
end
