flatten_csets(s::T) where T<:NamedTuple = [s]
flatten_csets(v::T) where T<:Vector{<:NamedTuple} = v
flatten_csets(t::T) where T<:Tuple = [(x for x in n) for n in t]
flatten_csets(t::T) where T<:Vector = [(x for x in n) for n in t]

function define_csets_matches(csets; match_on=nothing)
    cols = Pair[]
    for cpairs in flatten_csets(csets)
        for (handle, concept_set) in pairs(cpairs)
            push!(cols, handle =>
                  concept_matches(concept_set; match_on=match_on))
        end
    end
    return @funsql(define($cols...))
end
const var"funsql#define_csets_matches" = define_csets_matches

function define_csets_roundups(csets)
    cols = Pair[]
    for cpairs in flatten_csets(csets)
        for (handle, _) in pairs(cpairs)
            push!(cols, handle => @funsql roundups($handle))
        end
    end
    return @funsql(define($cols...))
end
const var"funsql#define_csets_roundups" = define_csets_roundups

function define_csets_aggregates(csets, args::Pair...)
	aggregates = Pair[]
    for cpairs in flatten_csets(csets)
        for (slot, _) in pairs(cpairs)
		    test = @funsql($slot)
			for (handle, template) in args
				@assert template isa FunSQL.SQLNode
				node = deepcopy(template)
				core = getfield(node, :core)
				@assert core isa FunSQL.AggregateNode
				core.filter = test
				if Symbol("") == handle || "" == handle || handle == nothing
                    label = string(slot)
				else
					label = "$(slot)_$(handle)"
				end
				push!(aggregates, label => node)
			end
        end
    end
    return @funsql(define($aggregates...))
end
const var"funsql#define_csets_aggregates" = define_csets_aggregates

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

@funsql concept_set_pivot(match, match_on=nothing; roundup = true, group = []) = begin
    define($group...)
    define_csets_matches($match; match_on=$match_on)
    group(person_id, $group...)
    define_csets_aggregates($match, "" => any(true))
    group($group...)
    define(n_people => count())
    define_csets_aggregates($match, "" => count_if(true))
    order(n_people.desc())
    $(castbool(roundup) ? @funsql(begin
            define(n_people => roundups(n_people))
            define_csets_roundups($match)
        end) : @funsql(define()))
end
