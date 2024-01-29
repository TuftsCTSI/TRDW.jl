@funsql begin

collapse_intervals(start_date=:datetime, end_date=:datetime_end;
                   gap=0, group_by=[person_id]) = begin
    partition($group_by..., order_by = [$start_date],
        frame = (mode = rows, start = -Inf, finish = -1))
    define(new => datediff_day(max($end_date), $start_date) <= $gap ? 0 : 1 )
    partition($group_by..., order_by = [$start_date, -new], frame = (mode = rows))
    define(era => sum(new))
    group($group_by..., era)
    define(datetime => min($start_date),
           datetime_end => max($end_date))
end

like_acronym(s, pat) =
    $(' ' in pat ? @funsql(ilike($s, $("%$(pat)%"))) :
        @funsql(rlike($s, $("(^|[^A-Za-z])$(pat)(\$|[^A-Za-z])"))))

like_acronym(s, pats...) =
    or($([@funsql(like_acronym($s, $pat)) for pat in pats]...))

icontains(s, pat::String) = ilike($s, $("%$(pat)%"))
icontains(s, pats::Vector{String}) = and($([@funsql(icontains($s, $pat)) for pat in pats]...))
icontains(s, pats...) = or($([@funsql(icontains($s, $pat)) for pat in pats]...))

is_integer(s) = rlike($s, "^[0-9]+\$")
roundup(n) =  ceiling($n/10)*10

deduplicate(keys...; order_by=[]) = begin
    partition($(keys...), order_by = [$([keys..., order_by...]...)], name = deduplicate)
    filter(deduplicate.row_number() <= 1)
end

has(v) = $(let name = "has_$v"; @funsql($name => any(isnotnull($v.person_id))) end)

bounded_iterate(q, n::Integer) =
    $(n > 1 ? @funsql($q.bounded_iterate($q, $(n - 1))) : n > 0 ? q : @funsql(define()))

bounded_iterate(q, r::UnitRange{<:Integer}) = begin
    as(base)
    over(append(args = $[@funsql(from(base).bounded_iterate($q, $n)) for n in r]))
end

antijoin(q, lhs, rhs=nothing) =
    $(let name = gensym(), rhs = something(rhs, lhs);
          @funsql(left_join($name => $q, $lhs == $rhs).filter(isnull($name.$rhs)))
    end)

restrict_by(q) = restrict_by(person_id, $q)

restrict_by(column_name, q) = begin
    left_join(
        subset => $q.filter(is_not_null($column_name)).group($column_name),
        $column_name == subset.$column_name)
    filter(is_null($column_name) || is_not_null(subset.$column_name))
end

# there are some lookups that are independent of table
value_isa(ids...) = is_descendant_concept(value_as_concept_id, $ids...)
qualifier_isa(ids...) = is_descendant_concept(qualifier_concept_id, $ids...)
unit_isa(ids...) = is_descendant_concept(unit_concept_id, $ids...)

end_of_day(date; day_offset=0) =
    to_timestamp($date)+make_interval(0, 0, 0, $day_offset, 23, 59, 59.999999)

end

"""filter_with(pair, filter)

This function correlates by `person_id` upon the joined table, optionally filters,
and then returns the first entry by `occurrence_id` that matches.

TODO: update FunSQL to avoid leaking of `name`.
"""
function filter_with(pair::Pair{Symbol, FunSQL.SQLNode}, predicate=true)
    (name, base) = pair
    partname = gensym()
    return @funsql(begin
        join($name => $base, $name.person_id == person_id && $predicate)
        partition(occurrence_id; order_by = [occurrence_id], name = $partname)
        filter($partname.row_number() <= 1)
        undefine($name)
    end)
end
funsql_filter_with = filter_with

filter_with(node::FunSQL.SQLNode, predicate=true) =
    filter_with(gensym() => node, predicate)

"""filter_without(pair, filter)

This function correlates by `person_id` upon the joined table, optionally filters,
and then returns the first entry by `occurrence_id` that doesn't match.

TODO: update FunSQL to avoid leaking of `name`.
"""
function filter_without(pair::Pair{Symbol, FunSQL.SQLNode}, predicate=true)
    (name, base) = pair
    return @funsql(begin
        left_join($name => $base, $name.person_id == person_id)
        filter($(something(predicate, true)))
        filter(isnull($name.person_id))
        undefine($name)
    end)
end
funsql_filter_without = filter_without

filter_without(node::FunSQL.SQLNode, predicate=true) =
    filter_without(gensym() => node, predicate)

function group_with(pair::Pair{Symbol, FunSQL.SQLNode}, predicate=true; partname=nothing)
    (name, base) = pair
    return @funsql(begin
        left_join($name => $base, $name.person_id == person_id && $predicate)
        partition(occurrence_id; order_by = [occurrence_id], name = $partname)
        filter($partname.row_number() <= 1)
        undefine($name)
    end)
end
funsql_group_with = group_with

#group_with(node::FunSQL.SQLNode, predicate=true) =
#    group_with(gensym() => node, predicate)


""" castbool(v)

This function permits us to use `!\$v` expressions within a notebook.
"""
function castbool(v::FunSQL.SQLNode)::Bool
    v isa FunSQL.SQLNode ? v = getfield(v, :core) : nothing
    if v isa FunSQL.FunctionNode && v.name == :not
        if length(v.args) == 1
            v = v.args[1]
        end
    end
    v isa FunSQL.SQLNode ? v = getfield(v, :core) : nothing
    if v isa FunSQL.LiteralNode
        return !v.val
    end
    error("expecting !bool")
end

castbool(v::Bool) = v

funsql_castbool = castbool

function roundups(n; round=true)
    if !isa(round, Bool)
        round = castbool(round)
    end
    return round ? @funsql(concat("≤", roundup($n))) : n
end

funsql_roundups = roundups

funsql_assert(predicate) =
    @funsql(filter(coalesce(assert_true($predicate), true)))

funsql_assert_isnotnull(name::Symbol) =
   @funsql($name => coalesce(assert_true(isnotnull($name)), $name))

funsql_assert_isnotnull(name::AbstractString) =
    funsql_assert_isnotnull(Symbol(name))

function funsql_assert_one_row(; carry=[])
    q = funsql_assert(@funsql(count()==1))
    parts =  [@funsql($n => first($n)) for n in carry]
    return q |> @funsql(define($parts...))
end

function funsql_take_first(keys...; order_by=[])
    partname = gensym()
    @funsql begin
        partition($(keys...), order_by = [$([keys..., order_by...]...)], name = $partname)
        filter($partname.row_number() <= 1)
    end
end

funsql_take_first_occurrence() = @funsql(take_first(person_id; order_by=[datetime]))
funsql_take_latest_occurrence() = @funsql(take_first(person_id; order_by=[datetime.desc()]))

function funsql_rollup(items...; define=[])
    base = gensym()
    gset = []
    defn = []
    args = []
    tail = []
    sort = []
    for el in items
        if el isa Symbol
            push!(gset, el)
        elseif el isa Pair
            push!(defn, el)
            push!(gset, el[1])
        elseif el isa FunSQL.SQLNode && getfield(el, :core) isa FunSQL.AggregateNode
            name = getfield(getfield(el, :core), :name)
            push!(defn, @funsql($name => $el))
            push!(gset, name)
        else
            @error(something(dump(el), "unable to group by $el"))
        end
        push!(sort, @funsql($(gset[end]).asc(nulls=last)))
    end
    while length(gset) > 0
        node = @funsql(from($base).group($gset..., $tail...))
        push!(args, node)
        item = pop!(gset)
        push!(tail, @funsql $item => missing)
    end
    push!(args, @funsql(from($base).group($tail...)))
    return @funsql(define($defn...).as($base).over(append(args=$args)).define($define...).order($sort...))
end
