@funsql begin

count_values(keys...) = begin
	group($(keys...))
	define(n => count())
	partition()
	define(`%` => round(100 * n / sum(n), 1))
	order(n.desc())
end

define_era(datetime, datetime_end) = begin
    partition(person_id, order_by = [$datetime, occurrence_id],
              frame = (mode = rows, start = -Inf, finish = -1),
              name = _preceding)
    define(_is_start_of_era => _preceding.max($datetime_end) >= datetime ? 0 : 1)
    partition(person_id, order_by = [$datetime, _is_start_of_era.desc(), occurrence_id],
              frame = (mode = rows, start = -Inf, finish = 0),
              name = _preceding_or_current)
    define_after(era => _preceding_or_current.sum(_is_start_of_era),
                 name = occurrence_id)
    undefine(_preceding, _is_start_of_era, _preceding_or_current)
end

group_by_era(datetime=datetime, datetime_end=datetime_end) = begin
    define_era($datetime, $datetime_end)
    group(person_id, era)
    define(datetime => min(datetime), datetime_end => max(datetime_end))
end

like_acronym(s, pat) =
    $(' ' in pat ? @funsql(ilike($s, $("%$(pat)%"))) :
        @funsql(rlike($s, $("(^|[^A-Za-z])$(pat)(\$|[^A-Za-z])"))))

like_acronym(s, pats...) =
    or($([@funsql(like_acronym($s, $pat)) for pat in pats]...))

icontains(s, pat::AbstractString) =
    icontains => ilike($s, $("%$(pat)%"))
icontains(s, pats::Vector) =
    icontains => and($([@funsql(icontains($s, $p)) for p in pats]...))
icontains(s, pats::NTuple) =
    icontains => and($([@funsql(icontains($s, $p)) for p in pats]...))
icontains(s, pat, pats...) =
    icontains => or($([@funsql(icontains($s, $p)) for p in [pat, pats...]]...))

is_integer(s) = rlike($s, "^[0-9]+\$")
roundup(n) =  $n < 10 ? "<10" : $n

collect_to_string(v) = array_join(array_sort(array_distinct(collect_list($v))), "; ")

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

antijoin(q, lhs, rhs=lhs) = begin
     left_join(_antijoin => $q, $lhs == _antijoin.$rhs)
     filter(isnull(_antijoin.$rhs))
     undefine(_antijoin)
end

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

""" filter_with(pair, filter)

This function correlates by `person_id` upon the joined table, optionally filters.
"""
function funsql_filter_with(pair::Pair{Symbol, FunSQL.SQLNode}, predicate=true)
    (name, base) = pair
    partname = :_filter_with
    return @funsql(begin
        partition(order_by = [person_id], name = $partname)
        join($name => $base, $name.person_id == person_id && $predicate)
        partition($partname.row_number(); order_by = [person_id], name = $partname)
        filter($partname.row_number() <= 1)
        undefine($partname)
        undefine($name)
    end)
end

funsql_filter_with(node::FunSQL.SQLNode) =
    funsql_filter_with(:_filter_with_node => node)

""" filter_without(pair, filter)

This function correlates by `person_id` upon the joined table, optionally filters.
"""
function funsql_filter_without(pair::Pair{Symbol, FunSQL.SQLNode}, predicate=true)
    (name, base) = pair
    return @funsql(begin
        left_join($name => $base, $name.person_id == person_id && $predicate)
        filter(isnull($name.person_id))
        undefine($name)
    end)
end

funsql_filter_without(node::FunSQL.SQLNode) =
    funsql_filter_without(:_filter_without_node => node)

""" group_with(pair, filter)

This function correlates by `person_id` upon the joined table, as a group.
"""
function funsql_group_with(pair::Pair{Symbol, FunSQL.SQLNode}, predicate=true;
        partname=nothing)
    (name, base) = pair
    return @funsql(begin
        partition(order_by = [person_id], name = $partname)
        left_join($name => $base, $name.person_id == person_id && $predicate)
        partition($partname.row_number(); order_by = [person_id], name = $partname)
        filter($partname.row_number() <= 1)
        undefine($name)
    end)
end

function funsql_attach_earliest(pair::Pair{Symbol, FunSQL.SQLNode}, predicate = true)
    (name, base) = pair
    return funsql_attach_first(pair, predicate, order_by = [@funsql $name.datetime.asc(nulls = last)])
end

function funsql_attach_latest(pair::Pair{Symbol, FunSQL.SQLNode}, predicate = true)
    (name, base) = pair
    return funsql_attach_first(pair, predicate, order_by = [@funsql $name.datetime.desc(nulls = last)])
end

function funsql_attach_first(pair::Pair{Symbol, FunSQL.SQLNode}, predicate = true; order_by)
    (name, base) = pair
    partname = :_attach_first
    hasname = Symbol("has_$name")
    return @funsql begin
        partition(order_by = [person_id], name = $partname)
        left_join($name => $base, $name.person_id == person_id && $predicate)
        partition($partname.row_number(), order_by = $order_by, name = $partname)
        filter($partname.row_number() <= 1)
        undefine($partname)
        define($hasname => isnotnull($name.person_id))
    end
end

function funsql_join_with(pair::Pair{Symbol, FunSQL.SQLNode}, predicate=true)
    (name, base) = pair
    return @funsql(join($name => $base, $name.person_id == person_id && $predicate))
end

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
    return round ? @funsql(roundup($n)) : n
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

function funsql_take_first(keys...; order_by=[], name=nothing)
     partname = something(name, :_take_first)
     udefine = isnothing(name) ? @funsql(undefine($partname)) : @funsql(define())
     @funsql begin
         partition($(keys...), order_by = [$([keys..., order_by...]...)], name = $partname)
         filter($partname.row_number() <= 1)
         $udefine
     end
end

funsql_take_earliest_occurrence(;name=nothing) =
    @funsql(take_first(person_id; order_by=[datetime.asc()], name=$name))
funsql_take_latest_occurrence(;name=nothing) =
    @funsql(take_first(person_id; order_by=[datetime.desc()], name=$name))
