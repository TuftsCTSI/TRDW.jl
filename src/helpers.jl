@funsql begin

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
roundups(n, round::Bool=true) = $(round ? @funsql(concat("≤", roundup($n))) : n)

deduplicate(keys...) = begin
    partition($(keys...), order_by = [$(keys...)], name = deduplicate)
    filter(deduplicate.row_number() <= 1)
end

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
