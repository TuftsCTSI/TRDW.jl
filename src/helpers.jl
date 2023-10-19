@funsql begin

like_acronym(s, pat) =
    $(' ' in pat ? @funsql(ilike($s, $("%$(pat)%"))) :
        @funsql(rlike($s, $("(^|[^A-Za-z])$(pat)(\$|[^A-Za-z])"))))

like_acronym(s, pats...) =
    or($([@funsql(like_acronym($s, $pat)) for pat in pats]...))

is_integer(s) = rlike($s, "^[0-9]+\$")
round_up(n) =  ceiling($n/10)*10

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

end
