@funsql begin

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

collect_list_ordered(value; by = line) =
    agg(`(array_sort(collect_list(named_struct("by", ?, "value", ?))).value)`, $by, $value)

collect_set_ordered(value; by = line) =
    agg(`(array_distinct(array_sort(collect_list(named_struct("by", ?, "value", ?))).value))`, $by, $value)

deduplicate(keys...; order_by=[]) = begin
    partition($(keys...), order_by = [$([keys..., order_by...]...)], name = deduplicate)
    filter(deduplicate.row_number() <= 1)
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

end
