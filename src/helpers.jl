@funsql begin

like_acronym(s, pat) =
    $(' ' in pat ? @funsql(ilike($s, $("%$(pat)%"))) :
        @funsql(rlike($s, $("(^|[^A-Za-z])$(pat)(\$|[^A-Za-z])"))))

like_acronym(s, pats...) =
    or($([@funsql(like_acronym($s, $pat)) for pat in pats]...))

is_integer(s) = rlike($s, "^[0-9]+\$")

deduplicate(keys...) = begin
    partition($(keys...), order_by = [$(keys...)], name = deduplicate)
    filter(deduplicate.row_number() <= 1)
end

end
