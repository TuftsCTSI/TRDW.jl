@funsql begin

is_date_between(d, s, f) = `(? BETWEEN ? AND ?)`($d, $s, $f)

like_acronym(s, pat) =
    $(' ' in pat ? @funsql(` ILIKE `($s, $("%$(pat)%"))) :
         @funsql(` RLIKE `($s, $("(^|[^A-Za-z])$(pat)(\$|[^A-Za-z])"))))

like_acronym(s, pats...) =
    or($([@funsql(like_acronym($s, $pat)) for pat in pats]...))

deduplicate(keys...) = begin
    partition($(keys...), order_by = [$(keys...)], name = deduplicate)
    filter(deduplicate.row_number() <= 1)
end

in_category(concept_id, type, args) =
    in($concept_id, begin
        from(concept_ancestor)
        filter(in(ancestor_concept_id,
                  $([Integer(getfield(type, x)) for x in args])...))
        select(descendant_concept_id)
    end)

end
