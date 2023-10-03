@funsql begin
datediff_day(a, b) = `datediff(DAY, ?, ?)`($a, $b)
datediff_year(a, b) = `datediff(YEAR, ?, ?)`($a, $b)
is_date_between(d, s, f) = `(? BETWEEN ? AND ?)`($d, $s, $f)

matches_acronym(s, pat) =
	$(' ' in pat ? @funsql(` ILIKE `($s, $("%$(pat)%"))) :
         @funsql(` RLIKE `($s, $("(^|[^A-Za-z])$(pat)(\$|[^A-Za-z])"))))

deduplicate(keys...) = begin
	partition($(keys...), order_by = [$(keys...)], name = deduplicate)
	filter(deduplicate.row_number[] <= 1)
end

end
