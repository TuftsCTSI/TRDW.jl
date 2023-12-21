@funsql begin

table_size(db::FunSQL.SQLConnection, table) = begin
	from($table)
	group()
	define(
		table_name => $("$table"),
		n_rows => count(),
		n_columns => $(length(db.catalog[table].columns)))
end

table_size(db::FunSQL.SQLConnection) = begin
	append(
		args =
			$[@funsql(table_size($db, $table))
			for table in keys(db.catalog)])
end

table_density(db::FunSQL.SQLConnection, table, q = nothing) = begin
	$(q !== nothing ? q : @funsql(from($table)))
	group()
	cross_join(
		index =>
			from(
				explode(sequence(1, $(length(db.catalog[table].columns)))),
				columns = [i]))
	define(
		column_name =>
			case($(Iterators.flatten([
				(@funsql(index.i == $i), "$table.$column")
				for (i, column) in enumerate(db.catalog[table].columns)])...)),
		n_not_null =>
			case($(Iterators.flatten([
				(@funsql(index.i == $i), @funsql(count($column)))
				for (i, column) in enumerate(db.catalog[table].columns)])...)),
		pct_not_null =>
			case($(Iterators.flatten([
				(@funsql(index.i == $i), @funsql(100 * count($column) / count()))
				for (i, column) in enumerate(db.catalog[table].columns)])...)),
		approx_n_distinct =>
			case($(Iterators.flatten([
				(@funsql(index.i == $i), @funsql(approx_count_distinct($column)))
				for (i, column) in enumerate(db.catalog[table].columns)])...)))
end

end
