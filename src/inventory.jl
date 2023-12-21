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

validate_primary_key(table, column) = begin
    from($table)
    group()
    define(
        column_name => $("$table.$column"),
        n_rows => count(),
        n_keys => count_distinct($column))
    define(is_valid_pk => n_rows == n_keys)
end

validate_foreign_key(
        source_table, source_column, target_table, target_column) = begin
    from($source_table)
    filter(is_not_null($source_column))
    left_join(target => from($target_table), $source_column == target.$target_column)
    filter(is_null(target.$target_column))
    group()
    define(
        column_name => $("$source_table.$source_column"),
        n_bad_rows => count(),
        n_bad_keys => count_distinct($source_column))
    define(is_valid_fk => n_bad_keys == 0)
    cross_join(
        begin
            from($source_table)
            group()
            define(
                n_all_rows => count($source_column),
                n_all_keys => count_distinct($source_column))
            define(n_null => count() - n_all_rows)
            define(pct_null => 100 * n_null / count())
        end)
end

end
