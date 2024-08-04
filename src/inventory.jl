@funsql begin

frequency(args...) = begin
    group($(args...))
    define(n => count())
    partition(name = all)
    define(`%` => floor(100 * n / all.sum(n), 1))
    order(n.desc())
end

array_frequency(expr; name = $(FunSQL.label(convert(FunSQL.SQLNode, expr)))) = begin
    partition(name = all)
    cross_join(from(explode_outer($expr), columns = [$name]))
    group($name)
    define(n => count())
    define(`%` => floor(100 * n / any_value(all.count()), 1))
    order(n.desc())
end

histogram(expr, bins = 20; name = $(FunSQL.label(convert(FunSQL.SQLNode, expr)))) = begin
    define(value => $expr)
    group()
    as(source)
    over(
        append(
            begin
                from(source)
                cross_join(histogram => from(explode(:HISTOGRAM), columns = [row]).bind(:HISTOGRAM => histogram_numeric(value, $bins)))
                define($name => histogram.row >> x, n => bigint(histogram.row >> y))
            end,
            from(source).define($name => missing, n => count(filter = is_null(value))))).filter(n > 0)
    partition(name = all)
    define(`%` => floor(100 * n / all.sum(n), 1))
    order($name)
end

percentiles(expr, qs = [0, 0.025, 0.25, 2.5, 25, 50, 75, 97.5, 99.75, 99.975, 100]; name = $(FunSQL.label(convert(FunSQL.SQLNode, expr)))) = begin
    group()
    cross_join(pcts => from($(i = eachindex(qs), q = qs)))
    define(
        pcts.q,
        $name => `[]`(percentile($expr, array(args = $(qs ./ 100))), pcts.i - 1))
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
