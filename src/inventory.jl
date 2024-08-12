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

validate_primary_key(source::FunSQL.SQLNode, columns::Vector{Symbol}) = begin
    $source
    group($(columns...))
    define(key_is_not_null => and(args = $[@funsql(is_not_null($column)) for column in columns]))
    group()
    define(
        primary_key => $(_primary_key_name(source, columns)),
        n => sum(count()),
        ndv => count(filter = key_is_not_null),
        n_null => nullif(sum(count(), filter = !key_is_not_null), 0),
        n_dup => nullif(sum(count(), filter = count() > 1 && key_is_not_null), 0),
        ndv_dup => nullif(count(filter = count() > 1 && key_is_not_null), 0))
    define(is_valid => is_null(n_null) && is_null(n_dup), after = primary_key)
    define(pct_null => floor(100 * n_null / n, 1), after = n_null)
    define(pct_dup => floor(100 * n_dup / n, 1), after = n_dup)
end

validate_primary_key(table::Symbol, columns::Vector{Symbol}) =
    validate_primary_key(from($table), $columns)

validate_primary_key(source::Union{FunSQL.SQLNode, Symbol}, column::Symbol) =
    validate_primary_key($source, [$column])

validate_primary_key(sources::Vector, columns) =
    append(args = $[@funsql(validate_primary_key($source, $columns)) for source in sources])

validate_foreign_key(source::FunSQL.SQLNode, source_columns::Vector{Symbol}, target::FunSQL.SQLNode, target_columns::Vector{Symbol}) = begin
    source => $source.group($(source_columns...)).define(is_present => true)
    join(
        target => $target.group($(target_columns...)).define(is_present => true),
        on = and(args = $[@funsql(source.$source_column == target.$target_column) for (source_column, target_column) in zip(source_columns, target_columns)]),
        left = true,
        right = true)
    define(
        source_is_present => coalesce(source.is_present, false),
        source_key_is_not_null => and(args = $[@funsql(is_not_null(source.$source_column)) for source_column in source_columns]),
        target_is_present => coalesce(target.is_present, false),
        target_key_is_not_null => and(args = $[@funsql(is_not_null(target.$target_column)) for target_column in target_columns]))
    group()
    define(
	foreign_key => $(_foreign_key_name(source, source_columns, target, target_columns)),
        is_valid => coalesce(!any(source_key_is_not_null && !target_is_present), true),
        n => sum(source.count()),
        n_linked => sum(source.count(), filter = target_is_present),
        n_bad => sum(source.count(), filter = source_key_is_not_null && !target_is_present),
        ndv_bad => nullif(count(filter = source_key_is_not_null && !target_is_present), 0),
        n_tgt => sum(target.count()),
        n_tgt_linked => sum(target.count(), filter = source_is_present),
        med_card => median(source.count(), filter = target_is_present),
        max_card => max(source.count(), filter = target_is_present))
    define(pct_linked => floor(100 * n_linked / n, 1), after = n_linked)
    define(pct_bad => floor(100 * n_bad / n, 1), after = n_bad)
    define(pct_tgt_linked => floor(100 * n_tgt_linked / n_tgt, 1), after = n_tgt_linked)
end

validate_foreign_key(source::Union{FunSQL.SQLNode, Symbol}, source_column::Symbol, target::Union{FunSQL.SQLNode, Symbol}, target_column::Symbol) =
    validate_foreign_key($source, [$source_column], $target, [$target_column])

validate_foreign_key(source::Union{FunSQL.SQLNode, Symbol}, source_columns::Vector{Symbol}, target_table::Symbol, target_columns::Vector{Symbol}) =
    validate_foreign_key($source, $source_columns, from($target_table), $target_columns)

validate_foreign_key(source_table::Symbol, source_columns::Vector{Symbol}, target::FunSQL.SQLNode, target_columns::Vector{Symbol}) =
    validate_foreign_key(from($source_table), $source_columns, $target, $target_columns)

validate_foreign_key(sources::Vector, source_columns, target, target_columns) =
    append(args = $[@funsql(validate_foreign_key($source, $source_columns, $target, $target_columns)) for source in sources])

validate_foreign_key(source, source_columns, target) =
    validate_foreign_key($source, $source_columns, $target, $source_columns)

end

function _primary_key_name(source, columns)
    table = FunSQL.label(source)
    if length(columns) == 1
        "$(table).$(columns[1])"
    else
        "$(table) ($(join(string.(columns), ", ")))"
    end
end

function _foreign_key_name(source, source_columns, target, target_columns)
    source_table = FunSQL.label(source)
    target_table = FunSQL.label(target)
    if length(source_columns) == length(target_columns) == 1
        "$(source_table).$(source_columns[1]) → $(target_table).$(target_columns[1])"
    else
        "$(source_table) ($(join(string.(source_columns), ", "))) → $(target_table) ($(join(string.(target_columns), ", ")))"
    end
end
