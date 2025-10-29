@funsql begin

frequency(args...) = begin
    group($(args...))
    define(n => count())
    partition(name = all)
    define(`%` => floor(100 * n / all.sum(n), 1))
    order(n.desc())
end

array_frequency(expr; name = $(FunSQL.label(expr))) = begin
    partition(name = all)
    cross_join(from(explode_outer($expr), columns = [$name]))
    group($name)
    define(n => count())
    define(`%` => floor(100 * n / any_value(all.count()), 1))
    order(n.desc())
end

histogram(expr, bins = 20; name = $(FunSQL.label(expr))) = begin
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

percentiles(expr, qs = [0, 0.025, 0.25, 2.5, 25, 50, 75, 97.5, 99.75, 99.975, 100]; name = $(FunSQL.label(expr))) = begin
    group()
    cross_join(pcts => from($(i = eachindex(qs), q = qs)))
    define(
        pcts.q,
        $name => `[]`(percentile($expr, array(args = $(qs ./ 100))), pcts.i - 1))
end

validate_primary_key(source::FunSQL.SQLQuery, columns::Vector{Symbol}) = begin
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

validate_primary_key(source::Union{FunSQL.SQLQuery, Symbol}, column::Symbol) =
    validate_primary_key($source, [$column])

validate_primary_key(sources::Vector, columns) =
    append(args = $[@funsql(validate_primary_key($source, $columns)) for source in sources])

validate_foreign_key(source::FunSQL.SQLQuery, source_columns::Vector{Symbol}, target::FunSQL.SQLQuery, target_columns::Vector{Symbol}) = begin
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

validate_foreign_key(source::Union{FunSQL.SQLQuery, Symbol}, source_column::Symbol, target::Union{FunSQL.SQLQuery, Symbol}, target_column::Symbol) =
    validate_foreign_key($source, [$source_column], $target, [$target_column])

validate_foreign_key(source::Union{FunSQL.SQLQuery, Symbol}, source_columns::Vector{Symbol}, target_table::Symbol, target_columns::Vector{Symbol}) =
    validate_foreign_key($source, $source_columns, from($target_table), $target_columns)

validate_foreign_key(source_table::Symbol, source_columns::Vector{Symbol}, target::FunSQL.SQLQuery, target_columns::Vector{Symbol}) =
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

@funsql row_delta(table, keys = [$(Symbol("$(table)_id"))], previous_table = $(Symbol("previous_$(table)"))) = begin
    current => from($table).define(is_present => true)
    join(
        previous => if_set($previous_table, from($previous_table), from($table).filter(false)).define(is_present => true),
        and(args = $[@funsql(current.$key == previous.$key) for key in keys]),
        left = true,
        right = true)
    frequency(state => is_null(previous.is_present) ? "added" : is_null(current.is_present) ? "dropped" : "retained")
end

function funsql_column_delta(table, keys = [Symbol("$(table)_id")], previous_table = Symbol("previous_$(table)"))
    function custom_resolve(n, ctx)
        tail′ = FunSQL.resolve(ctx)
        t = FunSQL.row_type(tail′)
        curr_t = t.fields[:current]
        prev_t = t.fields[:previous]
        fields = collect(Base.keys(curr_t.fields))
        qs = FunSQL.SQLQuery[]
        for field in fields
            if field in Base.keys(prev_t.fields)
                push!(qs, @funsql(count_if(previous.$field !== current.$field)))
            else
                push!(qs, @funsql(count(current.$field)))
            end
        end
        @funsql begin
            $tail′
            group()
            cross_join(summary_case => from(explode(sequence(1, $(length(fields)))), columns = [index]))
            define(column => $(_summary_switch(string.(fields))))
            define(n_changed => $(_summary_switch(qs)))
            define(pct_changed => floor(100 * n_changed / count(), 1))
        end
    end
    @funsql begin
        current => from($table)
        join(
            previous => if_set($previous_table, from($previous_table), from($table).filter(false)),
            and(args = $[@funsql(current.$key == previous.$key) for key in keys]))
        $(CustomResolve(custom_resolve))
    end
end
