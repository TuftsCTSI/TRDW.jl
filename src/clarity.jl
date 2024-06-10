funsql_left_join_bypk(table_name, primary_key; define=[]) = begin
    define = [@funsql($column => $table_name.$column) for column in define]
    @funsql(begin
        left_join($table_name => from($table_name), $primary_key == $table_name.$primary_key)
        define($define...)
    end)
end

funsql_define_zc_lookup(name; zc_table=nothing) = begin
    if name isa Pair
        (alias, name) = name
    else
        alias = name
    end
    zc_table = something(zc_table, Symbol("zc_$name"))
    _c = Symbol("$(name)_c")
    @funsql(begin
        left_join($zc_table => from($zc_table), $_c == $zc_table.$_c)
        define($alias => $zc_table.name)
        undefine($zc_table)
    end)
end

function decode_nest!(name::Symbol, map::Vector, default)::FunSQL.SQLNode
    (key, val) = pop!(map)
    if length(map) > 0
        default = decode_nest!(name, map, default)
    end
    @funsql($name == $key ? $val : $default)
end

funsql_decode_c(name::Symbol, map::Pair...; default=nothing) =
    decode_nest!(name, collect(map), something(default, name))

# TODO: move to ETL
@funsql define_do_not_contact() = begin
    join(person=>person(), person.person_id == person_id)
    left_join(do_not_contact => from(`patient_4`).filter(RSH_PREF_C == 2),
        person.omop.person_source_value == do_not_contact.PAT_ID)
    define(do_not_contact => isnotnull(do_not_contact.PAT_ID))
    with(`patient_4` =>
        from($(FunSQL.SQLTable(qualifiers = [:main, :epicclarity], name = :patient_4,
                               columns = [:PAT_ID, :RSH_PREF_C]))))
end
