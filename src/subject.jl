function temp_schema_prefix()
    prefix = get(ENV, "DATABRICKS_TEMP_SCHEMA_PREFIX", nothing)
    if isnothing(prefix)
        uname =  get(ENV, "USER", get(ENV, "USERNAME", nothing))
        @assert !isnothing(uname)
        prefix = "zz_" * uname
    end
    prefix
end

is_production_schema_prefix() =
    ("zz" == temp_schema_prefix())

function user_schema(case::String)
    @assert length(case) == 8
    return Symbol(temp_schema_prefix() * "_" * case)
end

function user_index(case::String)
    @assert length(case) == 8
    table = FunSQL.SQLTable(qualifiers = [:ctsi, user_schema(case)], name = :index,
                            columns = [:person_id, :datetime, :datetime_end])
    return FunSQL.From(table)
end

function root_subject(case::String)
    @assert length(case) == 8
    base = FunSQL.SQLTable(qualifiers = [:ctsi, :person_map], name = Symbol(case),
                           columns = [:person_id, :subject_id, :added])
    return FunSQL.From(base)
end

function user_subject(case::String)
    base = root_subject(case)
    if is_production_schema_prefix()
        return base
    end
    temp = FunSQL.SQLTable(qualifiers = [:ctsi, user_schema(case)], name = :subject,
                           columns = [:person_id, :subject_id, :added])
    return base |> FunSQL.Append(FunSQL.From(temp))
end

function funsql_to_subject_id(case; rename=true, assert=true)
    name = gensym()
    subject_query = user_subject(case)
    query = @funsql begin
        left_join($name => $subject_query, $name.person_id == person_id)
    end
    if assert
        query = @funsql($query.filter(is_null(assert_true(is_not_null($name.person_id)))))
    end
    if rename
        return @funsql($query.define_front($name.subject_id).undefine(person_id))
    end
    return @funsql($query.define(person_id => $name.subject_id))
end

funsql_fact_to_subject_id(case) =
    @funsql begin
        fact_to_subject_id($case, domain_concept_id_1, fact_id_1)
        fact_to_subject_id($case, domain_concept_id_2, fact_id_2)
    end

funsql_fact_to_subject_id(case, domain_concept_id, fact_id) = begin
     name = gensym()
     subject_query = user_subject(case)
     @funsql begin
         left_join($name => $subject_query, $name.person_id == $fact_id)
         filter($domain_concept_id != 1147314 || isnotnull($name.person_id))
         define($fact_id => ($domain_concept_id == 1147314) ? $name.subject_id : $fact_id)
     end
end


""" user_rebuild_subject_table(db, case)

This has two modes of operation. When `is_production_schema_prefix()` we append to the
`ctsi.person_map.\$CASE` base cohort identity table new entries from the index.

Otherwise, we recreate `ctsi.zz_\$(USER)\$CASE` temporary table and populate it with
cohort entries that are not in the base cohort table. The identity values here start with
a high number so that it's clear they are temporary entries.

That `user_subject(case)` includes both base values with the user's temporary values.
"""
function user_rebuild_subject(db, case)
    @assert length(case) == 8
    base_query = FunSQL.From(
        create_table_if_not_exists(db, :person_map, Symbol(case),
            :subject_id => "bigint generated by default as identity(START WITH 1)",
            :person_id => :INTEGER, :added => :TIMESTAMP))
    catalog = get(ENV, "DATABRICKS_CATALOG", "ctsi")
    base_map_sql = FunSQL.render(db,
        FunSQL.ID(catalog) |> FunSQL.ID(:person_map) |> FunSQL.ID(Symbol(case)))
    temp_map_sql = FunSQL.render(db,
        FunSQL.ID(catalog) |> FunSQL.ID(user_schema(case)) |> FunSQL.ID(:subject))
    if is_production_schema_prefix()
        target_sql = base_map_sql
    else
        n_start = run(db,
            "SELECT coalesce(max(subject_id),0) AS max FROM $base_map_sql")[1,1] + 100000001
        DBInterface.execute(db, "DROP TABLE IF EXISTS $temp_map_sql")
        create_table_if_not_exists(db, user_schema(case), :subject,
            :subject_id => "bigint generated by default as identity(START WITH $n_start)",
            :person_id => :INTEGER,
            :added => :TIMESTAMP)
        target_sql = temp_map_sql
    end
    index_query = user_index(case)
    additions = FunSQL.render(db, @funsql begin
        $index_query
        group(person_id)
        left_join(c => $base_query, person_id == c.person_id)
        filter(isnull(c.person_id))
        order(rand())
        select(person_id, now())
    end)
    DBInterface.execute(db, """INSERT INTO $target_sql (person_id, added) ($additions)""")
    @info "table $target_sql updated at $(now())"
    return user_subject(case)
end

function user_rebuild_index(db, case, query::FunSQL.SQLNode)
    sname = user_schema(case)
    create_table(db, sname, :index, @funsql begin
        $query.select(person_id,
                      datetime => to_timestamp(datetime),
                      datetime_end => to_timestamp(datetime_end))
    end)
    return user_index(case)
end

user_rebuild(db, case, query::FunSQL.SQLNode) =
    (user_rebuild_index(db, case, query), user_rebuild_subject(db, case))

user_queries(case) = (user_index(case), user_subject(case))

function merge_subject_table!(db, case, q::FunSQL.SQLNode)
    load_sql = FunSQL.render(db, @funsql begin
             $q
             group(subject_id)
             define(person_id => first(person_id))
             assert_one_row(;define=[person_id])
             assert(isnotnull(person_id) && isnotnull(subject_id))
        end)
    catalog = get(ENV, "DATABRICKS_CATALOG", "ctsi")
    subject_sql = FunSQL.render(db, FunSQL.ID(catalog) |> FunSQL.ID(:person_map) |> FunSQL.ID(Symbol(case)))
    #DBInterface.execute(db, "TRUNCATE TABLE $subject_sql")
    query = """
        MERGE INTO $subject_sql AS target
        USING ($load_sql) AS cohort
        ON target.subject_id == cohort.subject_id
        WHEN MATCHED THEN
            UPDATE SET person_id = cohort.person_id
        WHEN NOT MATCHED THEN
            INSERT (subject_id, person_id, added)
            VALUES (cohort.subject_id, cohort.person_id, now())
    """
    DBInterface.execute(db, query)
    @info "table $subject_sql updated at $(now())"
end
