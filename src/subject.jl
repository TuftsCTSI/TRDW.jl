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

function user_schema()
    case = get_case_code()
    return Symbol(temp_schema_prefix() * "_" * case)
end

function funsql_subject_table()
    case = get_case_code()
    FunSQL.SQLTable(qualifiers = [env_catalog(), :person_map], name = Symbol(case),
                    columns = [:person_id, :subject_id, :added, :removed])
end

funsql_subject_query() =
    @funsql begin
        from(subject_table())
        filter(isnull(removed))
        undefine(added, removed)
        left_join(person => person(),
                  person_id == person.person_id,
                  optional=true)
    end

function define_subject_id(; assert=true)
    name = :_subject_id
    query = funsql_subject_query()
    query = @funsql begin
        left_join($name => $query, $name.person_id == person_id)
    end
    query = @funsql($query.define_front($name.subject_id))
    if assert
        query = @funsql($query.filter(is_null(assert_true(is_not_null($name.subject_id)))))
    end
    return @funsql($query.define_front($name.subject_id).undefine($name))
end

funsql_define_subject_id(; assert=true) =
    funsql_if_not_defined(:subject_id, define_subject_id(; assert=assert))

function to_subject_id(; assert=true)
    query = define_subject_id(; assert=assert)
    return @funsql($query.undefine(person_id))
end

funsql_to_subject_id(; assert=true) =
   funsql_if_not_defined(:subject_id, to_subject_id(;  assert=assert))

funsql_fact_to_subject_id(domain_concept_id, fact_id) = begin
    name = :_fact_to_subject_id
    query = funsql_subject_query()
    @funsql begin
        left_join($name => $query, $name.person_id == $fact_id)
        filter($domain_concept_id != 1147314 || isnotnull($name.person_id))
        define($fact_id => ($domain_concept_id == 1147314) ? $name.subject_id : $fact_id)
        undefine($name)
    end
end

funsql_fact_to_subject_id() =
    @funsql begin
        fact_to_subject_id(domain_concept_id_1, fact_id_1)
        fact_to_subject_id(domain_concept_id_2, fact_id_2)
    end

function ensure_subject_table!(db; datatype=nothing)
    case = get_case_code()
    if isnothing(datatype)
        datatype = "bigint generated by default as identity(START WITH 1)"
    end
    create_table_if_not_exists(db, :person_map, Symbol(case),
        :subject_id => datatype, :person_id => :INTEGER,
        :added => :TIMESTAMP, :removed => :TIMESTAMP)
end

function merge_subject_table!(result::SQLResult; truncate=false, datatype=nothing)
    db = result.db
    sql = result.sql
    @assert occursin("subject_id", sql) "subject_id missing from query"
    case = get_case_code()
    subject_table = ensure_subject_table!(db; datatype=datatype)
    subject_sql = sqlname(db, subject_table)
    load_sql = """
        SELECT subject_id, person_id FROM (
            $sql
        )
        GROUP BY subject_id, person_id
        HAVING assert_true(count(subject_id) = 1)
    """
    if truncate
        DBInterface.execute(db, "TRUNCATE TABLE $subject_sql")
    end
    query = """
        MERGE INTO $subject_sql AS target
        USING ($load_sql) AS cohort
        ON target.subject_id = cohort.subject_id
        WHEN MATCHED THEN
            UPDATE SET removed = assert_true(person_id = cohort.person_id)
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (subject_id, person_id, added)
            VALUES (cohort.subject_id, cohort.person_id, current_timestamp())
    """
    print(query)
    DBInterface.execute(db, query)
    query = """
        UPDATE $subject_sql
        SET removed = current_timestamp()
        WHERE subject_id NOT IN (SELECT subject_id FROM ($load_sql))
    """
    DBInterface.execute(db, query)
    c = DBInterface.execute(db, "SELECT COUNT(*) FROM $subject_sql WHERE removed IS NULL")
    n = DataFrame(c)[1,1]
    message = "has $n persons; updated at $(now())"
    DBInterface.execute(db, "COMMENT ON TABLE $subject_sql IS '$message'")
    @info "table $subject_sql $message"
    nothing
end
