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

function user_project_schema()
    project_code = funsql_get_project_code()
    return temp_schema_prefix() * "_" * project_code
end
funsql_user_project_schema = user_project_schema

function project_schema()
    project_code = funsql_get_project_code()
    return "zz_" * project_code
end
funsql_project_schema = project_schema

function funsql_subject_table()
    project_code = funsql_get_project_code()
    FunSQL.SQLTable(qualifiers = [env_catalog(), :person_map], name = Symbol(project_code),
                    columns = [:person_id, :subject_id, :added, :removed])
end

function ensure_subject_table(db; datatype = "bigint generated by default as identity(START WITH 1)")
    project_code = funsql_get_project_code()
    create_table_if_not_exists(db, :person_map, Symbol(project_code),
        :subject_id => datatype, :person_id => :INTEGER,
        :added => :TIMESTAMP, :removed => :TIMESTAMP)
end

funsql_subject_query(table=funsql_subject_table()) =
    @funsql begin
        from($table)
        filter(isnull(removed))
        undefine(added, removed)
        left_join(person => person(),
                  person_id == person.person_id,
                  optional=true)
    end

function funsql_define_subject_id(table=funsql_subject_table(); assert=true)
    name = :_subject_id
    query = funsql_subject_query(table)
    query = @funsql begin
        left_join($name => $query, $name.person_id == person_id)
    end
    query = @funsql($query.define_front($name.subject_id))
    if assert
        query = @funsql($query.filter(is_null(assert_true(is_not_null($name.subject_id)))))
    end
    return @funsql($query.define_front($name.subject_id).undefine($name))
end

function funsql_to_subject_id(table=funsql_subject_table(); assert=true)
    query = funsql_define_subject_id(table; assert=assert)
    return @funsql($query.undefine(person_id))
end

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

struct MergeSubjectSpecification
    node::FunSQL.SQLNode
    truncate::Bool
end

funsql_merge_subject(node; truncate=false) = MergeSubjectSpecification(node, truncate)

function run(db, spec::MergeSubjectSpecification)
    sql = FunSQL.render(db, spec.node)
    if false
        # TODO: requires FunSQL#metadata branch
        # check sql.columns for subject_id
    else
        @assert !occursin("subject_id", sql) "use merge_customer_subject() for customer-provided subject ids"
    end
    subject_table = ensure_subject_table(db)
    subject_sql = sqlname(db, subject_table)
    if spec.truncate
        DBInterface.execute(db, "TRUNCATE TABLE $subject_sql")
    end
    load_sql = """
        (SELECT DISTINCT person_id FROM ($sql))
    """
    query = """
        MERGE INTO $subject_sql AS target
        USING $load_sql AS cohort
        ON target.person_id = cohort.person_id
        WHEN MATCHED THEN
            UPDATE SET removed = NULL
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (person_id, added)
            VALUES (cohort.person_id, current_timestamp())
    """
    DBInterface.execute(db, query)
    query = """
        UPDATE $subject_sql
        SET removed = current_timestamp()
        WHERE person_id NOT IN $load_sql
    """
    DBInterface.execute(db, query)
    c = DBInterface.execute(db, "SELECT COUNT(*) FROM $subject_sql WHERE removed IS NULL")
    n = DataFrame(c)[1,1]
    message = "has $n persons; updated at $(now())"
    DBInterface.execute(db, "COMMENT ON TABLE $subject_sql IS '$message'")
    @info "table $subject_sql $message"
    nothing
end

struct MergeCustomerSubjectSpecification
    node::FunSQL.SQLNode
    datatype::String
    truncate::Bool
end

funsql_merge_customer_subject(node; truncate=false, datatype="BIGINT") =
    MergeCustomerSubjectSpecification(node, datatype, truncate)

function run(db, spec::MergeCustomerSubjectSpecification)
    sql = FunSQL.render(db, spec.node)
    if false
        # TODO: requires FunSQL#metadata branch
        # check sql.columns for subject_id
    else
        @assert occursin("subject_id", sql) "use merge_subject() to add/remove subjects"
    end
    subject_table = ensure_subject_table(db; spec.datatype)
    subject_sql = sqlname(db, subject_table)
    load_sql = """
        SELECT person_id, first_value(subject_id) AS subject_id FROM (
            $sql
        )
        GROUP BY person_id
        HAVING assert_true(count(subject_id) = 1) IS NULL
    """
    if spec.truncate
        DBInterface.execute(db, "TRUNCATE TABLE $subject_sql")
    end
    query = """
        MERGE INTO $subject_sql AS target
        USING ($load_sql) AS cohort
        ON target.person_id = cohort.person_id
        WHEN MATCHED THEN
            UPDATE SET removed = assert_true(target.subject_id = cohort.subject_id)
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (subject_id, person_id, added)
            VALUES (cohort.subject_id, cohort.person_id, current_timestamp())
    """
    DBInterface.execute(db, query)
    query = """
        UPDATE $subject_sql
        SET removed = current_timestamp()
        WHERE person_id NOT IN (SELECT person_id FROM ($load_sql))
    """
    # DBInterface.execute(db, query)
    c = DBInterface.execute(db, "SELECT COUNT(*) FROM $subject_sql WHERE removed IS NULL")
    n = DataFrame(c)[1,1]
    message = "has $n persons; updated at $(now())"
    DBInterface.execute(db, "COMMENT ON TABLE $subject_sql IS '$message'")
    @info "table $subject_sql $message"
    nothing
end
