build_dsn(; kws...) =
    join(["$key=$val" for (key, val) in pairs(kws)], ';')

function connect_to_databricks(; catalog = nothing, schema = nothing)
    DATABRICKS_SERVER_HOSTNAME = ENV["DATABRICKS_SERVER_HOSTNAME"]
    DATABRICKS_HTTP_PATH = ENV["DATABRICKS_HTTP_PATH"]
    DATABRICKS_ACCESS_TOKEN = ENV["DATABRICKS_ACCESS_TOKEN"]
    DATABRICKS_CATALOG = get(ENV, "DATABRICKS_CATALOG", "ctsi")

    catalog = something(catalog, DATABRICKS_CATALOG)
    schema = get(ENV, "TRDW_SCHEMA", schema)

    DATABRICKS_DSN = build_dsn(
        Driver = "/opt/simba/spark/lib/64/libsparkodbc_sb64.so",
        Host = DATABRICKS_SERVER_HOSTNAME,
        Port = 443,
        SSL = 1,
        ThriftTransport = 2,
        HTTPPath = DATABRICKS_HTTP_PATH,
        UseNativeQuery = 1,
        AuthMech = 3,
        Catalog = catalog,
        Schema = schema,
        UID = "token",
        PWD = DATABRICKS_ACCESS_TOKEN)

    DBInterface.connect(ODBC.Connection, DATABRICKS_DSN)
end

isessentiallyuppercase(s) =
    all(ch -> isuppercase(ch) || isdigit(ch) || ch == '_', s)

function connect(specs...; catalog = nothing, exclude = nothing)
    DATABRICKS_CATALOG = get(ENV, "DATABRICKS_CATALOG", "ctsi")
    catalog = something(catalog, DATABRICKS_CATALOG)

    conn = connect_to_databricks(catalog = catalog)
    table_map = Dict{Symbol, FunSQL.SQLTable}()
    for spec in specs
        prefix, (catalogname, schemaname) = _unpack_spec(spec, catalog)
        raw_cols = ODBC.columns(conn,
                                catalogname = catalogname,
                                schemaname = schemaname)
        cols = [(lowercase(row.TABLE_CAT),
                 lowercase(row.TABLE_SCHEM),
                 lowercase(row.TABLE_NAME),
                 isessentiallyuppercase(row.COLUMN_NAME) ? lowercase(row.COLUMN_NAME) : row.COLUMN_NAME)
                for row in Tables.rows(raw_cols)]
        tables = _tables_from_column_list(cols)
        for table in tables
            exclude === nothing || !exclude(table) || continue
            name = Symbol("$prefix$(table.name)")
            table_map[name] = table
        end
    end
    cat = FunSQL.SQLCatalog(tables = table_map, dialect = FunSQL.SQLDialect(:spark))

    FunSQL.SQLConnection(conn, catalog = cat)
end

const connect_with_funsql = connect # backward compatibility

_unpack_spec(schema::Union{Symbol, AbstractString, NTuple{2, Union{Symbol, AbstractString}}}, default_catalog) =
    "", _split_schema(schema, default_catalog)

_unpack_spec(pair::Pair, default_catalog) =
    string(first(pair)), _split_schema(last(pair), default_catalog)

function _split_schema(schema::AbstractString, default_catalog)
    parts = split(schema, '.', limit = 2)
    length(parts) == 1 ?
      (default_catalog, string(parts[1])) : (string(parts[1]), string(parts[2]))
end

_split_schema(schema::NTuple{2, Union{Symbol, AbstractString}}, default_catalog) =
    string(schema[1]), string(schema[2])

function _tables_from_column_list(rows)
    tables = FunSQL.SQLTable[]
    qualifiers = Symbol[]
    catalog = schema = name = nothing
    columns = Symbol[]
    for (cat, s, n, c) in rows
        cat = Symbol(cat)
        s = Symbol(s)
        n = Symbol(n)
        c = Symbol(c)
        if cat === catalog && s === schema && n === name
            push!(columns, c)
        else
            if !isempty(columns)
                t = FunSQL.SQLTable(qualifiers = qualifiers, name = name, columns = columns)
                push!(tables, t)
            end
            if cat !== catalog || s !== schema
                qualifiers = [cat, s]
            end
            catalog = cat
            schema = s
            name = n
            columns = [c]
        end
    end
    if !isempty(columns)
        t = FunSQL.SQLTable(qualifiers = qualifiers, name = name, columns = columns)
        push!(tables, t)
    end
    tables
end

macro connect(args...)
    return quote
        const $(esc(:db)) = TRDW.connect($(Any[esc(arg) for arg in args]...))
        export $(esc(:db))

        macro $(esc(:query))(q)
            ex = TRDW.FunSQL.transliterate(q, TRDW.FunSQL.TransliterateContext($(esc(:__module__)), $(esc(:__source__))))
            if ex isa Expr && ex.head in (:block, :(=), :const, :global, :local)
                return ex
            end
            return quote
                TRDW.run($(esc(:db)), $ex)
            end
        end
        export $(esc(Symbol("@query")))

        if :concept in keys($(esc(:db)).catalog.tables)

            const $(esc(:funsql_ABMS)) = TRDW.Vocabulary("ABMS", TRDW.run($(esc(:db)), @funsql ABMS_concept()))
            const $(esc(:funsql_ATC)) = TRDW.Vocabulary("ATC", TRDW.run($(esc(:db)), @funsql ATC_concept()))
            const $(esc(:funsql_CMS_Place_of_Service)) = TRDW.Vocabulary("CMS Place of Service", TRDW.run($(esc(:db)), @funsql CMS_Place_of_Service_concept()))
            const $(esc(:funsql_CPT4)) = TRDW.Vocabulary("CPT4", TRDW.run($(esc(:db)), @funsql CPT4_concept()))
            const $(esc(:funsql_Condition_Status)) = TRDW.Vocabulary("Condition Status", TRDW.run($(esc(:db)), @funsql Condition_Status_concept()))
            const $(esc(:funsql_HES_Specialty)) = TRDW.Vocabulary("HES Specialty", TRDW.run($(esc(:db)), @funsql HES_Specialty_concept()))
            const $(esc(:funsql_HemOnc)) = TRDW.Vocabulary("HemOnc", TRDW.run($(esc(:db)), @funsql HemOnc_concept()))
            const $(esc(:funsql_ICDO3)) = TRDW.Vocabulary("ICDO3", TRDW.run($(esc(:db)), @funsql ICDO3_concept()))
            const $(esc(:funsql_ICD10CM)) = TRDW.Vocabulary("ICD10CM", TRDW.run($(esc(:db)), @funsql ICD10CM_concept()))
            const $(esc(:funsql_ICD10PCS)) = TRDW.Vocabulary("ICD10PCS", TRDW.run($(esc(:db)), @funsql ICD10PCS_concept()))
            const $(esc(:funsql_ICD9CM)) = TRDW.Vocabulary("ICD9CM", TRDW.run($(esc(:db)), @funsql ICD9CM_concept()))
            const $(esc(:funsql_ICD9Proc)) = TRDW.Vocabulary("ICD9Proc", TRDW.run($(esc(:db)), @funsql ICD9Proc_concept()))
            const $(esc(:funsql_LOINC)) = TRDW.Vocabulary("LOINC", TRDW.run($(esc(:db)), @funsql LOINC_concept()))
            const $(esc(:funsql_Medicare_Specialty)) = TRDW.Vocabulary("Medicare Specialty", TRDW.run($(esc(:db)), @funsql Medicare_Specialty_concept()))
            const $(esc(:funsql_NDFRT)) = TRDW.Vocabulary("NDFRT", TRDW.run($(esc(:db)), @funsql NDFRT_concept()))
            const $(esc(:funsql_NUCC)) = TRDW.Vocabulary("NUCC", TRDW.run($(esc(:db)), @funsql NUCC_concept()))
            const $(esc(:funsql_None)) = TRDW.Vocabulary("None", TRDW.run($(esc(:db)), @funsql None_concept()))
            const $(esc(:funsql_OMOP_Extension)) = TRDW.Vocabulary("OMOP Extension", TRDW.run($(esc(:db)), @funsql OMOP_Extension_concept()))
            const $(esc(:funsql_Procedure_Type)) = TRDW.Vocabulary("Procedure Type", TRDW.run($(esc(:db)), @funsql Procedure_Type_concept()))
            const $(esc(:funsql_Provider)) = TRDW.Vocabulary("Provider", TRDW.run($(esc(:db)), @funsql Provider_concept()))
            const $(esc(:funsql_Race)) = TRDW.Vocabulary("Race", TRDW.run($(esc(:db)), @funsql Race_concept()))
            const $(esc(:funsql_RxNorm_Extension)) = TRDW.Vocabulary("RxNorm Extension", TRDW.run($(esc(:db)), @funsql RxNorm_Extension_concept()))
            const $(esc(:funsql_RxNorm)) = TRDW.Vocabulary("RxNorm", TRDW.run($(esc(:db)), @funsql RxNorm_concept()))
            const $(esc(:funsql_SNOMED)) = TRDW.Vocabulary("SNOMED", TRDW.run($(esc(:db)), @funsql SNOMED_concept()))
            const $(esc(:funsql_Type_Concept)) = TRDW.Vocabulary("Type Concept", TRDW.run($(esc(:db)), @funsql Type_Concept_concept()))
            const $(esc(:funsql_UCUM)) = TRDW.Vocabulary("UCUM", TRDW.run($(esc(:db)), @funsql UCUM_concept()))
            const $(esc(:funsql_Visit)) = TRDW.Vocabulary("Visit", TRDW.run($(esc(:db)), @funsql Visit_concept()))

            const $(esc(:funsql_Dose_Form_Group)) = TRDW.Vocabulary("Dose Form Group", TRDW.run($(esc(:db)), @funsql Dose_Form_Group_concept()))
            const $(esc(:funsql_Component_Class)) = TRDW.Vocabulary("Component Class", TRDW.run($(esc(:db)), @funsql Component_Class_concept()))
            const $(esc(:funsql_Ingredient)) = TRDW.Vocabulary("Ingredient", TRDW.run($(esc(:db)), @funsql Ingredient_concept()))
            const $(esc(:funsql_Route)) = TRDW.Vocabulary("Route", TRDW.run($(esc(:db)), @funsql Route_concept()))
            const $(esc(:funsql_Specialty)) = TRDW.Vocabulary("Specialty", TRDW.run($(esc(:db)), @funsql Specialty_concept()))

            export $(esc(:funsql_ABMS))
            export $(esc(:funsql_ATC))
            export $(esc(:funsql_CMS_Place_of_Service))
            export $(esc(:funsql_CPT4))
            export $(esc(:funsql_Condition_Status))
            export $(esc(:funsql_HES_Specialty))
            export $(esc(:funsql_HemOnc))
            export $(esc(:funsql_ICDO3))
            export $(esc(:funsql_ICD10CM))
            export $(esc(:funsql_ICD10PCS))
            export $(esc(:funsql_ICD9CM))
            export $(esc(:funsql_ICD9Proc))
            export $(esc(:funsql_LOINC))
            export $(esc(:funsql_Medicare_Specialty))
            export $(esc(:funsql_NDFRT))
            export $(esc(:funsql_NUCC))
            export $(esc(:funsql_None))
            export $(esc(:funsql_OMOP_Extension))
            export $(esc(:funsql_Procedure_Type))
            export $(esc(:funsql_Provider))
            export $(esc(:funsql_Race))
            export $(esc(:funsql_RxNorm_Extension))
            export $(esc(:funsql_RxNorm))
            export $(esc(:funsql_SNOMED))
            export $(esc(:funsql_Type_Concept))
            export $(esc(:funsql_UCUM))
            export $(esc(:funsql_Visit))

            export $(esc(:funsql_Dose_Form_Group))
            export $(esc(:funsql_Component_Class))
            export $(esc(:funsql_Ingredient))
            export $(esc(:funsql_Route))
            export $(esc(:funsql_Specialty))

        end

        nothing
    end
end
