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
    db = FunSQL.SQLConnection(conn, catalog = cat)
    concept_cache = create_concept_cache(db)
    if concept_cache !== nothing
        metadata = Dict(:concept_cache => concept_cache)
        cat = FunSQL.SQLCatalog(tables = cat.tables, dialect = cat.dialect, metadata = metadata)
        db = FunSQL.SQLConnection(conn, catalog = cat)
    end
    db
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

function query_macro(__module__, __source__, db, q)
    db = esc(db)
    ex = FunSQL.transliterate(q, TRDW.FunSQL.TransliterateContext(__module__, __source__))
    if ex isa Expr && ex.head in (:(=), :const, :global, :local) ||
        ex isa Expr && ex.head === :block && any(ex′ isa Expr && ex′.head in (:(=), :const, :global, :local) for ex′ in ex.args)
        return ex
    end
    return quote
        TRDW.run($db, $ex)
    end
end

macro query(db, q)
    return query_macro(__module__, __source__, db, q)
end

macro connect(args...)
    ex = quote
        const db = TRDW.connect($(args...))
        export db

        import TRDW: @query
        macro query(q)
            return TRDW.query_macro(__module__, __source__, Expr(:($), :db), q)
        end
        export @query

        nothing
    end
    return esc(ex)
end
