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
        prefix, (catalogname, schemaname) = _unpack_spec(spec)
        tables = _introspect_schema(conn, catalogname, schemaname)
        for table in tables
            exclude === nothing || !exclude(table) || continue
            name = Symbol("$prefix$(table.name)")
            table_map[name] = table
        end
    end
    metadata = Dict{Symbol, Any}()
    metadata[:default_catalog] = catalog
    metadata[:concept_cache] = create_concept_cache(conn, get(table_map, :concept, nothing))
    metadata[:ctime] = trunc(Int, datetime2unix(Dates.now()))
    cat = FunSQL.SQLCatalog(tables = table_map, dialect = FunSQL.SQLDialect(:spark), metadata = metadata)
    db = FunSQL.SQLConnection(conn, catalog = cat)
    db
end

const connect_with_funsql = connect # backward compatibility

_unpack_spec(schema::Union{Symbol, AbstractString, NTuple{2, Union{Symbol, AbstractString}}}) =
    "", _split_schema(schema)

_unpack_spec(pair::Pair) =
    string(first(pair)), _split_schema(last(pair))

function _split_schema(schema::AbstractString)
    parts = split(schema, '.', limit = 2)
    length(parts) == 1 ?
      (nothing, string(parts[1])) : (string(parts[1]), string(parts[2]))
end

_split_schema(schema::NTuple{2, Union{Symbol, AbstractString}}) =
    string(schema[1]), string(schema[2])

function _introspect_schema(conn, catalogname, schemaname)
    info_schema = FunSQL.ID(:information_schema)
    if catalogname !== nothing
        info_schema = FunSQL.ID(catalogname) |> info_schema
    end
    introspect_clause =
        FunSQL.FROM(:t => info_schema |> FunSQL.ID(:tables)) |>
        FunSQL.JOIN(
            :c => info_schema |> FunSQL.ID(:columns),
            on = FunSQL.FUN(:and,
                FunSQL.FUN("=", (:t, :table_catalog), (:c, :table_catalog)),
                FunSQL.FUN("=", (:t, :table_schema), (:c, :table_schema)),
                FunSQL.FUN("=", (:t, :table_name), (:c, :table_name)))) |>
        FunSQL.WHERE(FunSQL.FUN("=", (:t, :table_schema), schemaname)) |>
        FunSQL.ORDER((:t, :table_catalog), (:t, :table_schema), (:t, :table_name), (:c, :ordinal_position)) |>
        FunSQL.SELECT(
            :created => (:t, :created),
            :table_catalog => (:t, :table_catalog),
            :table_schema => (:t, :table_schema),
            :table_name => (:t, :table_name),
            :column_name => (:c, :column_name))
    sql = FunSQL.render(introspect_clause, dialect = FunSQL.SQLDialect(:spark))
    cr = DBInterface.execute(conn, sql)
    column_list = [(row.created,
                    lowercase(row.table_catalog),
                    lowercase(row.table_schema),
                    lowercase(row.table_name),
                    isessentiallyuppercase(row.column_name) ? lowercase(row.column_name) : row.column_name)
                  for row in Tables.rows(cr)]
    _tables_from_column_list(column_list)
end

function _tables_from_column_list(rows)
    tables = FunSQL.SQLTable[]
    qualifiers = Symbol[]
    ctime = catalog = schema = name = nothing
    columns = Symbol[]
    for (ct, cat, s, n, c) in rows
        ct = trunc(Int, datetime2unix(ct))
        cat = Symbol(cat)
        s = Symbol(s)
        n = Symbol(n)
        c = Symbol(c)
        if cat === catalog && s === schema && n === name
            push!(columns, c)
        else
            if !isempty(columns)
                metadata = Dict{Symbol, Any}(:ctime => ctime)
                t = FunSQL.SQLTable(; qualifiers, name, columns, metadata)
                push!(tables, t)
            end
            if cat !== catalog || s !== schema
                qualifiers = [cat, s]
            end
            ctime = ct
            catalog = cat
            schema = s
            name = n
            columns = [c]
        end
    end
    if !isempty(columns)
        metadata = Dict{Symbol, Any}(:ctime => ctime)
        t = FunSQL.SQLTable(; qualifiers, name, columns, metadata)
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
            return TRDW.query_macro(__module__, __source__, db, q)
        end
        export @query

        nothing
    end
    return esc(ex)
end
