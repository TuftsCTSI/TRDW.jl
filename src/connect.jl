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

struct CatalogMetadata
    default_catalog::String
    concept_cache::Union{Tuple{ODBC.Connection, String}, Nothing}
    src_time::Int

    CatalogMetadata(default_catalog, concept_cache) =
        new(default_catalog, concept_cache, trunc(Int, datetime2unix(Dates.now())))
end

function get_metadata(cat::FunSQL.SQLCatalog)
    m = get(cat.metadata, :trdw, nothing)
    m isa CatalogMetadata || return
    m
end

struct TableMetadata
    ctime::Int
    is_view::Bool
    etl_hash::Union{String, Nothing}
    etl_time::Union{Int, Nothing}
end

function get_metadata(t::FunSQL.SQLTable)
    m = get(t.metadata, :trdw, nothing)
    m isa TableMetadata || return
    m
end

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
    concept_cache = create_concept_cache(conn, get(table_map, :concept, nothing))
    metadata = (; trdw = CatalogMetadata(catalog, concept_cache))
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
        FunSQL.JOIN(
            :etl_hash_t => info_schema |> FunSQL.ID(:table_tags),
            on = FunSQL.FUN(:and,
                FunSQL.FUN("=", (:t, :table_catalog), (:etl_hash_t, :catalog_name)),
                FunSQL.FUN("=", (:t, :table_schema), (:etl_hash_t, :schema_name)),
                FunSQL.FUN("=", (:t, :table_name), (:etl_hash_t, :table_name)),
                FunSQL.FUN("=", (:etl_hash_t, :tag_name), "etl_hash")),
            left = true) |>
        FunSQL.JOIN(
            :etl_time_t => info_schema |> FunSQL.ID(:table_tags),
            on = FunSQL.FUN(:and,
                FunSQL.FUN("=", (:t, :table_catalog), (:etl_time_t, :catalog_name)),
                FunSQL.FUN("=", (:t, :table_schema), (:etl_time_t, :schema_name)),
                FunSQL.FUN("=", (:t, :table_name), (:etl_time_t, :table_name)),
                FunSQL.FUN("=", (:etl_time_t, :tag_name), "etl_time")),
            left = true) |>
        FunSQL.WHERE(FunSQL.FUN("=", (:t, :table_schema), schemaname)) |>
        FunSQL.ORDER((:t, :table_catalog), (:t, :table_schema), (:t, :table_name), (:c, :ordinal_position)) |>
        FunSQL.SELECT(
            :created => (:t, :created),
            :is_view => FunSQL.FUN("=", (:t, :table_type), "VIEW"),
            :etl_hash => (:etl_hash_t, :tag_value),
            :etl_time => (:etl_time_t, :tag_value),
            :table_catalog => (:t, :table_catalog),
            :table_schema => (:t, :table_schema),
            :table_name => (:t, :table_name),
            :column_name => (:c, :column_name))
    sql = FunSQL.render(introspect_clause, dialect = FunSQL.SQLDialect(:spark))
    cr = DBInterface.execute(conn, sql)
    column_list = [(row.created,
                    row.is_view,
                    row.etl_hash,
                    row.etl_time,
                    lowercase(row.table_catalog),
                    lowercase(row.table_schema),
                    lowercase(row.table_name),
                    isessentiallyuppercase(row.column_name) ? lowercase(row.column_name) : row.column_name)
                  for row in Tables.rows(cr)]
    ODBC.clear!(conn)
    _tables_from_column_list(column_list)
end

function _tables_from_column_list(rows)
    tables = FunSQL.SQLTable[]
    qualifiers = Symbol[]
    ctime = is_view = etl_hash = etl_time = catalog = schema = name = nothing
    columns = Symbol[]
    for (ct, v, etl_h, etl_t, cat, s, n, c) in rows
        ct = trunc(Int, datetime2unix(ct))
        etl_h = etl_h !== missing ? etl_h : nothing
        etl_t = etl_t !== missing ? tryparse(Int, etl_t) : nothing
        cat = Symbol(cat)
        s = Symbol(s)
        n = Symbol(n)
        c = Symbol(c)
        if cat === catalog && s === schema && n === name
            push!(columns, c)
        else
            if !isempty(columns)
                metadata = (; trdw = TableMetadata(ctime, is_view, etl_hash, etl_time))
                t = FunSQL.SQLTable(; qualifiers, name, columns, metadata)
                push!(tables, t)
            end
            if cat !== catalog || s !== schema
                qualifiers = [cat, s]
            end
            ctime = ct
            is_view = v
            etl_hash = etl_h
            etl_time = etl_t
            catalog = cat
            schema = s
            name = n
            columns = [c]
        end
    end
    if !isempty(columns)
        metadata = (; trdw = TableMetadata(ctime, is_view, etl_hash, etl_time))
        t = FunSQL.SQLTable(; qualifiers, name, columns, metadata)
        push!(tables, t)
    end
    tables
end

function query_macro(__module__, __source__, db, q)
    db = esc(db)
    ex = FunSQL.transliterate(q, TRDW.FunSQL.TransliterateContext(__module__, __source__))
    if ex isa Expr && ex.head in (:(=), :macrocall) ||
        ex isa Expr && ex.head === :block && any(ex′ isa Expr && ex′.head in (:(=), :macrocall) for ex′ in ex.args)
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
