build_dsn(; kws...) =
    join(["$key=$val" for (key, val) in pairs(kws)], ';')

function connect_to_databricks(; catalog = nothing, schema = nothing, allowed_local_paths = ["."])
    DATABRICKS_SERVER_HOSTNAME = ENV["DATABRICKS_SERVER_HOSTNAME"]
    DATABRICKS_HTTP_PATH = ENV["DATABRICKS_HTTP_PATH"]
    DATABRICKS_ACCESS_TOKEN = ENV["DATABRICKS_ACCESS_TOKEN"]
    DATABRICKS_CATALOG = get(ENV, "DATABRICKS_CATALOG", "ctsi")

    DATABRICKS_DRIVERS = [
        "/opt/simba/spark/lib/64/libsparkodbc_sb64.so",
        "/Library/simba/spark/lib/libsparkodbc_sb64-universal.dylib"
    ]
    if !any(isfile, DATABRICKS_DRIVERS)
        error("Cannot find Databricks ODBC driver")
    end
    driver = DATABRICKS_DRIVERS[findfirst(isfile, DATABRICKS_DRIVERS)]

    catalog = something(catalog, DATABRICKS_CATALOG)

    DATABRICKS_DSN = build_dsn(
        Driver = driver,
        Host = DATABRICKS_SERVER_HOSTNAME,
        Port = 443,
        SSL = 1,
        ThriftTransport = 2,
        HTTPPath = DATABRICKS_HTTP_PATH,
        UseNativeQuery = 1,
        AuthMech = 3,
        Catalog = catalog,
        Schema = schema,
        StagingAllowedLocalPaths = join(abspath.(allowed_local_paths), ','),
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

function connect(specs...; catalog = nothing, exclude = nothing, allowed_local_paths = ["."])
    DATABRICKS_CATALOG = get(ENV, "DATABRICKS_CATALOG", "ctsi")
    catalog = something(catalog, DATABRICKS_CATALOG)
    conn = connect_to_databricks(catalog = catalog, allowed_local_paths = allowed_local_paths)
    table_map = Dict{Symbol, FunSQL.SQLTable}()
    for spec in specs
        prefix, (catalogname, schemaname) = _unpack_spec(spec)
        tables = _introspect_schema(something(catalogname, catalog), schemaname)
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

module DatabricksTablesAPI

using StructTypes

struct Column
    name::String
end

struct TableProperties
    etl_hash::Union{String, Nothing}
    etl_time::Union{String, Nothing}
end

StructTypes.names(::Type{TableProperties}) =
    ((:etl_hash, Symbol("trdw.etl_hash")), (:etl_time, Symbol("trdw.etl_time")))

struct Table
    table_type::Symbol
    name::String
    catalog_name::String
    schema_name::String
    columns::Vector{Column}
    created_at::Int
    properties::TableProperties
end

struct Result
    tables::Vector{Table}
end

Result(::Nothing) =
    Result(Table[])

end # module DatabricksTablesAPI

function _introspect_schema(catalog, schema)
    DATABRICKS_SERVER_HOSTNAME = ENV["DATABRICKS_SERVER_HOSTNAME"]
    DATABRICKS_ACCESS_TOKEN = ENV["DATABRICKS_ACCESS_TOKEN"]
    response =
        try
            HTTP.get(
                "https://$DATABRICKS_SERVER_HOSTNAME/api/2.1/unity-catalog/tables",
                query = ["catalog_name" => catalog, "schema_name" => schema],
                headers = ["Authorization" => "Bearer $DATABRICKS_ACCESS_TOKEN"])
        catch e
            if e isa HTTP.StatusError && e.status == 404
                return FunSQL.SQLTable[]
            end
            rethrow()
        end
    result = JSON3.read(response.body, DatabricksTablesAPI.Result)
    tables = FunSQL.SQLTable[]
    for t in result.tables
        qualifiers = [Symbol(lowercase(t.catalog_name)), Symbol(lowercase(t.schema_name))]
        name = Symbol(lowercase(t.name))
        columns = [Symbol(isessentiallyuppercase(c.name) ? lowercase(c.name) : c.name) for c in t.columns]
        ctime = t.created_at
        is_view = t.table_type === :VIEW
        etl_hash = t.properties.etl_hash
        etl_time = t.properties.etl_time isa String ? tryparse(Int, t.properties.etl_time) : nothing
        metadata = (; trdw = TableMetadata(ctime, is_view, etl_hash, etl_time))
        push!(tables, FunSQL.SQLTable(; qualifiers, name, columns, metadata))
    end
    tables
end

function query_macro(__module__, __source__, db, q)
    db = esc(db)
    ctx = FunSQL.TransliterateContext(__module__, __source__)
    if FunSQL.transliterate_is_definition(q)
        return FunSQL.transliterate_definition(q, ctx)
    end
    ex = FunSQL.transliterate_toplevel(q, ctx)
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
            esc(:(@query $db $q))
        end
        export @query

        nothing
    end
    return esc(ex)
end
