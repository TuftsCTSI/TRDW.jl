module TRDW

export @run_funsql

using DataFrames
using ODBC
using FunSQL

const wide_notebook_style = html"""
<style>
    @media screen and (min-width: calc(700px + 25px + 283px + 34px + 25px)) {
        main {
            margin: 0 auto;
            max-width: 2000px;
            padding-right: calc(283px + 34px + 25px);
        }
    }
</style>
"""

WideStyle() =
    wide_notebook_style

build_dsn(; kws...) =
    join(["$key=$val" for (key, val) in pairs(kws)], ';')

function connect_to_databricks(; catalog = nothing, schema = nothing)
    DATABRICKS_SERVER_HOSTNAME = ENV["DATABRICKS_SERVER_HOSTNAME"]
    DATABRICKS_HTTP_PATH = ENV["DATABRICKS_HTTP_PATH"]
    DATABRICKS_ACCESS_TOKEN = ENV["DATABRICKS_ACCESS_TOKEN"]
    DATABRICKS_CATALOG = ENV["DATABRICKS_CATALOG"]

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

function connect_with_funsql(conn::ODBC.Connection, cols)
    cat = FunSQL.SQLCatalog(
        tables = FunSQL.tables_from_column_list(cols),
        dialect = FunSQL.SQLDialect(:spark))

    FunSQL.SQLConnection(conn, catalog = cat)
end

function connect_with_funsql(specs...; catalog = nothing, exclude = nothing)
    DATABRICKS_CATALOG = ENV["DATABRICKS_CATALOG"]
    catalog = something(catalog, DATABRICKS_CATALOG)

    conn = connect_to_databricks(catalog = catalog)
    table_map = Dict{Symbol, FunSQL.SQLTable}()
    for spec in specs
        prefix, schemaname = _unpack_spec(spec)
        raw_cols = ODBC.columns(conn,
                                catalogname = catalog,
                                schemaname = schemaname)
        cols = [(lowercase(row.TABLE_SCHEM),
                 lowercase(row.TABLE_NAME),
                 lowercase(row.COLUMN_NAME))
                for row in Tables.rows(raw_cols)]
        tables = FunSQL.tables_from_column_list(cols)
        for table in tables
            exclude === nothing || !exclude(table) || continue
            name = Symbol("$prefix$(table.name)")
            table_map[name] = table
        end
    end
    cat = FunSQL.SQLCatalog(tables = table_map, dialect = FunSQL.SQLDialect(:spark))

    FunSQL.SQLConnection(conn, catalog = cat)
end

_unpack_spec(schema::Union{Symbol, AbstractString}) =
    "", string(schema)

_unpack_spec(pair::Pair) =
    string(first(pair)), string(last(pair))

function cursor_to_dataframe(cr)
    df = DataFrame(cr)
    # Remove `Missing` from column types where possible.
    disallowmissing!(df, error = false)
    df
end

run(db, q) =
    DBInterface.execute(db, q) |>
    cursor_to_dataframe

macro run_funsql(db, q)
    :(run($db, @funsql($q)))
end

function describe_all(db)
    tables = Pair{Symbol, Any}[]
    for name in sort(collect(keys(db.catalog)))
        fields = describe_table(db, name)
        push!(tables, name => fields)
    end
    Dict(tables)
end

function describe_table(db, name)
    t = db.catalog[name]
    ddl = run(db, "SHOW CREATE TABLE `$(t.schema)`.`$(t.name)`")[1,1]
    cols = match(r"\(\s*([^)]+)\)", ddl)[1]
    toks = split(cols, r"\s+|(?=\W)|\b")
    i = 1
    fields = Pair{Symbol, Any}[]
    done = false
    while !done
        f, i = parse_name(toks, i)
        ft, i = parse_type(toks, i)
        if i <= length(toks)
            _, i = parse_punctuation([","], toks, i)
        else
            done = true
        end
        push!(fields, f => ft)
    end
    fields
end

function parse_name(toks, i)
    i <= length(toks) || error("unexpected end of type string")
    tok = toks[i]
    i += 1
    occursin(r"\A\w+\z", tok) || error("unexpected token $tok")
    return Symbol(tok), i
end

function parse_type(toks, i)
    i <= length(toks) || error("unexpected end of type string")
    tok = toks[i]
    i += 1
    if tok in ["BOOLEAN", "SMALLINT", "INT", "FLOAT", "STRING", "DATE", "TIMESTAMP", "BINARY"]
        not_null, i = parse_not_null(toks, i)
        return (type = Symbol(lowercase(tok)), not_null = not_null), i
    elseif tok == "ARRAY"
        _, i = parse_punctuation(["<"], toks, i)
        elt, i = parse_type(toks, i)
        _, i = parse_punctuation([">"], toks, i)
        not_null, i = parse_not_null(toks, i)
        return (type = :array, eltype = elt, not_null = not_null), i
    elseif tok == "STRUCT"
        _, i = parse_punctuation(["<"], toks, i)
        fields = Pair{Symbol, Any}[]
        done = false
        while !done
            f, i = parse_name(toks, i)
            _, i = parse_punctuation([":"], toks, i)
            ft, i = parse_type(toks, i)
            push!(fields, f => ft)
            tok, i = parse_punctuation([",", ">"], toks, i)
            done = tok == ">"
        end
        not_null, i = parse_not_null(toks, i)
        return (type = :struct, fields = fields, not_null = not_null), i
    else
        error("unexpected token $tok")
    end
end

function parse_not_null(toks, i)
    if i + 1 <= length(toks) && toks[i] == "NOT" && toks[i + 1] == "NULL"
        return true, i + 2
    else
        return false, i
    end
end

function parse_punctuation(vals, toks, i)
    i <= length(toks) || error("unexpected end of type string")
    tok = toks[i]
    i += 1
    tok in vals || error("unexpected token $tok")
    return tok, i
end

end
