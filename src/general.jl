const wide_notebook_style = html"""
<style>
/*    @media screen and (min-width: calc(700px + 25px + 283px + 34px + 25px)) */
        main {
            margin: 0 auto;
            max-width: 2000px;
            padding-right: 50px;
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

function connect_with_funsql(specs...; catalog = nothing, exclude = nothing)
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
    name = FunSQL.render(db, FunSQL.ID(t.qualifiers, t.name))
    ddl = run(db, "SHOW CREATE TABLE $name")[1,1]
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

clarity_dict(table_name) =
    HTML("""<a href="https://datahandbook.epic.com/ClarityDictionary/Details?tblName=$table_name"><code>$table_name</code></a>""")

fhir(table) =
    HTML("""<a href="https://www.hl7.org/fhir/$(lowercase(string(table))).html"><code>$(string(table))</code></a>""")

export clarity_dict, fhir
