run(db, q) =
    run(db, convert(FunSQL.SQLNode, q))

function run(db, sql::AbstractString)
    SQLResult(db, sql)
end

function run(db, q::FunSQL.SQLNode)
    sql = FunSQL.render(db, q)
    SQLResult(db, sql)
end

struct SQLResult
    db::FunSQL.SQLConnection{ODBC.Connection}
    sql::String
    result::Ref{DataFrame}

    SQLResult(db, sql) =
        new(db, sql, Ref{DataFrame}())
end

function ensure_result!(r::SQLResult)
    if !isassigned(r.result)
        r.result[] = cursor_to_dataframe(DBInterface.execute(r.db, r.sql))
    end
    r.result[]
end

function cursor_to_dataframe(cr)
    df = DataFrame(cr)
    # Remove `Missing` from column types where possible.
    disallowmissing!(df, error = false)
    # Render columns that are named `html` or `*_html` as HTML.
    htmlize!(df)

    df
end

function htmlize!(df)
    for col in names(df)
        if col == "html" || endswith(col, "_html")
            df[!, col] = htmlize.(df[!, col])
        end
    end
end

htmlize(str::AbstractString) =
    HTML(str)

htmlize(val) =
    val

Tables.istable(::Type{SQLResult}) =
    true

Tables.columnaccess(::Type{SQLResult}) =
    true

Tables.columns(r::SQLResult) =
    Tables.columns(ensure_result!(r))

Tables.rowaccess(::Type{SQLResult}) =
    true

Tables.rows(r::SQLResult) =
    Tables.rows(ensure_result!(r))

Tables.schema(r::SQLResult) =
    Tables.schema(ensure_result!(r))

Tables.getcolumn(r::SQLResult, i::Union{Int, Symbol}) =
    Tables.getcolumn(ensure_result!(r), i)

Tables.columnnames(r::SQLResult) =
    Tables.columnnames(ensure_result!(r))
