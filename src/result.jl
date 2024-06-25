struct SQLResult
    db::FunSQL.SQLConnection{ODBC.Connection}
    sql::String
    fmt::SQLFormat
    ref::Ref{DataFrame}

    SQLResult(db, sql, fmt) =
        new(db, sql, fmt, Ref{DataFrame}())

    SQLResult(db, sql, fmt, data) =
        new(db, sql, fmt, Ref{DataFrame}(data))
end

Base.show(io::IO, r::SQLResult) =
    print(io, "SQLResult()")

Base.show(io::IO, mime::MIME"text/html", r::SQLResult) =
    Base.show(io, mime, _format(ensure_result!(r), r.fmt))

Base.convert(::Type{FunSQL.AbstractSQLNode}, df::DataFrame) =
    FunSQL.From(df)

Base.convert(::Type{FunSQL.AbstractSQLNode}, r::SQLResult) =
    convert(FunSQL.SQLNode, ensure_result!(r))

run(db, q) =
    run(db, convert(FunSQL.SQLNode, q))

function run(db, sql::AbstractString; fmt = SQLFormat())
    SQLResult(db, sql, fmt)
end

function run(db, q::FunSQL.SQLNode; fmt = SQLFormat())
    sql = FunSQL.render(db, q)
    SQLResult(db, sql, fmt)
end

function run(db, df::DataFrame; fmt = SQLFormat())
    sql = "" # FunSQL.render(db, FunSQL.From(df))
    SQLResult(db, sql, fmt, df)
end

function run(db, r::SQLResult)
    db === r.db ? r : run(db, ensure_result!(r))
end

function ensure_result!(r::SQLResult)
    if !isassigned(r.ref)
        r.ref[] = cursor_to_dataframe(DBInterface.execute(r.db, r.sql))
        ODBC.clear!(r.db.raw)
    end
    r.ref[]
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
