"""
    TRDW.XLSX

Writes XLSX workbooks, encrypted or not, through XLSXEncrypt.jl.

Column headers arrive as FunSQL labels, which percent-encode some characters
(for example `.` as `%2E`). `write` decodes them with `percent_decode` before
writing, so the workbook shows the original names. XLSXEncrypt checks sheet
names, sheet sizes, cell lengths, and passwords, and throws `ArgumentError`
when one is invalid.
"""
module XLSX

import DataFrames
import XLSXEncrypt

"""
    XLSX.write(file, table; password = nothing)
    XLSX.write(file, sheets::AbstractVector{<:Pair{<:AbstractString}}; password = nothing)

Write tabular data to an XLSX file, optionally encrypting it.
The single-table form writes one sheet named "Sheet1".
The multi-sheet form accepts a vector of `name => table` pairs.
Each table can be any Tables.jl source. Its column names are decoded with
`percent_decode` and written as the header row.

Throws an error if `file` already exists.
"""
write(file::AbstractString, table; password = nothing) =
    XLSXEncrypt.write_xlsx(file, decode_headers(table); password)

write(file::AbstractString, sheets::AbstractVector{<:Pair{<:AbstractString}}; password = nothing) =
    XLSXEncrypt.write_xlsx(file, [name => decode_headers(table) for (name, table) in sheets]; password)

"""
    decode_headers(table) -> DataFrame

Return `table` as a `DataFrame` whose column names have been decoded with
`percent_decode`. Columns are not copied. Throws `ArgumentError` if two
columns decode to the same name.
"""
function decode_headers(table)
    df = DataFrames.DataFrame(table; copycols = false)
    DataFrames.rename(df, [name => percent_decode(name) for name in names(df)])
end

_is_hex(b::UInt8) =
    (UInt8('0') <= b <= UInt8('9')) ||
    (UInt8('A') <= b <= UInt8('F')) ||
    (UInt8('a') <= b <= UInt8('f'))

function _hex_val(b::UInt8)
    if UInt8('0') <= b <= UInt8('9')
        b - UInt8('0')
    elseif UInt8('A') <= b <= UInt8('F')
        b - UInt8('A') + 0x0A
    else
        b - UInt8('a') + 0x0A
    end
end

"""
    percent_decode(s) -> String

Decode all percent-encoded sequences (`%XX`) in `s` back to their original bytes,
then interpret the result as UTF-8. FunSQL encodes `.` as `%2E` and `%` as `%25`
in column labels; this decodes those and any other `%XX` sequence, including
multi-byte UTF-8 characters. A `%` not followed by two hex digits is kept as is.
"""
function percent_decode(s::AbstractString)
    bytes = UInt8[]
    i = 1
    while i <= ncodeunits(s)
        if codeunit(s, i) == UInt8('%') && i + 2 <= ncodeunits(s)
            hi = codeunit(s, i + 1)
            lo = codeunit(s, i + 2)
            if _is_hex(hi) && _is_hex(lo)
                push!(bytes, _hex_val(hi) << 4 | _hex_val(lo))
                i += 3
                continue
            end
        end
        push!(bytes, codeunit(s, i))
        i += 1
    end
    String(bytes)
end

end # module XLSX

struct WriteXLSXSpecification
    prefix::String
    sheets::Vector{Pair{String, FunSQL.SQLQuery}}
    encrypt::Bool
    skip::Bool
end

_xlsx_spec(file, sheets; encrypt, skip) =
    WriteXLSXSpecification(string(file), [string(k) => v for (k, v) in sheets],
        encrypt, something(skip, get(ENV, "CI", nothing) != "true"))

function _xlsx_dispatch(encrypt::Bool, file::Union{Symbol, AbstractString}, sheet1::Pair{<:Union{Symbol, AbstractString}, <:Any}, rest::Pair{<:Union{Symbol, AbstractString}, <:Any}...; skip=nothing)
    _xlsx_spec(file, (sheet1, rest...); encrypt, skip)
end

function _xlsx_dispatch(encrypt::Bool, sheet1::Pair{<:Union{Symbol, AbstractString}, <:Any}, rest::Pair{<:Union{Symbol, AbstractString}, <:Any}...; skip=nothing)
    name = string(first(sheet1))
    @info "No filename provided; using \"$name\" as filename"
    _xlsx_spec(name, (sheet1, rest...); encrypt, skip)
end

function _xlsx_dispatch(::Bool, ::Union{Symbol, AbstractString}, args...; skip=nothing)
    throw(ArgumentError("Arguments after the filename must be \"Name\" => query() pairs."))
end

"""
    @query write_xlsx("filename", "Sheet 1" => query_1(), "Sheet 2" => query_2())

Write query results to an unencrypted XLSX workbook.
When the filename is omitted, it defaults to the first sheet name.

Requires JavaCall boilerplate in your notebook:

    using JavaCall
    JavaCall.isloaded() ? nothing : JavaCall.init()
    JavaCall.assertroottask_or_goodenv()
"""
funsql_write_xlsx(args...; skip=nothing) = _xlsx_dispatch(false, args...; skip)

"""
    @query write_encrypted_xlsx("filename", "Sheet 1" => query_1(), "Sheet 2" => query_2())

Write query results to a password-protected XLSX workbook.
When the filename is omitted, it defaults to the first sheet name.

Requires JavaCall boilerplate in your notebook:

    using JavaCall
    JavaCall.isloaded() ? nothing : JavaCall.init()
    JavaCall.assertroottask_or_goodenv()
"""
funsql_write_encrypted_xlsx(args...; skip=nothing) = _xlsx_dispatch(true, args...; skip)

function make_password()
    valid_characters = "ABCDEFGHJKLMNPQRSTUVWXYZabcdefghjkmnpqrstuvwxyz0123456789"
    return join(rand(valid_characters, 13))
end

function get_password()
    if !haskey(ENV, "PASSWORD_FILE")
        return nothing
    end
    pwfile = ENV["PASSWORD_FILE"]
    if isfile(pwfile)
        password = strip(read(pwfile, String))
        @assert length(password) > 10
    else
        password = make_password()
        open(pwfile, "w") do f
            write(f, password * "\n")
        end
    end
    return password
end

function _validate_xlsx_content(dataframes::AbstractVector{<:Pair{String, DataFrame}})
    XLSX.validate_sheet_names(first.(dataframes))
    for (name, df) in dataframes
        nrows = size(df, 1)
        ncols = size(df, 2)
        if nrows + 1 > XLSX.EXCEL_MAX_ROWS
            throw(ArgumentError(
                "Sheet \"$name\" exceeds Excel's row limit of $(XLSX.EXCEL_MAX_ROWS) " *
                "(header + $nrows data rows)"))
        end
        if ncols > XLSX.EXCEL_MAX_COLUMNS
            throw(ArgumentError(
                "Sheet \"$name\" has $ncols columns, exceeding Excel's maximum " *
                "of $(XLSX.EXCEL_MAX_COLUMNS) columns per sheet"))
        end
        for c in Tables.columnnames(df)
            raw = string(c)
            decoded = XLSX.percent_decode(raw)
            XLSX.check_javacall_compatible(decoded; context = "column header \"$decoded\"")
        end
    end
end

function run(db, spec::WriteXLSXSpecification)
    dataframes = Pair{String, DataFrame}[]
    total_rows = 0
    for (name, query) in spec.sheets
        data = run(db, query)
        df = DataFrame(data)
        total_rows += size(df, 1)
        push!(dataframes, name => df)
    end
    _validate_xlsx_content(dataframes)
    when =
        let t = tryparse(Int, get(ENV, "SOURCE_DATE_EPOCH", ""))
            t !== nothing ? Dates.unix2datetime(t) : Dates.now(UTC)
        end
    suffix = Dates.format(when, "yyyymmddtHHMM")
    filename = "download/$(spec.prefix)_$(suffix).xlsx"
    n_sheets = length(dataframes)
    summary = n_sheets == 1 ? "$total_rows rows" : "$n_sheets sheets ($total_rows rows)"
    if spec.skip
        @htl("<p>Not writing $summary to $filename: not production schema</p>")
    else
        password = spec.encrypt ? get_password() : nothing
        if spec.encrypt && (isnothing(password) || password == "")
            @htl("<p>Not writing $summary to $filename: password not available</p>")
        else
            @assert length(methods(TRDW.XLSX.write)) > 0 """To use XLSX writing you need:
              import JavaCall
              # then, after TRDW is imported
              JavaCall.isloaded() ? nothing : JavaCall.init()
            """
            mkpath(dirname(filename))
            assert_new_file(filename)
            TRDW.XLSX.write(filename, dataframes; password)
            if spec.encrypt && !is_production_schema_prefix()
                @info "password for $filename is $password"
            end
            @htl("""
                <p>$summary written. Download <a href="$filename">$filename</a>.</p>
            """)
        end
    end
end

