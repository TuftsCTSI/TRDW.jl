module XLSX

"""
    XLSX.write(file, table; password = nothing)
    XLSX.write(file, sheets::AbstractVector{<:Pair{<:AbstractString}}; password = nothing)

Write tabular data to an XLSX file, optionally encrypting it.
The single-table form writes one sheet named "Sheet1".
The multi-sheet form accepts a vector of `name => table` pairs.
"""
function write
end

"""Characters that FunSQL percent-encodes in column labels.
Period (`.`) is the qualifier separator; percent (`%`) is the escape character.
Column headers from FunSQL queries contain `%2E` for period and `%25` for percent,
and must be decoded for display via `decode_funsql_label`."""
const FUNSQL_ENCODED_CHARS = ('.', '%')

"""
    decode_funsql_label(s) -> String

Decode a FunSQL percent-encoded column label back to its original form.
Reverses the encoding where `.` becomes `%2E` and `%` becomes `%25`.
"""
function decode_funsql_label(s::AbstractString)
    s = replace(s, "%2E" => ".")
    s = replace(s, "%25" => "%")
    s
end

"""Maximum Unicode codepoint that JavaCall can correctly convert to a Java String.
Characters above this threshold (supplementary plane, U+10000 and above) require
UTF-16 surrogate pairs, which JavaCall's JNI string conversion does not handle.
Such characters are silently corrupted (e.g., U+1F7E2 appears as U+00F0)."""
const JAVACALL_MAX_CODEPOINT = 0xFFFF

"""Maximum characters in an Excel cell. Values exceeding this limit are silently
truncated when the file is opened in Excel."""
const EXCEL_MAX_CELL_LENGTH = 32767

"""Column auto-size threshold in Apache POI units (1/256th of a character width).
Columns wider than this after auto-sizing are set to DEFAULT_COLUMN_WIDTH instead."""
const MAX_COLUMN_WIDTH = 70 * 256

"""Fallback column width (POI units) applied when auto-sized width exceeds
MAX_COLUMN_WIDTH. The visible narrowing signals that content is truncated."""
const DEFAULT_COLUMN_WIDTH = 50 * 256

"""XML control character ranges invalid in OOXML cell content.
Includes C0 controls (U+0000..U+0008, U+000B, U+000C, U+000E..U+001F)
but excludes tab (U+0009), newline (U+000A), and carriage return (U+000D)
which are valid in XML text nodes."""
const XML_INVALID_CONTROL_CHARS = ('\x00':'\x08', '\x0b':'\x0b', '\x0c':'\x0c', '\x0e':'\x1f')

"""Characters allowed in sheet names. Only filesystem-safe printable ASCII is
permitted because sheet names may be used as filename prefixes."""
const ALLOWED_SHEET_NAME_CHARS = Set{Char}(
    vcat(collect('A':'Z'), collect('a':'z'), collect('0':'9'),
         [' ', '-', '_', ',', '(', ')']))

const MAX_SHEET_NAME_LENGTH = 31

function validate_sheet_name(name::AbstractString)
    isempty(name) &&
        throw(ArgumentError("Sheet name cannot be empty"))
    length(name) > MAX_SHEET_NAME_LENGTH &&
        throw(ArgumentError("Sheet name exceeds $MAX_SHEET_NAME_LENGTH characters: $(repr(name))"))
    for c in name
        if c ∉ ALLOWED_SHEET_NAME_CHARS
            throw(ArgumentError("Sheet name contains character '$(c)' which is not allowed: $(repr(name))"))
        end
    end
    check_javacall_compatible(name; context = "sheet name \"$name\"")
    name
end

"""
    check_javacall_compatible(s; context) -> s

Verify that all characters in `s` have codepoints within the Basic Multilingual Plane
(at or below U+FFFF). Throws `ArgumentError` with the provided context if a
supplementary plane character is found.
"""
function check_javacall_compatible(s::AbstractString; context::AbstractString)
    for c in s
        if codepoint(c) > JAVACALL_MAX_CODEPOINT
            throw(ArgumentError(
                "Character '$(c)' (U+$(uppercase(string(codepoint(c), base=16, pad=5)))) " *
                "in $context cannot be encoded by JavaCall " *
                "(supplementary plane characters above U+FFFF are not supported)"))
        end
    end
    s
end

"""
    check_cell_length(s; context) -> s

Verify that `s` does not exceed Excel's maximum cell length of $EXCEL_MAX_CELL_LENGTH
characters. Throws `ArgumentError` with the provided context if exceeded.
"""
function check_cell_length(s::AbstractString; context::AbstractString)
    n = length(s)
    if n > EXCEL_MAX_CELL_LENGTH
        throw(ArgumentError(
            "Cell value in $context exceeds Excel's maximum length " *
            "of $EXCEL_MAX_CELL_LENGTH characters (actual: $n)"))
    end
    s
end

"""
    sanitize_for_xlsx(s) -> String

Replace XML-invalid control characters with spaces.
Preserves tab (U+0009), newline (U+000A), and carriage return (U+000D).
"""
sanitize_for_xlsx(s::AbstractString) =
    map(c -> c in '\x00':'\x08' || c == '\x0b' || c == '\x0c' || c in '\x0e':'\x1f' ? ' ' : c, s)

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
    if !haskey(ENV, "CACHE_DIR")
        return nothing
    end
    pwfile = joinpath(ENV["CACHE_DIR"], "password.txt")
    if isfile(pwfile)
        password = strip(read(pwfile, String))
        @assert length(password) > 10
    else
        password = make_password()
        f = open(pwfile, "w")
        write(f, password * "\n")
        flush(f)
        close(f)
    end
    return password
end

function _validate_xlsx_content(dataframes::AbstractVector{<:Pair{String, DataFrame}})
    for (name, df) in dataframes
        XLSX.validate_sheet_name(name)
        for c in Tables.columnnames(df)
            header = string(c)
            XLSX.check_javacall_compatible(header; context = "column header \"$header\"")
        end
        for (k, r) in enumerate(Tables.rows(df))
            for c in Tables.columnnames(r)
                val = Tables.getcolumn(r, c)
                if !ismissing(val) && !(val isa Bool) && !(val isa Dates.Date) && !(val isa Dates.DateTime) && !(val isa Number)
                    s = string(val)
                    ctx = "column \"$(string(c))\", row $k"
                    XLSX.check_javacall_compatible(s; context = ctx)
                    XLSX.check_cell_length(s; context = ctx)
                end
            end
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
    password = spec.encrypt ? get_password() : nothing
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
    elseif spec.encrypt && (isnothing(password) || password == "")
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

