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

"""
    decode_funsql_label(s) -> String

Decode a FunSQL percent-encoded column label back to its original form.
Reverses the encoding where `.` becomes `%2E` and `%` becomes `%25`.

This performs a minimal two-sequence decode sufficient for display purposes.
For full single-pass percent-decoding used during validation (to recover
multi-byte UTF-8 characters hidden behind `%XX` encoding), see `percent_decode`.
Both functions produce identical results on FunSQL-generated labels because
FunSQL encodes only `.` and `%`.
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

"""Maximum rows in an Excel worksheet (including the header row).
Sheets with more rows than this cannot be opened correctly in Excel."""
const EXCEL_MAX_ROWS = 1_048_576

"""Maximum columns in an Excel worksheet.
Sheets with more columns than this cannot be opened correctly in Excel."""
const EXCEL_MAX_COLUMNS = 16_384

"""Column auto-size threshold in Apache POI units (1/256th of a character width).
Columns wider than this after auto-sizing are set to DEFAULT_COLUMN_WIDTH instead."""
const MAX_COLUMN_WIDTH = 70 * 256

"""Fallback column width (POI units) applied when auto-sized width exceeds
MAX_COLUMN_WIDTH. The visible narrowing signals that content is truncated."""
const DEFAULT_COLUMN_WIDTH = 50 * 256

"""Correction factor applied to POI's auto-sized column width to compensate for
font metric differences between Java AWT (used by POI) and Excel Online's renderer."""
const AUTOSIZE_CORRECTION_FACTOR = 1.1

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
    validate_sheet_names(names) -> names

Validate each sheet name individually and check for case-insensitive duplicates.
Excel treats sheet names as case-insensitive for uniqueness.
"""
function validate_sheet_names(names)
    seen = Set{String}()
    for name in names
        validate_sheet_name(name)
        key = lowercase(name)
        if key in seen
            throw(ArgumentError("Duplicate sheet name (case-insensitive): $(repr(name))"))
        end
        push!(seen, key)
    end
    names
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
Returns `s` unchanged when no invalid characters are present.
"""
sanitize_for_xlsx(s::AbstractString) =
    _needs_sanitization(s) ? map(c -> any(r -> c in r, XML_INVALID_CONTROL_CHARS) ? ' ' : c, s) : s

_needs_sanitization(s::AbstractString) =
    any(c -> any(r -> c in r, XML_INVALID_CONTROL_CHARS), s)

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
    find_invalid_control_chars(s) -> Vector{Char}

Return the distinct XML-invalid control characters found in `s`, in order of
first occurrence. Used to report which characters `sanitize_for_xlsx` replaced.
"""
function find_invalid_control_chars(s::AbstractString)
    found = Char[]
    for c in s
        if any(r -> c in r, XML_INVALID_CONTROL_CHARS) && c ∉ found
            push!(found, c)
        end
    end
    found
end

"""
    describe_codepoints(chars) -> String

Format a collection of characters as a comma-separated list of Unicode
codepoints (e.g. "U+0000, U+001F") for use in diagnostic messages.
"""
describe_codepoints(chars) =
    join(["U+$(uppercase(string(codepoint(c), base=16, pad=4)))" for c in chars], ", ")

"""
    percent_decode(s) -> String

Decode all percent-encoded sequences (`%XX`) in `s` back to their original bytes,
then interpret the result as UTF-8. This is a full single-pass decode used during
validation to recover the original characters from FunSQL's percent-encoded column
labels so that `check_javacall_compatible` can detect supplementary plane characters
that would otherwise be hidden behind ASCII percent-encoding.

For display purposes (column headers written to cells), use `decode_funsql_label`
instead, which decodes only the two sequences FunSQL produces (`%2E` and `%25`).
Both functions yield identical results on FunSQL-generated labels.
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
    if !haskey(ENV, "CACHE_DIR")
        return nothing
    end
    pwfile = joinpath(ENV["CACHE_DIR"], "password.txt")
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

