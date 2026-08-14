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

const INVALID_SHEET_NAME_CHARS = r"[\[\]:*?/\\%]"
const MAX_SHEET_NAME_LENGTH = 31

function validate_sheet_name(name::AbstractString)
    isempty(name) &&
        throw(ArgumentError("Sheet name cannot be empty"))
    length(name) > MAX_SHEET_NAME_LENGTH &&
        throw(ArgumentError("Sheet name exceeds 31 characters: $(repr(name))"))
    m = match(INVALID_SHEET_NAME_CHARS, name)
    m !== nothing &&
        throw(ArgumentError("Sheet name contains invalid character '$(m.match)': $(repr(name))"))
    (startswith(name, "'") || endswith(name, "'")) &&
        throw(ArgumentError("Sheet name cannot start or end with apostrophe: $(repr(name))"))
    name
end

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

function run(db, spec::WriteXLSXSpecification)
    @assert length(methods(TRDW.XLSX.write)) > 0 """To use XLSX writing you need:
      import JavaCall
      # then, after TRDW is imported
      JavaCall.isloaded() ? nothing : JavaCall.init()
    """
    dataframes = Pair{String, DataFrame}[]
    total_rows = 0
    for (name, query) in spec.sheets
        data = run(db, query)
        df = DataFrame(data)
        total_rows += size(df, 1)
        push!(dataframes, name => df)
    end
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
        mkpath(dirname(filename))
        TRDW.XLSX.write(filename, dataframes; password)
        if spec.encrypt && !is_production_schema_prefix()
            @info "password for $filename is $password"
        end
        @htl("""
            <p>$summary written. Download <a href="$filename">$filename</a>.</p>
        """)
    end
end

