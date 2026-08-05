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

end # module XLSX
