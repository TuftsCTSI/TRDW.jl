module TRDW

export @run_funsql, @concepts, @valuesets, @codesets, OHDSI

using CSV
using DBInterface
using DataFrames
using Dates
using FunSQL
using FunSQL: @dissect
using HTTP
using HypertextLiteral
using JSON
using LightXML
using ODBC

import Tables
import DBInterface.execute
import Base.show

include("general.jl")
include("define.jl")
include("inventory.jl")
include("spark.jl")
include("helpers.jl")
include("export.jl")
include("vocabulary.jl")
include("codeset.jl")
include("valueset.jl")
include("subject.jl")
include("filters.jl")
include("linking.jl")
include("counting.jl")
include("report.jl")
include("wiise.jl")
include("soarian.jl")
include("clarity.jl")
include("template.jl")
include("profile.jl")
include("ohdsi.jl")
include("xlsx.jl")

include("care_site.jl")
include("concept.jl")
include("condition.jl")
include("device.jl")
include("drug.jl")
include("location.jl")
include("measurement.jl")
include("note.jl")
include("observation.jl")
include("person.jl")
include("procedure.jl")
include("provider.jl")
include("specimen.jl")
include("visit.jl")
include("visit_detail.jl")

funsql_export() =
    for name in Base.names(@__MODULE__, all = true)
        if startswith(string(name), "funsql_")
            @eval export $name
        end
    end

funsql_export()

global show_funsql_path = false

FunSQL.showpath(io::IO, path::Vector{FunSQL.SQLNode}) = begin
    if show_funsql_path && !isempty(path)
        q = FunSQL.highlight(path)
        println(io, " in:")
        FunSQL.pprint(io, q)
    end
end

end
