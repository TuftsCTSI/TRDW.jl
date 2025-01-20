module TRDW

export @connect, @query, Chain, Fun, Var, @concepts, @valuesets, OHDSI, user_project_schema, project_schema, is_discovery

using CSV
using DBInterface
using DataFrames
using Dates
using FunSQL
using FunSQL: @dissect, Chain, Fun, Var
using HTTP
using HypertextLiteral
using JSON
using JSON3
using OrderedCollections: OrderedDict
using HashArrayMappedTries
using Unicode
using LightXML
using ODBC
using PlutoUI
using Scratch
using Arrow
using SHA
using AbstractPlutoDingetjes

import Tables
import DBInterface.execute
import Base.show

include("config.jl")
include("connect.jl")
include("format.jl")
include("result.jl")
include("general.jl")
include("nodes.jl")
include("inventory.jl")
include("spark.jl")
include("helpers.jl")
include("export.jl")
include("vocabulary.jl")
#include("valueset.jl")
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
include("drawio.jl")
include("etl.jl")
include("ddl.jl")
include("index.jl")

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
