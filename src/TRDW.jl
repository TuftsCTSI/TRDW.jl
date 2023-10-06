module TRDW

export @run_funsql

using DataFrames
using ODBC
using FunSQL
using Dates
using CSV
using ZipFile

include("general.jl")
include("spark.jl")
include("helpers.jl")
include("export.jl")
include("omop.jl")
include("vocabulary.jl")

include("concept.jl")
include("condition_era.jl")
include("condition.jl")
include("death.jl")
include("device.jl")
include("dose_era.jl")
include("drug_era.jl")
include("drug.jl")
include("measurement.jl")
include("note.jl")
include("note_nlp.jl")
include("observation.jl")
include("person.jl")
include("procedure.jl")
include("provider.jl")
include("specimen.jl")
include("visit_detail.jl")
include("visit.jl")

funsql_export() =
    for name in Base.names(@__MODULE__, all = true)
        if startswith(string(name), "funsql#")
            @eval export $name
        end
    end

funsql_export()

end
