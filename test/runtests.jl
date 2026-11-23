# Run from the package root with
#
#     julia +1.11.6 --project=test test/runtests.jl
#

using Test
using TRDW

include("support/mock_catalog.jl")
include("support/environment.jl")
include("support/render.jl")
include("support/interpolation.jl")

@testset "TRDW" begin
    @testset "loads" begin
        @test isdefined(TRDW, :funsql_person)
    end
    @testset "test helpers" begin
        include("selftest/mock_catalog.jl")
        include("selftest/environment.jl")
        include("selftest/render.jl")
        include("selftest/interpolation.jl")
    end
    @testset "unit" begin
        include("unit/interpolation.jl")
    end
    @testset "render" begin
        include("render/zero_args.jl")
        include("render/helpers.jl")
        include("render/omop.jl")
        include("render/delta.jl")
        include("render/inventory.jl")
        include("render/counting.jl")
        include("render/nodes.jl")
        include("render/spark.jl")
    end
end
